from datetime import timedelta
from typing import List, Optional, Sequence, Tuple

from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.xcom import XCOM_RETURN_KEY

from ttd.cloud_provider import AwsCloudProvider
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_task_visitor import EmrTaskVisitor
from ttd.eldorado.base import ElDoradoError
from ttd.eldorado.visitor import AbstractVisitor
from ttd.eldorado.xcom.helpers import get_push_xcom_task_id, get_xcom_pull_jinja_string
# Openlineage support
from ttd.openlineage import OpenlineageConfig, OpenlineageTransport
from ttd.operators.ttd_emr_add_steps_operator import TtdEmrAddStepsOperator
from ttd.sensors.ttd_emr_step_sensor import TtdEmrStepSensor
from ttd.tasks.base import BaseTask
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask


def _deep_copy_args(args: Optional[Sequence[Tuple[str, str]]] = None) -> List[Tuple[str, str]]:
    if args is not None:
        return [(x, y) for (x, y) in args]
    return []


class EmrJobTask(ChainOfTasks):
    EXECUTABLE_PATH = "s3://ttd-build-artefacts/eldorado/snapshots/master/latest/el-dorado-assembly.jar"
    JOB_JAR = "command-runner.jar"

    def __init__(
        self,
        name: str,
        class_name: str,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        deploy_mode: str = "cluster",
        executable_path: Optional[str] = None,
        command_line_arguments=None,
        timeout_timedelta: timedelta = timedelta(hours=4),
        action_on_failure: str = "TERMINATE_CLUSTER",
        configure_cluster_automatically: bool = False,
        cluster_specs: Optional[EmrClusterSpecs] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
        job_jar: Optional[str] = None,
        is_pyspark: bool = False,
        region_name: str = "us-east-1",
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        do_xcom_push: bool = False,
        xcom_json_path: Optional[str] = None,
        retries=1,
        maximize_resource_allocation=False,
    ):
        """
        do_xcom_push: If we set do_xcom_push to true, we will hunt for the file on s3 specified by xcom_json_path
                      then once the EmrJobTask is finished it will create a task that pushes the result to xcom
        xcom_json_path: This should be an s3 path, it will be passed into EMR cluster env var called XCOM_JSON_PATH
                        you should log a json-like file there, by default it will be "{log_uri}/{job_flow_id}/xcoms/{emr_task_name}_json"
                        You can see an example of a decorator that does this here (log_xcom function)
                        https://gitlab.adsrvr.org/thetradedesk/teams/forecast/ds_forecasting/-/blob/master/training_and_validation/helpers/airflow.py
                        Gotcha: you might want to avoid using .json in the path name though as the PythonOperator
                        could wrongly think this is a local file (see template_ext in airflow docs)
        """
        self.name = name

        self.action_on_failure = action_on_failure
        self.timeout_timedelta = timeout_timedelta
        self.job_jar = job_jar or self.JOB_JAR
        self.retries = retries

        self.deploy_mode = deploy_mode
        self.add_job_task = OpTask()
        self.watch_job_task = OpTask()

        tasks = [self.add_job_task, self.watch_job_task]

        self.do_xcom_push = do_xcom_push
        self.xcom_json_path = xcom_json_path

        if do_xcom_push:
            self.xcom_push_task = OpTask()
            tasks.append(self.xcom_push_task)

        super().__init__(task_id=name, tasks=tasks)

        self.class_name = class_name
        self.eldorado_config_option_pairs_list = _deep_copy_args(eldorado_config_option_pairs_list)
        self.additional_args_option_pairs_list = _deep_copy_args(additional_args_option_pairs_list)
        self.configure_cluster_automatically = configure_cluster_automatically
        self.cluster_config = cluster_specs
        self.command_line_arguments = command_line_arguments
        self.cluster_calc_defaults = cluster_calc_defaults

        self.executable_path = executable_path or self.EXECUTABLE_PATH
        self.is_pyspark = is_pyspark
        self.region_name = region_name

        self.openlineage_config = openlineage_config
        self.maximize_resource_allocation = maximize_resource_allocation

        # If we want to apply an experiment, we can break this task out here,
        # and COW the dags all the way down
        # self.is_experiment = is_experiment

        if self.cluster_config:
            self._init_job_step_ops()

    def set_cluster_specs(self, cluster_config: EmrClusterSpecs):
        if cluster_config is None:
            raise Exception("cluster_config must not be None")

        if self.cluster_config is not None:
            # do nothing. existing config is not overwritten. We should log a warning
            return

        self.cluster_config = cluster_config
        self._init_job_step_ops()

        for d in self._downstream:
            if isinstance(d, EmrJobTask):
                d.set_cluster_specs(cluster_config)

    def _init_job_step_ops(self):
        if self.do_xcom_push:
            if self.xcom_json_path is None:
                log_uri = EmrClusterTask.log_uri(self.cluster_config.cluster_name)
                self.xcom_json_path = f"s3://{log_uri}{self._get_job_flow_id()}/xcoms/{self.name}_json"
            self._init_xcom_push_job_step_op()
        spark_submit_args = self._build_spark_step_args()
        self._init_add_job_step_op(spark_submit_args)
        self._init_watch_job_step_op()

    def _get_job_flow_id(self) -> str:
        return EmrClusterTask.job_flow_id(self.cluster_config.cluster_name)

    def _init_watch_job_step_op(self):
        task_watch_name = f"{self.cluster_config.cluster_name}_watch_task_{self.name}"
        task_add_name = f"{self.cluster_config.cluster_name}_add_task_{self.name}"
        job_flow_id = self._get_job_flow_id()
        self.watch_job_task.set_op(
            op=TtdEmrStepSensor(
                job_flow_id=job_flow_id,
                step_id=f"{{{{ task_instance.xcom_pull('{task_add_name}', key='{XCOM_RETURN_KEY}')[0] }}}}",
                cluster_task_id=EmrClusterTask.create_cluster_task_id(self.cluster_config.cluster_name),
                task_id=task_watch_name,
                timeout=self.timeout_timedelta.total_seconds(),
                aws_conn_id="aws_default",
                poke_interval=60,  # seconds
                region_name=self.region_name,
                retries=self.retries,
                retry_delay=timedelta(minutes=1),
            )
        )

    def _format_spark_step(self, spark_submit_args):
        robust_transport_is_enabled = self.openlineage_config.enabled and self.openlineage_config.transport == OpenlineageTransport.ROBUST
        steps = [{
            "Name": self.name,
            "ActionOnFailure": self.action_on_failure if not robust_transport_is_enabled else "CONTINUE",
            "HadoopJarStep": {
                "Jar": self.job_jar,
                "Args": spark_submit_args
            },
        }]
        return steps

    def _init_add_job_step_op(self, spark_submit_args):
        task_add_name = f"{self.cluster_config.cluster_name}_add_task_{self.name}"

        spark_step = self._format_spark_step(spark_submit_args)

        job_flow_id = self._get_job_flow_id()

        self.add_job_task.set_op(
            op=TtdEmrAddStepsOperator(
                task_id=task_add_name,
                job_flow_id=job_flow_id,
                aws_conn_id="aws_default",
                steps=spark_step,
                region_name=self.region_name,
            )
        )

    def _get_push_xcom_task_id(self) -> str:
        """
        Get the task id for the push xcom task
        """
        return get_push_xcom_task_id(self.name)

    def get_xcom_pull_jinja_string(self, key: str) -> str:
        """
        Gets a jinja string for a specific key from the logged json file
        See do_xcom_push and xcom_json_path in the init
        """
        if not self.do_xcom_push:
            raise AssertionError("do_xcom_push must be set to True to be able to pull")
        return get_xcom_pull_jinja_string(self._get_push_xcom_task_id(), key)

    @staticmethod
    def _load_json_from_s3_as_xcom(xcom_json_path: str, task_instance: TaskInstance) -> None:
        # pylint: disable=import-outside-toplevel
        import json

        from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

        aws_cloud_storage = AwsCloudStorage()
        for k, v in json.loads(aws_cloud_storage.read_key(xcom_json_path)).items():
            task_instance.xcom_push(k, v)

    def _init_xcom_push_job_step_op(self):
        self.xcom_push_task.set_op(
            op=PythonOperator(
                task_id=self._get_push_xcom_task_id(),
                python_callable=self._load_json_from_s3_as_xcom,
                op_kwargs=dict(  # pylint: disable=use-dict-literal
                    xcom_json_path=self.xcom_json_path
                ),
            )
        )

    def _build_spark_step_args(self):
        # if running a bash script against an emr cluster as a step
        # i.e. to run a docker container, or some python applications
        # we do not need spark-submit, a deployment mode or a class name because a .sh script has no class
        if not self.class_name and not self.is_pyspark:
            script_args = [self.executable_path]
            self._add_command_line_args(script_args)
            return script_args

        spark_submit_args = [
            "spark-submit",
            "--deploy-mode",
            self.deploy_mode,
        ]

        if not self.is_pyspark:
            spark_submit_args.extend(["--class", self.class_name])

        additional_args_list: list[tuple[
            str, str]] = (list(self.additional_args_option_pairs_list) if self.additional_args_option_pairs_list is not None else [])
        # These args are the spark args

        if (self.configure_cluster_automatically or not self.additional_args_option_pairs_list):
            worker_instance_type = (self.cluster_config.core_fleet_instance_type_configs.get_base_instance())
            worker_instance_count = (self.cluster_config.core_fleet_instance_type_configs.get_scaled_instance_count())

            if not worker_instance_type.instance_spec_defined():
                raise ElDoradoError("Can't automatically configure resources for Spark on instance type that doesn't support it!")
            additional_args_list = self.get_spark_resources_extended_with_args(
                additional_args_list,
                worker_instance_type,
                worker_instance_count,
                self.cluster_calc_defaults,
            )

        if self.do_xcom_push:
            additional_args_list.append((
                "conf",
                f"spark.yarn.appMasterEnv.XCOM_JSON_PATH={self.xcom_json_path}",
            ))

        eldorado_config_option_pairs_list = self.eldorado_config_option_pairs_list + self.openlineage_config.get_eldorado_opts_to_disable_bundled_openlineage(
        )
        # setup openlineage options
        if self.openlineage_config.enabled and (self.class_name is not None or self.executable_path is not None):
            if not self.is_pyspark:
                additional_args_list += self.openlineage_config.get_java_spark_options_list(
                    self.name, self.class_name, self.cluster_config.cluster_name, self.cluster_config.environment, AwsCloudProvider()
                )
            else:
                additional_args_list += self.openlineage_config.get_pyspark_options_list(
                    self.name, self.executable_path, self.cluster_config.cluster_name, self.cluster_config.environment, AwsCloudProvider()
                )
        for option_pair in additional_args_list:
            spark_submit_args += [f"--{option_pair[0]}", option_pair[1]]

        # Java args ted from the ing sources
        # -- The environment we're running on
        # -- Any java settings from the instance we're running on
        # -- Any config settings for the job itself.

        java_args = [
            ("ttd.env", self.cluster_config.environment.execution_env),
            #  https://www.lunasec.io/docs/blog/log4j-zero-day/ to mitigate possible issue
            ("log4j2.formatMsgNoLookups", "true"),
        ]
        java_args += eldorado_config_option_pairs_list

        java_args_string = " ".join([f"-D{option_pair[0]}={option_pair[1]}" for option_pair in java_args])

        spark_submit_args.append(f"--driver-java-options={java_args_string}")

        # Append the spark jar we're actually running - this HAS to follow the java args
        spark_submit_args.append(self.executable_path)

        # Append the command line arguments - this HAS to be the last arg
        self._add_command_line_args(spark_submit_args)

        return spark_submit_args

    def _add_command_line_args(self, args):
        command_line_arguments = self.command_line_arguments or ""
        if isinstance(command_line_arguments, str):
            # Single argument
            args.append(command_line_arguments)
        else:
            # List of arguments
            args.extend(command_line_arguments)

    def get_spark_resources_extended_with_args(
        self,
        additional_args_list: Sequence[Tuple[str, str]],
        worker_instance_type: EmrInstanceType,
        worker_instance_count: int,
        defaults: ClusterCalcDefaults,
    ) -> List[Tuple[str, str]]:
        resources_conf = worker_instance_type.calc_cluster_params(
            worker_instance_count,
            defaults.parallelism_factor,
            defaults.min_executor_memory,
            defaults.max_cores_executor,
            defaults.memory_tolerance,
            defaults.partitions,
        ).to_spark_arguments()

        # Maximize resource allocation should take precedence over
        # whatever defaults we set via cluster calc defaults
        if self.maximize_resource_allocation:
            resources_conf = {"conf_args": {}, "args": {}}

        spark_conf_args = resources_conf["conf_args"]
        conf_args = dict(pair[1].split("=", 1) for pair in additional_args_list if pair[0] == "conf")
        spark_conf_args.update(conf_args)

        spark_args = resources_conf["args"]
        args = dict(pair for pair in additional_args_list if pair[0] != "conf")
        spark_args.update(args)

        args_list = [(k, str(v)) for k, v in spark_args.items()] + [("conf", f"{k}={v}") for k, v in spark_conf_args.items()]
        return args_list

    def __rshift__(self, other: BaseTask) -> BaseTask:
        super().__rshift__(other)

        if self.cluster_config and isinstance(other, EmrJobTask):
            other.set_cluster_specs(self.cluster_config)

        return other

    def accept(self, visitor: AbstractVisitor) -> None:
        if isinstance(visitor, EmrTaskVisitor):
            visitor.visit_emr_job_task(self)
        else:
            super().accept(visitor)
