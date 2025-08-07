import os

from abc import ABC
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from airflow.models.variable import Variable
from airflow.providers.databricks.operators.databricks import XCOM_RUN_ID_KEY

from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.autoscaling_config import DatabricksAutoscalingConfig
from ttd.eldorado.databricks.aws_config import DatabricksAwsConfiguration, DatabricksEbsConfiguration
from ttd.eldorado.databricks.docker_config import DatabricksDockerImageOptions
from ttd.eldorado.databricks.region import DatabricksRegion
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.eldorado.databricks.tasks.notebook_databricks_task import NotebookDatabricksTask
from ttd.eldorado.databricks.tasks.python_wheel_databricks_task import PythonWheelDatabricksTask
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.databricks.workspace import DevUsEastDatabricksWorkspace
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.mixins.tagged_cluster_mixin import TaggedClusterMixin
from ttd.operators.databricks_push_xcom_operator import DatabricksPushXcomOperator
from ttd.eldorado.aws.cluster_configs.monitoring_configuration import MonitoringConfigurationSpark3
from ttd.operators.databricks_retry_operator import DatabricksRetryRunOperator
from ttd.operators.databricks_watch_run_operator import DatabricksWatchRunOperator
from ttd.operators.databricks_create_and_run_operator import DatabricksCreateAndRunJobOperator
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory, TtdEnv
from ttd.workers.worker import Workers
from ttd.dag_owner_utils import infer_team_from_call_stack


def get_current_branch_name() -> str:
    return os.getenv("AIRFLOW__WEBSERVER__INSTANCE_NAME")


INFERRED_INSTANCE_PROFILE_TEMPLATE = "arn:aws:iam::{account_id}:instance-profile/medivac/medivac-compute-{team}-{env}-{region}"
DEFAULT_REGION = DatabricksRegion.use()
DEMO_FOLDER = "demo"


def infer_instance_profile(account_id: str, team: str, env: TtdEnv, region: str) -> str:
    env_str = ""
    match env:
        case TtdEnvFactory.prod:
            env_str = "prod"
        case TtdEnvFactory.dev:
            env_str = "dev"
        case _:
            env_str = "prodtest"

    return INFERRED_INSTANCE_PROFILE_TEMPLATE.format(
        account_id=account_id,
        team=team.lower(),
        env=env_str.lower(),
        region=region.lower(),
    )


class DatabricksWorkflow(TaggedClusterMixin, ChainOfTasks, ABC):
    DATABRICKS_SPARK_VERSION = "13.3.x-scala2.12"

    def __init__(
        self,
        job_name: str,
        cluster_name: str,
        cluster_tags: Dict[str, str],
        worker_node_type: str,
        worker_node_count: int,
        tasks: List[DatabricksJobTask],
        log_uri: Optional[str] = None,
        log_region: Optional[str] = None,
        driver_node_type: Optional[str] = None,
        use_photon: bool = False,
        eldorado_cluster_options_list: Optional[Sequence[Tuple[str, str]]] = None,
        databricks_spark_version: str = DATABRICKS_SPARK_VERSION,
        spark_configs: Optional[Dict[str, str]] = None,
        databricks_instance_profile_arn: Optional[str] = None,
        retries: int = 2,
        retry_delay: timedelta = timedelta(minutes=1),
        ebs_config: Optional[DatabricksEbsConfiguration] = None,
        docker_image: Optional[DatabricksDockerImageOptions] = None,
        region: DatabricksRegion = DEFAULT_REGION,
        autoscaling_config: Optional[DatabricksAutoscalingConfig] = None,
        enable_elastic_disk: bool = False,
        bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None,
        install_ttd_certificates: bool = False,
        use_unity_catalog: bool = False,
    ):
        """
        use_unity_catalog: Set to True if designating job to read/write through Databricks Unity Catalog. If set to
                            True, job authenticates to a team/environment based role with specific catalog permissions.
        """
        task_group_id = job_name
        self.name = job_name
        self.cluster_name = cluster_name
        self.cluster_tags = cluster_tags
        self.eldorado_config_option_pairs_list_for_cluster = eldorado_cluster_options_list
        self.worker_node_type = worker_node_type
        self.worker_node_count = worker_node_count
        self.tasks = tasks
        self.workspace = None if not any(isinstance(task, NotebookDatabricksTask)
                                         for task in self.tasks) else DevUsEastDatabricksWorkspace()
        self.airflow_env = TtdEnvFactory.get_from_system()
        self.env = TtdEnvFactory.get_from_system()
        if self.airflow_env == TtdEnvFactory.dev or isinstance(self.workspace, DevUsEastDatabricksWorkspace):
            self.env = TtdEnvFactory.dev
        elif self.airflow_env == TtdEnvFactory.prod:
            self.env = TtdEnvFactory.prod
        else:
            self.env = TtdEnvFactory.prodTest
        self.use_photon = use_photon
        self.spark_configs = spark_configs or {}
        inferred_team = infer_team_from_call_stack()
        self.team = inferred_team if (inferred_team is not None and inferred_team != DEMO_FOLDER) else ""
        self.region = region
        self.driver_node_type = driver_node_type
        self.enable_elastic_disk = enable_elastic_disk
        self.bootstrap_script_actions = bootstrap_script_actions
        self.log_uri = log_uri or self.region.logs_bucket
        self.log_region = log_region or self.region.aws_region

        # Either try to use the provided instance profile arn or infer from team
        self.databricks_instance_profile_arn = databricks_instance_profile_arn
        if self.databricks_instance_profile_arn is None:

            self.databricks_instance_profile_arn = infer_instance_profile(
                account_id=self.databricks_aws_account_id,
                team=self.team.lower(),
                env=self.env,
                region=self.region.instance_profile_region,
            )

        self.cluster_tags.update({
            "Cloud": "aws",
            "Environment": str(self.env).lower(),
            "Resource": "Databricks",
            "Source": "Airflow",
            "Process": self.cluster_name,
            "JobRunTime": '{{ logical_date.strftime("%Y%m%d%H") }}',
        })

        # set eldorado options and supply defaults needed for eldorado-core to run
        eldorado_configs_for_cluster = (self.eldorado_config_option_pairs_list_for_cluster or []) + [("ttd.env", str(self.env))]
        self.spark_configs["spark.driver.extraJavaOptions"] = " ".join([f"-D{key}={value}" for key, value in eldorado_configs_for_cluster])

        create_and_run_job_task_id = f"{self.name}_create_and_run_job"
        # Create a job with multiple tasks (equivalent of EMR job with multiple steps)
        cluster_key = f"{self.name}_cluster"

        expanded_tasks = [x.get_main_task(cluster_key) for x in tasks]

        # Add new init scripts here
        init_scripts: List[Dict[str, Dict[str, str]]] = []

        if self.bootstrap_script_actions:
            for action in self.bootstrap_script_actions:
                init_scripts.append({"s3": {"destination": action.ScriptBootstrapAction["Path"]}})

        if install_ttd_certificates:
            init_scripts.append({"s3": {"destination": MonitoringConfigurationSpark3().DATABRICKS_IMPORT_TTD_CERTS_PATH}})

        # Fetch all dependent tasks, if they weren't passed in to this already
        all_tasks = {}
        worklist = tasks[:]

        while len(worklist) > 0:
            next = worklist.pop()
            all_tasks[next.task_name] = next

            for dependent in next.depends_on:
                if dependent.task_name not in all_tasks:
                    worklist.append(dependent)

        tasks = list(all_tasks.values())

        private_pip_added = False
        should_configure_openlineage = len(tasks) == 1
        for task in tasks[:]:
            if (isinstance(task, S3PythonDatabricksTask) or isinstance(task, PythonWheelDatabricksTask)) and not private_pip_added:
                init_scripts.append({
                    "s3": {
                        "destination":
                        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.3.2/latest/bootstrap/set-private-pip-repositories.sh"
                    }
                })
                private_pip_added = True

            if should_configure_openlineage:
                # Configure open lineage - we only support this for the time being
                # if this is a single cluster task
                task.configure_openlineage(
                    cluster_key,
                    self.cluster_name,
                    expanded_tasks,
                    init_scripts,
                    spark_configs if spark_configs else {},
                )

        # Creates a job that supports multiple clusters, and has run history in databricks
        # Does not have a schedule - the schedule is enforced via airflow
        # Currently supports 1 cluster intended to be shared by spark and monitoring tasks.
        if self.airflow_env == TtdEnvFactory.prodTest:
            databricks_job_name = f"{self.name}_{get_current_branch_name()}"
        elif self.airflow_env == TtdEnvFactory.prod:
            databricks_job_name = self.name
        else:
            databricks_job_name = f"{self.name}_{self.airflow_env.execution_env}"

        cluster_log_conf = {
            "dbfs": {
                "destination": self.log_uri
            }
        } if self.log_uri.startswith("dbfs") else {
            "s3": {
                "destination": self.log_uri,
                "region": self.log_region
            }
        }

        aws_config = DatabricksAwsConfiguration(
            ebs_config or DatabricksEbsConfiguration(),
            self.databricks_instance_profile_arn,
        )

        cluster_options: Dict[str, Union[str, Dict[str, Any]]] = {
            "job_cluster_key": cluster_key,
            "new_cluster": {
                "spark_version": databricks_spark_version,
                "spark_conf": self.spark_configs,
                "spark_env_vars": {
                    "OPENLINEAGE_ENABLE": "false",  # We use global init scripts till openlineage upgrade applied
                },
                "aws_attributes": aws_config.to_dict(),
                "node_type_id": self.worker_node_type,
                "num_workers": self.worker_node_count,
                "custom_tags": self.cluster_tags,
                "cluster_log_conf": cluster_log_conf,
                "enable_elastic_disk": self.enable_elastic_disk,
                "policy_id": self.databricks_policy_id,
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "PHOTON" if self.use_photon else "STANDARD",
                "init_scripts": init_scripts,
            },
        }

        if self.driver_node_type is not None:
            assert isinstance(cluster_options["new_cluster"], dict)
            cluster_options["new_cluster"]["driver_node_type_id"] = self.driver_node_type

        if private_pip_added:
            assert isinstance(cluster_options["new_cluster"], dict)
            cluster_options["new_cluster"]["spark_env_vars"]["PYPI_TOKEN"] = "{{ '{{secrets/pypi-read/pypi-token}}' }}"

        if autoscaling_config is not None:
            assert isinstance(cluster_options["new_cluster"], dict)
            cluster_options["new_cluster"]["autoscale"] = {
                "min_workers": autoscaling_config.min_workers,
                "max_workers": autoscaling_config.max_workers
            }

        if docker_image:
            assert isinstance(cluster_options["new_cluster"], dict)
            cluster_options["new_cluster"]["docker_image"] = docker_image.to_dict()

        job_parameters = {}
        default_parameters = []
        for task in tasks:
            params = task.task_parameters()
            for i in range(len(params)):
                # Databricks forces us to override each command line arg in the list via providing
                # a parameter. For example, if we have the arguments:
                #
                #     ["foo=bar", "date={{ execution_date }}"]
                #
                # We need to set up a key for each item in the list. The expansion of the templates
                # does not allow us to expand directly in to the list that is provided
                # to each task during the run now. The hack we're going to do then is
                # set up empty keys for each slot in the list, and then during the
                # run now, we override each of those with the proper value and
                # rebuild the list.
                arg_key = f"{task.task_name}-{i}"
                default_parameters.append({"name": arg_key, "default": ""})
                job_parameters[arg_key] = params[i]

        self.create_and_run_job_op = DatabricksCreateAndRunJobOperator(
            name=databricks_job_name,
            databricks_conn_id=self.databricks_conn_id,
            task_id=create_and_run_job_task_id,
            job_clusters=[cluster_options],
            tasks=[x.to_dict() for x in expanded_tasks],
            access_control_list=[
                # Team is pulled from folder structure/location of DAG file.
                {
                    "group_name": "SCRUM-" + self.team.upper(),
                    "permission_level": "CAN_MANAGE_RUN",
                },
                {
                    "group_name": "Developer",
                    "permission_level": "CAN_VIEW",
                },
            ],
            json={"parameters": default_parameters},
            run_job_json={"job_parameters": job_parameters},
            use_unity_catalog=use_unity_catalog,
            team=self.team,
            env=self.env
        )

        watch_run_task_id = f"{self.name}_watch_run"
        self.create_and_run_job_task = OpTask(op=self.create_and_run_job_op)

        self.watch_run_task = OpTask(
            op=DatabricksWatchRunOperator(
                task_id=watch_run_task_id,
                run_id="{{ ti.xcom_pull(task_ids='" + create_and_run_job_task_id + "', key='" + XCOM_RUN_ID_KEY + "')}}",
                databricks_conn_id=self.databricks_conn_id,
                retries=retries,
                retry_delay=retry_delay,
                queue=Workers.k8s.queue,
                pool=Workers.k8s.pool,
                executor_config=K8sExecutorConfig.watch_task(),
                task_names=[x.task_key for x in expanded_tasks]
            )
        )
        self.check_status_task = OpTask(
            op=DatabricksRetryRunOperator(
                task_id=f"retry_{task_group_id}",
                task_group_id=task_group_id,
                databricks_conn_id=self.databricks_conn_id,
                submit_run_task_id=create_and_run_job_task_id,
                watch_run_task_id=watch_run_task_id,
                retries=retries,
                retry_delay=retry_delay,
                task_names=[x.task_key for x in expanded_tasks]
            )
        )

        self.xcom_push_tasks = [
            OpTask(
                op=DatabricksPushXcomOperator(
                    databricks_conn_id=self.databricks_conn_id,
                    submit_run_task_id=create_and_run_job_task_id,
                    task=task,
                )
            ) for task in tasks if task.do_xcom_push
        ]

        super().__init__(
            create_cluster_op_tags=self.create_and_run_job_op.json["job_clusters"][0]["new_cluster"]["custom_tags"],
            max_tag_length=1000,
            include_cluster_name_tag=False,
            # `ClusterName` is reserved tag in Databricks and shouldn't be set,
            # for ref: https://thetradedesk.slack.com/archives/C0BH8DT8B/p1725295026490389?thread_ts=1725281667.353049&cid=C0BH8DT8B
            task_id=job_name,
            tasks=[self.create_and_run_job_task, self.watch_run_task, self.check_status_task] + self.xcom_push_tasks
        )

        # has to happen after super __init__ because that wipes out task group
        self.as_taskgroup(task_group_id)

    @property
    def databricks_aws_account_id(self) -> str:
        if isinstance(self.workspace, DevUsEastDatabricksWorkspace):
            return self.workspace.account_id
        else:
            return Variable.get(f"databricks-aws-account-id-{self.region.workspace_region}", default_var="003576902480")

    @property
    def databricks_policy_id(self) -> str:
        if isinstance(self.workspace, DevUsEastDatabricksWorkspace):
            return self.workspace.policy_id
        else:
            return Variable.get(f"databricks-aws-policy-id-{self.region.workspace_region}", default_var="0002B326F274BB9F")

    @property
    def databricks_conn_id(self) -> str:
        if isinstance(self.workspace, DevUsEastDatabricksWorkspace):
            return self.workspace.conn_id
        else:
            return f"databricks-aws-{self.region.workspace_region}"

    @property
    def allowed_team_specific_instance_profile_arns(self) -> Set[str]:
        var_key = f"databricks-{self.team.lower()}-allowed-instance-profiles"
        var_val = Variable.get(var_key, deserialize_json=True, default_var=None)
        if var_val is None:
            return set()
        elif isinstance(var_val, list):
            return set(var_val)
        else:
            raise ValueError(f"{var_key} is not a list - got {type(var_val)}")

    def _validate_instance_profile_arn(self) -> None:
        team = self.team.lower()

        # Currently account id & region are fixed because global workspaces are not enabled broadly
        # If global roles are ever enabled, we'll need to update this to be workspace aware
        standard_arn = infer_instance_profile(
            account_id=self.databricks_aws_account_id,
            team=team,
            env=self.env,
            region=self.region.instance_profile_region,
        )

        # We are skipping checking the instance profile arn because it will fail during the validate_dags ci step
        # It fails because the variables used in prod/prodTest/staging are not mounted during the ci validation
        # As a result we can not load the list of whitelisted arns causing this to fail.
        #
        # This should not pose a data access risk because at time of execution, the dev env will not have any valid
        # databricks connection to use anyway, since this is just provided in the prodTest and prod envs.
        if self.airflow_env != TtdEnvFactory.dev:
            for job_cluster_spec in self.create_and_run_job_op.json["job_clusters"]:
                instance_profile_arn = job_cluster_spec["new_cluster"]["aws_attributes"]["instance_profile_arn"]
                if instance_profile_arn != standard_arn and instance_profile_arn not in self.allowed_team_specific_instance_profile_arns:
                    raise Exception(
                        f"Instance profile arn {instance_profile_arn} is not valid for team {team}. Expected {standard_arn} or a whitelisted arn"
                    )

    def _adopt_ttd_dag(self, ttd_dag: TtdDag) -> None:
        # add job tag now that we have dag id
        self.create_and_run_job_op.json["max_concurrent_runs"] = ttd_dag.airflow_dag.max_active_runs
        self.generate_and_set_tags(ttd_dag)

        self._validate_instance_profile_arn()

        super()._adopt_ttd_dag(ttd_dag)
