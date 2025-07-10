from datetime import timedelta
from typing import Dict, Any, Optional, Sequence, Tuple, List

from airflow.utils.trigger_rule import TriggerRule

from ttd.alicloud.ali_cluster_sensor import AliClusterSensor
from ttd.alicloud.ali_create_cluster_operator import AliCreateClusterOperator
from ttd.alicloud.ali_delete_cluster_operator import AliDeleteClusterOperator
from ttd.alicloud.ali_hook import Defaults
from ttd.alicloud.ali_livy_sensor import AliLivySensor
from ttd.alicloud.ali_spark_submit_operator import AliSparkSubmitOperator
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.eldorado.base import ClusterSpecs, TtdDag
from ttd.metrics.cluster import ClusterTaskDataBuilder
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from ttd.tasks.base import BaseTask
from ttd.tasks.setup_teardown import SetupTeardownTask
from ttd.ttdenv import TtdEnvFactory, TtdEnv
from ttd.mixins.tagged_cluster_mixin import TaggedClusterMixin
from ttd.eldorado import tag_utils
from alibabacloud_emr20160408 import models as emr_models


class AliClusterSpecs(ClusterSpecs):

    def __init__(
        self,
        cluster_task_id: str,
        cluster_name: str,
        environment: TtdEnv,
        master_instance_type: ElDoradoAliCloudInstanceTypes,
        core_instance_type: ElDoradoAliCloudInstanceTypes,
    ):
        super(AliClusterSpecs, self).__init__(cluster_name, environment)
        self.cluster_name = cluster_name
        self.cluster_id = ("{{ task_instance.xcom_pull(task_ids='" + cluster_task_id + "', key='return_value') }}")
        self.environment = environment
        self.master_instance_type = master_instance_type
        self.core_instance_type = core_instance_type


class AliCloudClusterTask(TaggedClusterMixin, SetupTeardownTask):
    """
    El-Dorado AliCloud EMR cluster

    @param name: Name of the cluster
    @param master_instance_type: Type instance and the number of virtual machines for head nodes
    @param core_instance_type: Type instance and the number of virtual machines for worker nodes
    @param environment: Execution environment
    @param region: Region for the cluster
    @param cluster_tags: Tags for the cluster
    @param emr_version: Version of emr cluster
    @param retries: Number of times to retry cluster upon failure.
    @param retry_delay: Delay between cluster retries.
    """

    def __init__(
        self,
        name: str,
        master_instance_type: ElDoradoAliCloudInstanceTypes,
        core_instance_type: ElDoradoAliCloudInstanceTypes,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        region: str = Defaults.CN4_REGION_ID,
        cluster_tags: Optional[Dict[str, str]] = None,
        emr_version: str = AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2,
        retries: Optional[int] = 1,
        retry_delay: Optional[timedelta] = timedelta(minutes=20),
        emr_default_config: Optional[List[emr_models.CreateClusterV2RequestConfig]] = None,
    ):
        if not AliCloudEmrVersions.is_spark_3(emr_version):
            from ttd.eldorado.aws.emr_cluster_task import ClusterVersionNotSupported
            raise ClusterVersionNotSupported(f"AliCloud EMR version={emr_version} isn't supported. Use higher version supporting Spark 3")
        self.name = name
        self.environment = environment
        self.region = region
        self.emr_version = emr_version
        self.master_instance_type = master_instance_type
        self.core_instance_type = core_instance_type

        self.cluster_tags = cluster_tags or {}

        self.cluster_tags.update({
            "Environment": self.environment.execution_env,
            "Process": self.name,
            "Resource": "EMR",
            "Source": "Airflow",
            "ClusterVersion": self.emr_version,
        })

        self.cluster_tags = tag_utils.add_creator_tag(self.cluster_tags)

        self._cluster_config = AliClusterSpecs(
            cluster_name=self.name,
            cluster_task_id=self.create_cluster_task_id(self.name),
            environment=self.environment,
            master_instance_type=self.master_instance_type,
            core_instance_type=self.core_instance_type,
        )

        cluster_config = self.default_cluster_config()

        cluster_task_data = ClusterTaskDataBuilder(name).for_alicloud(master_instance_type, core_instance_type).build()

        create_cluster_task = OpTask(
            op=AliCreateClusterOperator(
                task_id=self.create_cluster_task_id(name=name),
                cluster_name=name,
                cluster_config=cluster_config,
                region=region,
                cluster_tags=self.cluster_tags,
                master_instance_type=master_instance_type,
                core_instance_type=core_instance_type,
                cluster_task_data=cluster_task_data,
                emr_default_config=emr_default_config,
            )
        )

        self.cluster_id = self.get_cluster_id(name)

        wait_cluster_task = OpTask(
            op=AliClusterSensor(
                task_id=f"wait_cluster_creation_{name}",
                cluster_id=self.cluster_id,
                region=self.region,
                poke_interval=60 * 1,
                timeout=60 * 60,
            )
        )

        cluster_creation_task = ChainOfTasks(
            task_id="cluster_creation_task",
            tasks=[create_cluster_task, wait_cluster_task],
        ).as_taskgroup(f"setup_{name}")

        terminate_cluster_task = OpTask(
            op=AliDeleteClusterOperator(
                task_id=f"kill_cluster_{name}",
                cluster_id=self.cluster_id,
                region=self.region,
                # Trigger the cluster's termination when all the steps are done, regardless of whether they failed or succeeded
                trigger_rule=TriggerRule.ALL_DONE,
            )
        )

        super().__init__(
            create_cluster_op_tags=self.cluster_tags,
            max_tag_length=128,
            task_id=name,
            setup_task=cluster_creation_task,
            teardown_task=terminate_cluster_task,
            retries=retries,
            retry_delay=retry_delay,
        )

    @staticmethod
    def get_cluster_id(name: str):
        return ("{{ task_instance.xcom_pull('" + AliCloudClusterTask.create_cluster_task_id(name) + "', key='return_value') }}")

    def _adopt_ttd_dag(self, ttd_dag: TtdDag):
        super(AliCloudClusterTask, self)._adopt_ttd_dag(ttd_dag)
        self.generate_and_set_tags(ttd_dag)

    @staticmethod
    def create_cluster_task_id(name: str) -> str:
        return f"create_cluster_{name}"

    @property
    def cluster_specs(self) -> AliClusterSpecs:
        return self._cluster_config

    def add_parallel_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        super().add_parallel_body_task(body_task)

        if isinstance(body_task, AliCloudJobTask):
            body_task.set_cluster_specs(self.cluster_specs)

        return self

    def add_sequential_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        super().add_sequential_body_task(body_task)

        if isinstance(body_task, AliCloudJobTask):
            body_task.set_cluster_specs(self.cluster_specs)

        return self

    def default_cluster_config(self) -> Dict[str, str]:
        return {
            "region_id": self.region,
            "emr_version": self.emr_version,
            "log_bucket_dir": "ttd-eldorado",
        }


class AliCloudJobTask(ChainOfTasks):
    JAR_PATH = "oss://ttd-build-artefacts/eldorado/snapshots/master/latest/el-dorado-assembly.jar"

    def __init__(
        self,
        name: str,
        class_name: str,
        cluster_spec: Optional[AliClusterSpecs] = None,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        jar_path: str = None,
        configure_cluster_automatically: bool = False,
        command_line_arguments: Optional[List[str]] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
    ):
        """
        El-Dorado AliCloud Job Task

        @param name: Name of the task
        @param class_name: Spark class to be executed
        @param cluster_spec: The cluster config where the job gets executed
        @param eldorado_config_option_pairs_list: Configuration option for eldorado
        @param additional_args_option_pairs_list: Additional configuration option for Spark
        @param jar_path: File path for the application to be executed
        @param configure_cluster_automatically: Flag for enabling spark default configuration
        @param cluster_calc_defaults: Used for getting the spark default configuration
        @param command_line_arguments: Command line arguments for the application
        """
        self.name = name
        self.class_name = class_name
        self.cluster_spec = cluster_spec
        self.eldorado_config_option_pairs_list = eldorado_config_option_pairs_list
        self.additional_args_option_pairs_list = (
            dict((x, y) for x, y in additional_args_option_pairs_list) if additional_args_option_pairs_list is not None else dict()
        )
        self.jar_path = jar_path or self.JAR_PATH
        self.configure_cluster_automatically = configure_cluster_automatically
        self.cluster_calc_defaults = cluster_calc_defaults
        self.command_line_arguments = command_line_arguments or []

        self.add_job_task = OpTask()
        self.watch_job_task = OpTask()

        super().__init__(task_id=name, tasks=[self.add_job_task, self.watch_job_task])
        if self.cluster_spec:
            self._add_job(spark_submit_args=self._build_spark_conf())

    def set_cluster_specs(self, cluster_config: AliClusterSpecs):
        if not cluster_config:
            raise Exception("cluster_config must not be None")

        if self.cluster_spec:
            # do nothing. existing config is not overwritten. We should log a warning
            return

        self.cluster_spec = cluster_config
        self._add_job(spark_submit_args=self._build_spark_conf())

        for d in self._downstream:
            if isinstance(d, AliCloudJobTask):
                d.set_cluster_specs(cluster_config)

    def _add_job(self, spark_submit_args: Dict[str, Any]):
        task_add_id = f"{self.cluster_spec.cluster_name}_add_task_{self.name}"
        task_watch_id = f"{self.cluster_spec.cluster_name}_watch_task_{self.name}"

        self.add_job_task.set_op(
            op=AliSparkSubmitOperator(
                task_id=task_add_id,
                cluster_id=self.cluster_spec.cluster_id,
                jar_loc=self.jar_path,
                job_class=self.class_name,
                config_option=spark_submit_args,
                driver_memory=spark_submit_args["spark.driver.memory"],
                driver_cores=spark_submit_args["spark.driver.cores"],
                executor_memory=spark_submit_args["spark.executor.memory"],
                executor_cores=spark_submit_args["spark.executor.cores"],
                command_line_arguments=self.command_line_arguments,
            )
        )

        self.watch_job_task.set_op(
            op=AliLivySensor(
                task_id=task_watch_id,
                cluster_id=self.cluster_spec.cluster_id,
                batch_id="{{ task_instance.xcom_pull(task_ids='" + task_add_id + "', key='return_value') }}",
                poke_interval=60 * 1,
            )
        )

    def _build_spark_conf(self) -> Dict[str, Any]:
        # Java conf args
        eldorado_config_option_pairs_list = self.eldorado_config_option_pairs_list or []
        java_args = [("ttd.env", self.cluster_spec.environment.execution_env), ("log4j2.formatMsgNoLookups", "true"),
                     ("ttd.cluster-service", "AliEMR"),
                     ("javax.net.ssl.trustStorePassword", "{{ conn.aerospike_truststore_password.get_password() }}"),
                     ("javax.net.ssl.trustStore", "/tmp/ttd-internal-root-ca-truststore.jks")]
        java_args += eldorado_config_option_pairs_list
        java_args_string = " ".join([f"-D{option_pair[0]}={option_pair[1]}" for option_pair in java_args])
        spark_conf = {"spark.driver.extraJavaOptions": java_args_string}

        # Additional conf args
        additional_args_list: Dict[str, str] = (self.additional_args_option_pairs_list or [])
        if (self.configure_cluster_automatically or not self.additional_args_option_pairs_list):
            worker_instance_type = self.cluster_spec.core_instance_type
            worker_instance_count = self.cluster_spec.core_instance_type.node_count

            additional_args_list = self.get_spark_conf(
                additional_args_list=additional_args_list,
                worker_instance_type=worker_instance_type,
                worker_instance_count=worker_instance_count,
                defaults=self.cluster_calc_defaults,
            )
            spark_conf.update(additional_args_list)
        return spark_conf

    @staticmethod
    def get_spark_conf(
        additional_args_list: Dict[str, str],
        worker_instance_type: ElDoradoAliCloudInstanceTypes,
        worker_instance_count: int,
        defaults: ClusterCalcDefaults,
    ) -> Dict[str, Any]:
        resources_conf = worker_instance_type.instance_type.calc_cluster_params(
            worker_instance_count,
            defaults.parallelism_factor,
            defaults.min_executor_memory,
            defaults.max_cores_executor,
            defaults.memory_tolerance,
        ).to_spark_arguments()

        spark_conf_args = resources_conf["conf_args"]
        spark_conf_args.update(additional_args_list)

        return spark_conf_args

    def __rshift__(self, other: BaseTask) -> BaseTask:
        super().__rshift__(other)

        if self.cluster_spec and isinstance(other, AliCloudJobTask):
            other.set_cluster_specs(self.cluster_spec)

        return other
