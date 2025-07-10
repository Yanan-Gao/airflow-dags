import logging
from datetime import timedelta
from typing import Dict, Optional, List, Sequence, Tuple, Any

from airflow.models import Variable

from ttd.azure_vm.hdi_instance_types import HDIInstanceType
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.eldorado.azure.hdi_cluster_specs import HdiClusterSpecs
from ttd.eldorado.azure.hdi_cluster_config import HdiClusterConfig
from ttd.eldorado.hdiversion import HDIClusterVersions, HDIVersion
from ttd.eldorado.base import TtdDag
from ttd.hdinsight.livy_sensor import LivySensor
from ttd.hdinsight.livy_spark_submit_operator import LivySparkSubmitOperator
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.operators.azure_hadoop_logs_parser_operator import AzureHadoopLogsParserOperator
from ttd.metrics.cluster import ClusterTaskDataBuilder
from ttd.operators.task_group_retry_operator import SetupTaskRetryOperator
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from ttd.tasks.base import BaseTask
from ttd.tasks.setup_teardown import SetupTeardownTask
from ttd.hdinsight.hdi_cluster_sensor import HDIClusterSensor
from ttd.hdinsight.hdi_create_cluster_operator import HDICreateClusterOperator
from ttd.hdinsight.hdi_delete_cluster_operator import HDIDeleteClusterOperator
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.hdinsight.script_action_spec import HdiVaultScriptActionSpec, HdiScriptActionSpecBase, ScriptActionSpec
from ttd.ttdenv import TtdEnvFactory, TtdEnv
from ttd.mixins.tagged_cluster_mixin import TaggedClusterMixin
from ttd.workers.worker import Workers
from ttd.eldorado import tag_utils

HTTPS_SCRIPTS_ROOT = "https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/azure-scripts"
JKS_CERTS_LOCATION = "/tmp/ttd-internal-root-ca-truststore.jks"


class HDIClusterTask(TaggedClusterMixin, SetupTeardownTask):
    """
    El-Dorado Azure HDInsight cluster

    @param name: Name of the cluster
    @param vm_config: Type instance and the number of virtual machines for worker and head nodes
    @param environment: Execution environment
    @param region: Region for the cluster
    @param cluster_tags: Tags for the cluster
    @param retries: Number of times to retry the cluster creation tasks upon failure. If retry_only_on_creation_failure
    is set to True, this value is the number of times to retry the whole cluster upon failure of any of the tasks.
    @param retry_delay: Delay between cluster retries.
    @param retry_exponential_backoff: If true, double the retry delay between each successive retry.
    @param retry_only_on_creation_failure: When set to true (the default), we will only automatically retry when we fail to create a cluster. Spark job failures will not be retried
    """

    def __init__(
        self,
        name: str,
        vm_config: HDIVMConfig,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        region: str = "eastus",
        cluster_tags: Optional[Dict[str, str]] = None,
        cluster_version: HDIVersion = HDIClusterVersions.AzureHdiSpark33,
        extra_script_actions: Optional[List[HdiScriptActionSpecBase]] = None,
        permanent_cluster: bool = False,
        enable_openlineage: bool = True,
        retries: Optional[int] = 2,
        retry_delay: Optional[timedelta] = timedelta(minutes=20),
        retry_exponential_backoff: bool = False,
        retry_only_on_creation_failure: bool = True,
        enable_docker: bool = False,
        post_docker_install_scripts: Optional[List[HdiScriptActionSpecBase]] = None,
    ):
        self.name = name
        self.environment = environment
        self.region = region
        self.vm_config = vm_config
        self.enable_openlineage = enable_openlineage
        self.enable_docker = enable_docker

        self.cluster_tags = cluster_tags or {}

        self.cluster_tags.update({
            "Environment": self.environment.execution_env,
            "Resource": "HDInsight",
            "Job": self.name,
            "Source": "Airflow",
            "ClusterLifecycle": "Permanent" if permanent_cluster else "OnDemand",
            "ClusterVersion": cluster_version.version
        })
        self.cluster_tags = tag_utils.add_creator_tag(self.cluster_tags)

        if post_docker_install_scripts is None:
            post_docker_install_scripts = []

        default_cluster_config = self.default_cluster_config()

        vault_script_action_spec = HdiVaultScriptActionSpec(
            cluster_version=cluster_version, artefacts_storage_account=default_cluster_config.artefacts_storage_account
        )

        download_jks_certs = [
            ScriptActionSpec(
                action_name="download_root_ca",
                script_uri=f"{HTTPS_SCRIPTS_ROOT}/download_from_url.sh",
                parameters=[
                    f"'{HTTPS_SCRIPTS_ROOT}/ttd-internal-root-ca-truststore.jks?{{{{conn.azure_ttd_build_artefacts_sas.get_password()}}}}'",
                    JKS_CERTS_LOCATION,
                ]
            )
        ]

        script_action_specs = extra_script_actions or []
        script_action_specs.append(vault_script_action_spec)
        script_action_specs.extend(download_jks_certs)

        if enable_docker:
            # First we have to install docker, and then
            # we run a separate script to configure yarn to use docker
            script_action_specs.append(
                ScriptActionSpec(action_name="install_docker", script_uri=f"{HTTPS_SCRIPTS_ROOT}/install-docker.sh", parameters=[])
            )

            script_action_specs.append(
                ScriptActionSpec(
                    action_name="configure_docker_on_yarn",
                    script_uri=f"{HTTPS_SCRIPTS_ROOT}/configure-docker-on-yarn-controller.sh",
                    parameters=[]
                )
            )

            script_action_specs.extend(post_docker_install_scripts)

        cluster_task_data = ClusterTaskDataBuilder(name).for_azure(vm_config).build()

        create_cluster_task = OpTask(
            op=HDICreateClusterOperator(
                task_id=self.create_cluster_task_id(name=name),
                cluster_name=name,
                cluster_version=cluster_version,
                cluster_config=default_cluster_config,
                vm_config=self.vm_config,
                region=region,
                cluster_tags=self.cluster_tags,
                permanent_cluster=permanent_cluster,
                wait_for_creation=False,
                script_actions_specs=script_action_specs,
                enable_openlineage=self.enable_openlineage,
                enable_docker=self.enable_docker,
                execution_timeout=timedelta(minutes=5),
                cluster_task_data=cluster_task_data,
            )
        )

        self._cluster_specs = HdiClusterSpecs(
            cluster_name=self.name,
            cluster_task_id=self.create_cluster_task_id(self.name),
            worker_instance_type=self.vm_config.workernode_type,
            worker_instance_num=self.vm_config.num_workernode,
            environment=self.environment,
            storage_account=default_cluster_config.artefacts_storage_account,
            rest_conn_id=default_cluster_config.rest_conn_id,
        )

        wait_cluster_task = OpTask(
            op=HDIClusterSensor(
                task_id=f'{HDIClusterSensor.STEP_NAME}_{name}',
                cluster_name="{{ task_instance.xcom_pull(task_ids='" + self.create_cluster_task_id(name=name) + "', key='return_value') }}",
                cluster_task_name=self.name,
                rest_conn_id=self._cluster_specs.rest_conn_id,
                resource_group=default_cluster_config.resource_group,
                poke_interval=60 * 1,
                retries=1,
                retry_delay=timedelta(minutes=1),
                timeout=2 * 60 * 60,
                queue=Workers.k8s.queue,
                pool=Workers.k8s.pool,
                executor_config=K8sExecutorConfig.watch_task(),
            )
        )

        cluster_creation_task = ChainOfTasks(
            task_id="cluster_creation_task",
            tasks=[create_cluster_task, wait_cluster_task],
        ).as_taskgroup(f"setup_{name}")

        terminate_cluster_task = OpTask(
            op=HDIDeleteClusterOperator(
                task_id=f'{HDIDeleteClusterOperator.STEP_NAME}_{name}',
                cluster_name="{{ task_instance.xcom_pull(task_ids='" + self.create_cluster_task_id(name=name) + "', key='return_value') }}",
                cluster_config=default_cluster_config,
                permanent_cluster=permanent_cluster,
                retries=1,
                execution_timeout=timedelta(hours=2),
            )
        )

        super().__init__(
            create_cluster_op_tags=self.cluster_tags,
            max_tag_length=256,
            task_id=name,
            setup_task=cluster_creation_task,
            teardown_task=terminate_cluster_task,
            retries=retries,
            retry_delay=retry_delay,
            retry_exponential_backoff=retry_exponential_backoff,
            custom_retry_op=SetupTaskRetryOperator if retry_only_on_creation_failure else None
        )

    def _adopt_ttd_dag(self, ttd_dag: TtdDag):
        super(HDIClusterTask, self)._adopt_ttd_dag(ttd_dag)
        self.generate_and_set_tags(ttd_dag)

    @staticmethod
    def create_cluster_task_id(name: str) -> str:
        return f"{HDICreateClusterOperator.STEP_NAME}_{name}"

    @property
    def cluster_specs(self) -> HdiClusterSpecs:
        return self._cluster_specs

    def add_parallel_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        super().add_parallel_body_task(body_task)

        if isinstance(body_task, HDIJobTask):
            body_task.set_cluster_specs(self.cluster_specs)

        return self

    def add_sequential_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        super().add_sequential_body_task(body_task)

        if isinstance(body_task, HDIJobTask):
            body_task.set_cluster_specs(self.cluster_specs)

        return self

    @staticmethod
    def default_cluster_config() -> HdiClusterConfig:
        virtual_network_profile = Variable.get(
            "AZ_VIRTUAL_NETWORK_PROFILE",
            default_var={
                "ID":
                "/subscriptions/001a3882-eb1c-42ac-9edc-5e2872a07783/resourceGroups/va9-network-base-rg/providers/Microsoft"
                ".Network/virtualNetworks/va9-vnet",
                "SUBNET_NAME":
                "/subscriptions/001a3882-eb1c-42ac-9edc-5e2872a07783/resourceGroups/va9-network-base-rg/providers"
                "/Microsoft.Network/virtualNetworks/va9-vnet/subnets/Prod_Eldorado_v2_Public_prod"
            },
            deserialize_json=True,
        )
        logs_storage_account = Variable.get(
            "AZ_ELDORADO_LOGS_SA",
            default_var={
                "uri":
                "ttdeldorado.dfs.core.windows.net",
                "resource_id":
                "/subscriptions/001a3882-eb1c-42ac-9edc-5e2872a07783/resourceGroups/eldorado-rg/providers/Microsoft.Storage"
                "/storageAccounts/ttdeldorado"
            },
            deserialize_json=True
        )
        return HdiClusterConfig(
            resource_group="eldorado-rg",
            virtual_network_profile_id=virtual_network_profile["ID"],
            virtual_network_profile_subnet=virtual_network_profile["SUBNET_NAME"],
            logs_storage_account=logs_storage_account["uri"],
            logs_storage_account_resource_id=logs_storage_account["resource_id"],
            artefacts_storage_account=Variable.get("AZ_ELDORADO_ARTEFACTS_SA", default_var="ttdartefacts.dfs.core.windows.net"),
            msi_resource_id=Variable.get(
                "AZ_ELDORADO_MI",
                default_var="/subscriptions/001a3882-eb1c-42ac-9edc-5e2872a07783"
                "/resourceGroups/eldorado-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/eldorado-mi"
            ),
            rest_conn_id="azure_hdinsight_rest",
            ssh_conn_id="azure_hdinsight_ssh",
        )


class HDIJobTask(ChainOfTasks):
    JAR_PATH_PATTERN = "abfs://ttd-build-artefacts@{storage_account}/eldorado/snapshots/master/latest/el-dorado-assembly.jar"

    def __init__(
        self,
        name: str,
        class_name: Optional[str],
        cluster_specs: Optional[HdiClusterSpecs] = None,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        jar_path: Optional[str] = None,
        configure_cluster_automatically: bool = False,
        command_line_arguments: Optional[List[str]] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
        is_pyspark: bool = False,
        is_docker: bool = False,
        watch_step_timeout: Optional[timedelta] = None,
        print_parsed_spark_driver_logs: bool = False,
    ):
        """
        El-Dorado HDInsight Job Task

        @param name: Name of the task
        @param class_name: Spark class to be executed
        @param cluster_specs: The cluster config where the job gets executed
        @param eldorado_config_option_pairs_list: Configuration option for eldorado
        @param additional_args_option_pairs_list: Additional configuration option for Spark
        @param jar_path: File path for the application to be executed
        @param configure_cluster_automatically: Flag for enabling spark default configuration
        @param cluster_calc_defaults: Used for getting the spark default configuration
        @param command_line_arguments: Command line arguments for the application
        @param watch_step_timeout: How long the watch task will poll the cluster for before timing out
        @param print_parsed_spark_driver_logs: Flag to print parsed Spark driver logs into the Airflow UI
        """
        self.name = name
        self.class_name = class_name
        self.cluster_specs = cluster_specs
        self.eldorado_config_option_pairs_list = eldorado_config_option_pairs_list
        self.additional_args_option_pairs: Dict[str, Any] = {"conf_args": {}, "args": {}}
        if additional_args_option_pairs_list:
            for x, y in additional_args_option_pairs_list:
                if x == "conf":
                    split = y.split('=', 1)
                    right_hand_side = split[1]
                    try:
                        right_hand_side = int(right_hand_side)
                    except ValueError as verr:
                        pass

                    self.additional_args_option_pairs["conf_args"][split[0]] = right_hand_side

                elif x.startswith("spark."):
                    self.additional_args_option_pairs["conf_args"][x] = y

                else:
                    self.additional_args_option_pairs["args"][x] = y

        self.jar_path = jar_path or self.JAR_PATH_PATTERN.format(storage_account=cluster_specs.artefacts_storage_account)
        self.configure_cluster_automatically = configure_cluster_automatically
        self.cluster_calc_defaults = cluster_calc_defaults
        self.command_line_arguments = command_line_arguments or []
        self.watch_step_timeout = watch_step_timeout
        self.is_pyspark = is_pyspark
        self.is_docker = is_docker
        self.print_parsed_spark_driver_logs = print_parsed_spark_driver_logs

        self.add_job_task = OpTask()
        self.watch_job_task = OpTask()
        self.parse_job_task = OpTask()

        super().__init__(task_id=name, tasks=[self.add_job_task, self.watch_job_task, self.parse_job_task])
        if self.cluster_specs:
            self._add_job(spark_submit_args=self._build_spark_conf())

    def set_cluster_specs(self, cluster_config: HdiClusterSpecs):
        if not cluster_config:
            raise Exception("cluster_config must not be None")

        if self.cluster_specs:
            logging.warning("Cluster config is already set, won't overwrite")
            return

        self.cluster_specs = cluster_config
        self._add_job(spark_submit_args=self._build_spark_conf())

        for d in self._downstream:
            if isinstance(d, HDIJobTask):
                d.set_cluster_specs(cluster_config)

    def _add_job(self, spark_submit_args: Dict[str, Any]):
        task_add_id = f"{self.cluster_specs.cluster_name}_{LivySparkSubmitOperator.STEP_NAME}_{self.name}"
        task_watch_id = f"{self.cluster_specs.cluster_name}_{LivySensor.STEP_NAME}_{self.name}"
        task_parse_id = f"{self.cluster_specs.cluster_name}_{AzureHadoopLogsParserOperator.STEP_NAME}_{self.name}"

        self.add_job_task.set_op(
            op=LivySparkSubmitOperator(
                task_id=task_add_id,
                cluster_name=self.cluster_specs.full_cluster_name,
                livy_conn_id=self.cluster_specs.rest_conn_id,
                jar_loc=self.jar_path,
                job_class=None if self.is_pyspark else self.class_name,
                config_option=spark_submit_args,
                driver_memory=spark_submit_args["spark.driver.memory"],
                driver_cores=int(spark_submit_args["spark.driver.cores"]),
                executor_memory=spark_submit_args["spark.executor.memory"],
                executor_cores=int(spark_submit_args["spark.executor.cores"]),
                command_line_arguments=self.command_line_arguments,
                execution_timeout=timedelta(minutes=5),
                retries=2,
            )
        )

        self.watch_job_task.set_op(
            op=LivySensor(
                task_id=task_watch_id,
                cluster_name=self.cluster_specs.full_cluster_name,
                batch_id="{{ task_instance.xcom_pull(task_ids='" + task_add_id + "', key='return_value') }}",
                livy_conn_id=self.cluster_specs.rest_conn_id,
                poke_interval=60 * 1,
                retries=1,
                retry_delay=timedelta(minutes=1),
                execution_timeout=self.watch_step_timeout,
            )
        )

        self.parse_job_task.set_op(
            op=AzureHadoopLogsParserOperator(
                task_id=task_parse_id,
                app_id="{{ task_instance.xcom_pull(task_ids='" + task_watch_id + "', key='app_id') }}",
                cluster_name=self.cluster_specs.full_cluster_name,
                trigger_rule="none_skipped",
                print_parsed_spark_driver_logs=self.print_parsed_spark_driver_logs,
            )
        )

    def _build_spark_conf(self) -> Dict[str, Any]:
        eldorado_config_option_pairs_list = self.eldorado_config_option_pairs_list or []
        java_args = [("ttd.env", self.cluster_specs.environment.execution_env), ("log4j2.formatMsgNoLookups", "true"),
                     ("ttd.azure.enable", "true"), ("ttd.cluster-service", "HDInsight"),
                     ("javax.net.ssl.trustStorePassword", "{{ conn.aerospike_truststore_password.get_password() }}"),
                     ("javax.net.ssl.trustStore", JKS_CERTS_LOCATION)]
        java_args += eldorado_config_option_pairs_list
        java_args_string = " ".join([f"-D{option_pair[0]}={option_pair[1]}" for option_pair in java_args])
        spark_conf = {"spark.driver.extraJavaOptions": java_args_string + " -XX:MaxJavaStackTraceDepth=200 "}

        # Additional conf args
        additional_args: Dict[str, Any] = self.additional_args_option_pairs or {"conf_args": {}, "args": {}}

        if self.configure_cluster_automatically or self.is_pyspark or self.is_docker or not self.additional_args_option_pairs:
            worker_instance_type = self.cluster_specs.worker_instance_type
            worker_instance_count = self.cluster_specs.worker_instance_num

            additional_args["conf_args"] = self.get_spark_conf(
                additional_args_list=additional_args["conf_args"],
                worker_instance_type=worker_instance_type,
                worker_instance_count=worker_instance_count,
                defaults=self.cluster_calc_defaults
            )

        spark_conf.update(additional_args["conf_args"])

        # Patch the extra arguments that they provided in args over to the equivalent ones, if they exist,
        # since eventually the underlying livy operator is going to want these, and doesn't explicitly
        # handle these args generically.
        standalone_args = additional_args["args"]
        patch_table = {
            "driver-memory": "spark.driver.memory",
            "driver-cores": "spark.driver.cores",
            "executor-memory": "spark.executor.memory",
            "executor-cores": "spark.executor.cores",
            "num-executors": "spark.executor.instances",
            "master": "spark.master",
            "deploy-mode": "spark.deploy.mode"
        }

        for old, new in patch_table.items():
            if old in standalone_args:
                spark_conf[new] = standalone_args[old]

        return spark_conf

    @staticmethod
    def get_spark_conf(
        additional_args_list: Dict[str, str], worker_instance_type: HDIInstanceType, worker_instance_count: int,
        defaults: ClusterCalcDefaults
    ) -> Dict[str, Any]:
        resources_conf = worker_instance_type.calc_cluster_params(
            worker_instance_count, defaults.parallelism_factor, defaults.min_executor_memory, defaults.max_cores_executor,
            defaults.memory_tolerance
        ).to_spark_arguments()

        spark_conf_args = resources_conf["conf_args"]
        spark_conf_args.update(additional_args_list)

        return spark_conf_args

    def __rshift__(self, other: BaseTask) -> BaseTask:
        super().__rshift__(other)

        if self.cluster_specs and isinstance(other, HDIJobTask):
            other.set_cluster_specs(self.cluster_specs)

        return other
