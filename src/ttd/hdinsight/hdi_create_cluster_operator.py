import logging
import random
import string
from datetime import datetime
from typing import Dict, Any, List, Optional, Sequence
import hashlib

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, TaskInstance, BaseOperatorLink, Variable
from airflow.utils.xcom import XCOM_RETURN_KEY
from azure.core.exceptions import HttpResponseError
from azure.mgmt.hdinsight.models import (
    ClusterCreateProperties,
    OSType,
    Tier,
    ClusterDefinition,
    HardwareProfile,
    OsProfile,
    LinuxOperatingSystemProfile,
    VirtualNetworkProfile,
    Role,
    ComputeProfile,
    StorageProfile,
    StorageAccount,
    ScriptAction,
    DataDisksGroups,
)

from ttd.eldorado.azure.hdi_cluster_config import HdiClusterConfig
from ttd.eldorado.hdiversion import HDIVersion
from ttd.hdinsight.script_action_spec import HdiScriptActionSpecBase
from ttd.hdinsight.hdi_hook import HDIHook, AZ_VERBOSE_LOGS_VAR, WARNING_LEVEL
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.metrics.cluster_lifecycle import ClusterTaskData, ClusterLifecycleMetricPusher
from ttd.ttdenv import TtdEnv, TtdEnvFactory


class HDILink(BaseOperatorLink):
    name = "HDI Cluster"

    def get_link(self, operator: "HDICreateClusterOperator", dttm: datetime):  # type: ignore
        ti = TaskInstance(task=operator, execution_date=dttm)
        cluster_name = ti.xcom_pull(key=XCOM_RETURN_KEY, task_ids=operator.task_id)
        return self.format_link(cluster_name)

    @classmethod
    def format_link(cls, cluster_name: str) -> str:
        return (
            "https://portal.azure.com/#@ops.adsrvr.org/resource"
            "/subscriptions/001a3882-eb1c-42ac-9edc-5e2872a07783"
            "/resourceGroups/eldorado-rg"
            "/providers/Microsoft.HDInsight"
            f"/clusters/{cluster_name}"
            "/overview"
        )


class HDILogContainer(BaseOperatorLink):
    name = "HDI Log Container"

    def get_link(self, operator: "HDICreateClusterOperator", dttm: datetime):  # type: ignore
        ti = TaskInstance(task=operator, execution_date=dttm)
        cluster_name = ti.xcom_pull(key=XCOM_RETURN_KEY, task_ids=operator.task_id)
        return (
            "https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview"
            "/storageAccountId/%2Fsubscriptions%2F001a3882-eb1c-42ac-9edc-5e2872a07783"
            "%2FresourceGroups%2Feldorado-rg%2Fproviders%2FMicrosoft.Storage"
            "%2FstorageAccounts%2Fttdeldorado"
            f"/path/{cluster_name}-container"
        )


class HDICreateClusterOperator(BaseOperator):
    """
    An operator which creates an HDInsight cluster.

    @param cluster_name: The name of the cluster (Due to the naming requirements, the name will be prefixed with 6 random letters/numbers)
    @type cluster_name: str
    @param cluster_config: Contains resource group, virtual network, storage account, credentials.
    @param vm_config: Contains the type and the number of VMs for worker and head nodes.
    @type vm_config: HDIVMConfig
    @param region: The region of the cluster
    @type region: str
    @param cluster_tags: Tags of the cluster
    @type cluster_tags: Dict[str, str]
    @param script_actions: Contains a list of script actions
    @type script_actions: List[ScriptAction]
    """

    STEP_NAME = "create_cluster"

    operator_extra_links = (HDILink(), HDILogContainer())
    template_fields: Sequence[str] = ("cluster_tags", )

    def __init__(
        self,
        cluster_name: str,
        cluster_version: HDIVersion,
        cluster_config: HdiClusterConfig,
        vm_config: HDIVMConfig,
        region: str,
        cluster_task_data: ClusterTaskData,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        script_actions_specs: Optional[List[HdiScriptActionSpecBase]] = None,
        cluster_tags: Optional[Dict[str, str]] = None,
        permanent_cluster: bool = False,
        wait_for_creation: bool = False,
        enable_openlineage: bool = True,
        enable_docker: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.script_actions_specs = script_actions_specs
        self.cluster_name = cluster_name
        self.cluster_config = cluster_config
        self.vm_config = vm_config
        self.region = region
        self.cluster_task_data = cluster_task_data
        self.cluster_tags = cluster_tags
        self.cluster_version = cluster_version
        self.permanent_cluster = permanent_cluster
        self.environment = environment
        self.wait_for_creation = wait_for_creation
        self.enable_openlineage = enable_openlineage
        self.enable_docker = enable_docker

        if self.cluster_tags is None:
            self.cluster_tags = {}

    @property
    def resource_group(self) -> str:
        return self.cluster_config.resource_group

    def pre_execute(self, context):
        az_verbose_logs_level = Variable.get(AZ_VERBOSE_LOGS_VAR, default_var=WARNING_LEVEL)
        logging.getLogger("azure").setLevel(az_verbose_logs_level)

    def execute(self, context: Dict[str, Any]) -> str:  # type: ignore
        from ttd.hdinsight.hdi_metrics import send_hdi_metrics

        hdi_hook = HDIHook(region=self.region)

        full_cluster_name = (
            self.add_hashed_prefix(self.cluster_name, self.environment)
            if self.permanent_cluster else self.add_random_prefix(self.cluster_name)
        )

        if self.permanent_cluster and hdi_hook.check_cluster_exists(full_cluster_name, self.resource_group):
            return full_cluster_name

        rest_conn = BaseHook.get_connection(self.cluster_config.rest_conn_id)
        ssh_conn = BaseHook.get_connection(self.cluster_config.ssh_conn_id)

        script_actions = [spec.to_azure_script_action() for spec in self.script_actions_specs]  # type: ignore

        cluster_params = self.get_cluster_properties(
            cluster_version=self.cluster_version,
            container_name=full_cluster_name + "-container",
            cluster_config=self.cluster_config,
            vm_config=self.vm_config,
            script_action_list=script_actions,
            rest_login=rest_conn.login,
            rest_password=rest_conn.get_password(),
            ssh_login=ssh_conn.login,
            ssh_password=ssh_conn.get_password(),
            enable_openlineage=self.enable_openlineage
        )

        self.log.info(f"Creating cluster, name='{full_cluster_name}', link={HDILink.format_link(full_cluster_name)}")
        self.log.info(f"Cluster: {cluster_params}")
        try:
            cluster = hdi_hook.create_cluster(
                resource_group=self.resource_group,
                cluster_name=full_cluster_name,
                cluster_params=cluster_params,
                cluster_tags=self.cluster_tags,
                msi_resource_id=self.cluster_config.msi_resource_id,
                wait_for_creation=self.wait_for_creation,
            )

            self.log.info("Sending success execution metrics")
            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
                cluster_id=full_cluster_name,
            )

            ClusterLifecycleMetricPusher().cluster_startup_requested(full_cluster_name, context, self.cluster_task_data)

            self.log.info(f"Finished executing create cluster operator, id={cluster.id}")

            return full_cluster_name
        except HttpResponseError as e:
            self.log.info("Sending fail execution metrics")
            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
                cluster_id=full_cluster_name,
                error_code=e.error.code if e.error is not None else None,
                error_message=e.error.message if e.error is not None else None,
            )
            raise

    @staticmethod
    def add_hashed_prefix(cluster_name: str, environment: TtdEnv) -> str:
        enriched_name = cluster_name + environment.execution_env
        name_hash = hashlib.md5(enriched_name.encode()).hexdigest()
        prefix = name_hash[:6]
        if prefix[0].isdigit():
            prefix = chr(97 + int(prefix[0])) + prefix[1:]

        return prefix + "-" + cluster_name

    @staticmethod
    def add_random_prefix(cluster_name: str) -> str:
        rnd = random.choice(string.ascii_lowercase) + "".join(
            random.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )  # first char should be a letter

        return rnd + "-" + cluster_name

    @staticmethod
    def get_cluster_properties(
        cluster_version: HDIVersion,
        cluster_config: HdiClusterConfig,
        container_name: str,
        vm_config: HDIVMConfig,
        script_action_list: List[ScriptAction],
        rest_login,
        rest_password,
        ssh_login,
        ssh_password,
        enable_openlineage: bool = True
    ) -> ClusterCreateProperties:
        extra_configuration = {
            "gateway": {
                "restAuthCredential.enabled_credential": "True",
                "restAuthCredential.username": rest_login,
                "restAuthCredential.password": rest_password,
            }
        }
        if enable_openlineage:
            extra_configuration["spark2-defaults"] = {
                "spark.extraListeners":
                "com.microsoft.hdinsight.spark.metrics.SparkMetricsListener,"
                "org.apache.spark.sql.scheduler.EnhancementSparkListener,"
                "io.openlineage.spark.agent.OpenLineageSparkListener,"
                "com.thetradedesk.spark.listener.LineageMonitoringListener"
            }

        if vm_config.disks_per_node > 0:
            mount_dirs = ",".join([f"/data_disk_{num}/hadoop/hdfs/datanode" for num in range(0, vm_config.disks_per_node)])
            extra_configuration["hdfs-site"] = {"dfs.datanode.data.dir": mount_dirs}

        data_disks = None
        if vm_config.disks_per_node > 0:
            data_disks = [DataDisksGroups(disks_per_node=vm_config.disks_per_node)]

        return ClusterCreateProperties(
            cluster_version=cluster_version.version,
            os_type=OSType.linux,
            tier=Tier.standard,
            cluster_definition=ClusterDefinition(kind="Spark", configurations=extra_configuration),
            compute_profile=ComputeProfile(
                roles=[
                    Role(
                        name="headnode",
                        target_instance_count=vm_config.num_headnode,
                        hardware_profile=HardwareProfile(vm_size=vm_config.headnode_type.instance_name),
                        os_profile=OsProfile(
                            linux_operating_system_profile=LinuxOperatingSystemProfile(
                                username=ssh_login,
                                password=ssh_password,
                            )
                        ),
                        virtual_network_profile=VirtualNetworkProfile(
                            id=cluster_config.virtual_network_profile_id,
                            subnet=cluster_config.virtual_network_profile_subnet,
                        ),
                        script_actions=script_action_list,
                    ),
                    Role(
                        name="workernode",
                        target_instance_count=vm_config.num_workernode,
                        hardware_profile=HardwareProfile(vm_size=vm_config.workernode_type.instance_name),
                        os_profile=OsProfile(
                            linux_operating_system_profile=LinuxOperatingSystemProfile(
                                username=ssh_login,
                                password=ssh_password,
                            )
                        ),
                        virtual_network_profile=VirtualNetworkProfile(
                            id=cluster_config.virtual_network_profile_id,
                            subnet=cluster_config.virtual_network_profile_subnet,
                        ),
                        script_actions=script_action_list,
                        data_disks_groups=data_disks,
                    ),
                ]
            ),
            storage_profile=StorageProfile(
                storageaccounts=[
                    StorageAccount(
                        name=cluster_config.logs_storage_account,
                        file_system=container_name,
                        is_default=True,
                        resource_id=cluster_config.logs_storage_account_resource_id,
                        msi_resource_id=cluster_config.msi_resource_id,
                    )
                ]
            ),
        )
