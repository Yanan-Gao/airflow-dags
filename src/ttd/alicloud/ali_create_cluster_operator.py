import json
import random
import string
import logging
from typing import Dict, Any, List, Optional, Sequence
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from ttd.alicloud.ali_hook import AliEMRHook
from ttd.alicloud.ali_hook import Defaults
from alibabacloud_emr20160408 import models as emr_models
from alibabacloud_emr20160408.models import CreateClusterV2Request, CreateClusterV2RequestBootstrapAction
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.cloud_storages.ali_cloud_storage import AliCloudStorage
from ttd.alicloud.ali_ecs_hook import AliECSHook
from ttd.alicloud.ali_vpc_hook import AliVPCHook
from ttd.metrics.cluster import ClusterLifecycleMetricPusher, ClusterTaskData


class AliCreateClusterOperator(BaseOperator):
    """
    An operator which creates an AliCloud EMR cluster.

    @param cluster_name: The name of the cluster
    @param cluster_config: Contains  emr_version, log_bucket_dir
    @param vm_config: Contains the type and the number of VMs for worker and head nodes.
    @param region: The region of the cluster
    @param cluster_tags: Tags of the cluster
    """

    OSS_SCRIPTS_ROOT = "oss://ttd-build-artefacts/eldorado-core/release/bootstrap/latest"

    template_fields: Sequence[str] = ("cluster_tags", )

    def __init__(
        self,
        cluster_name: str,
        cluster_config: Dict[str, str],
        master_instance_type: ElDoradoAliCloudInstanceTypes,
        core_instance_type: ElDoradoAliCloudInstanceTypes,
        cluster_task_data: ClusterTaskData,
        region: Optional[str] = Defaults.CN4_REGION_ID,
        cluster_tags: Dict[str, str] = None,
        emr_default_config: Optional[List[emr_models.CreateClusterV2RequestConfig]] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if region != Defaults.CN4_REGION_ID:
            raise AirflowException(f"Not Supported create EMR in given region {region} now")

        self.cluster_name = cluster_name
        self.cluster_config = cluster_config
        self.region = region
        self.cluster_task_data = cluster_task_data
        self.cluster_tags = cluster_tags
        self.master_instance_type = master_instance_type
        self.core_instance_type = core_instance_type

        if self.cluster_tags is None:
            self.cluster_tags = {}

        self.emr_default_config = emr_default_config

    def execute(self, context: Dict[str, Any]) -> str:  # type: ignore
        ali_hook = AliEMRHook()

        full_cluster_name = self.add_random_prefix(self.cluster_name)
        region_info = self.get_region_info(
            self.region,
            self.master_instance_type.instance_type.instance_name,
            self.core_instance_type.instance_type.instance_name,
        )
        self.log.info(
            f"create cluster request with region: {region_info['region_id']},zone: {region_info['zone_id']}, vpc:{region_info['vpc_id']}, vswitch:{region_info['v_switch_id']}"
        )
        create_cluster_request = self.get_create_cluster_request(
            # pass instance type&node count to get region info
            region_info=region_info,
            cluster_name=full_cluster_name,
            cluster_config=self.cluster_config,
            master_instance_type=self.master_instance_type,
            core_instance_type=self.core_instance_type,
            cluster_tags=self.cluster_tags,  # type: ignore
            emr_default_config=self.emr_default_config,
        )

        request_json_str = json.dumps(create_cluster_request.to_map())
        self.log.info("Executing create cluster operator with cluster name: " + full_cluster_name + f", request: {request_json_str}")
        cluster_id = ali_hook.create_cluster(create_cluster_request=create_cluster_request)
        self.log.info("Finished executing create cluster operator")
        ClusterLifecycleMetricPusher().cluster_startup_requested(cluster_id, context, self.cluster_task_data)

        return cluster_id

    @staticmethod
    def make_spark_history_dir(bucket_name: str, cluster_name: str):
        oss_hook = AliCloudStorage()
        oss_hook.create_folder(bucket_name, f"airflow-logs/{cluster_name}/spark-history")

    @staticmethod
    def get_region_info(region_id: str, master_instance_type: str, core_instance_type: str) -> Dict[str, str]:
        # currently we only support to create emr in region 'cn-shanghai'
        # https://atlassian.thetradedesk.com/jira/browse/DATAPROC-3185
        # get available zone id with requested resource
        available_zone_ids = AliCreateClusterOperator.get_zone_ids(region_id, master_instance_type, core_instance_type)
        # get vpc & vswitchid
        vpc_id = Defaults.CN4_REGION_DEFAULT_VPC_ID
        random.shuffle(available_zone_ids)
        for zone_id in available_zone_ids:
            vswitch_id = AliCreateClusterOperator.get_vswitch_id(region_id, zone_id, vpc_id)
            if vswitch_id is not None:
                logging.info(f"choose vswitch_id: {vswitch_id} for zone_id: {zone_id}")
                return dict(
                    region_id=region_id,
                    zone_id=zone_id,
                    vpc_id=vpc_id,
                    v_switch_id=vswitch_id,
                )

        raise AirflowException(
            f"No available resource in given region {region_id} with instance_type {master_instance_type} and {core_instance_type}"
        )

    @staticmethod
    def get_zone_ids(region_id: str, master_instance_type: str, core_instance_type: str) -> List[str]:
        master_available_zones = AliCreateClusterOperator.get_available_zones(region_id, master_instance_type)
        core_available_zones = AliCreateClusterOperator.get_available_zones(region_id, core_instance_type)
        available_zones = [value for value in master_available_zones if value in core_available_zones]
        logging.info(f"available_zones: {available_zones}")
        if not available_zones:
            logging.error(
                f"No available zone! in given region {region_id} now with instance_type {master_instance_type} and {core_instance_type}"
            )
            raise AirflowException(
                f"No available zone! in given region {region_id} now with instance_type {master_instance_type} and {core_instance_type}"
            )
        return available_zones

    @staticmethod
    def get_available_zones(
        region_id: str,
        instance_type: str,
        destination_resource: str = Defaults.DESTINATION_RESOURCE,
        instance_charge_type: str = Defaults.CHARGE_TYPE_POST_PAID,
    ):
        ali_ecs_hook = AliECSHook()
        zones = ali_ecs_hook.describe_available_resource(region_id, instance_type, destination_resource, instance_charge_type)
        return list(
            map(
                lambda zone: zone.zone_id,
                list(filter(
                    lambda zone: AliCreateClusterOperator.check_zone_resource(zone),
                    zones,
                )),
            )
        )

    @staticmethod
    def check_zone_resource(zone) -> bool:
        if (zone.status != Defaults.STATUS_AVAILABLE or zone.status_category != Defaults.WITH_STOCK):
            return False
        # List[DescribeAvailableResourceResponseBodyAvailableZonesAvailableZoneAvailableResourcesAvailableResource]
        for r1 in zone.available_resources.available_resource:
            # List[DescribeAvailableResourceResponseBodyAvailableZonesAvailableZoneAvailableResourcesAvailableResourceSupportedResourcesSupportedResource]
            for r2 in r1.supported_resources.supported_resource:
                if (r2.status == Defaults.STATUS_AVAILABLE and r2.status_category == Defaults.WITH_STOCK):
                    return True
        return False

    @staticmethod
    def get_vswitch_id(region_id: str, zone_id: str, vpc_id: str) -> Optional[str]:
        vswitches = AliCreateClusterOperator.get_available_vswitches(region_id, zone_id, vpc_id)
        logging.info(f"vswitches: {vswitches}")
        if not vswitches:
            logging.warning(f"No available vswitchid in region = {region_id}, zone_id = {zone_id} and vpc_id = {vpc_id}")
            return None
        return random.choice(vswitches)

    @staticmethod
    def get_available_vswitches(region_id: str, zone_id: str, vpc_id: str):
        ali_vpc_hook = AliVPCHook()
        vswitches = ali_vpc_hook.describe_vswitches(region_id, zone_id, vpc_id)
        return list(
            map(
                lambda s: s.v_switch_id,
                list(
                    filter(
                        lambda s: s.status == Defaults.STATUS_AVAILABLE and s.v_switch_name.startswith(Defaults.VSWITCH_PREFIX),
                        vswitches,
                    )
                ),
            )
        )

    @staticmethod
    def add_random_prefix(cluster_name: str) -> str:
        rnd = random.choice(string.ascii_lowercase) + "".join(
            random.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )  # first char should be a letter

        return rnd + "-" + cluster_name

    def _bootstrap_actions(self):
        script_path = f"{self.OSS_SCRIPTS_ROOT}/download-from-oss.sh"
        jks_oss_path = f"{self.OSS_SCRIPTS_ROOT}/ttd-internal-root-ca-truststore.jks"
        jks_local_path = "/tmp/ttd-internal-root-ca-truststore.jks"

        oss_keys_connection = json.loads(BaseHook.get_connection("alicloud_oss_access_key").get_extra())
        if "access_key_id" not in oss_keys_connection or "access_key_secret" not in oss_keys_connection:
            logging.warning(
                "Secret alicloud_oss_access_key with access_key_id and access_key_secret not specified. "
                "Bootstrap script to download CERTs will not work"
            )

        access_key_id = oss_keys_connection.get("access_key_id", "None")
        secret_access_key = oss_keys_connection.get("access_key_secret", "None")
        download_certs_args = " ".join([access_key_id, secret_access_key, jks_oss_path, jks_local_path])

        bootstrap_actions = [
            CreateClusterV2RequestBootstrapAction(
                name="download-certs", path=script_path, arg=download_certs_args, execution_fail_strategy="FAILED_CONTINUE"
            )
        ]

        return bootstrap_actions

    def get_create_cluster_request(
        self,
        region_info: Dict[str, str],
        cluster_name: str,
        cluster_config: Dict[str, Any],
        master_instance_type: ElDoradoAliCloudInstanceTypes,
        core_instance_type: ElDoradoAliCloudInstanceTypes,
        cluster_tags: Dict[str, str],
        emr_default_config: Optional[List[emr_models.CreateClusterV2RequestConfig]] = None,
    ) -> CreateClusterV2Request:
        emr_version = cluster_config["emr_version"]
        log_bucket_dir = cluster_config["log_bucket_dir"]

        # make dir for spark-history
        AliCreateClusterOperator.make_spark_history_dir(log_bucket_dir, cluster_name)

        host_group = [
            emr_models.CreateClusterV2RequestHostGroup(
                host_group_type="MASTER",
                disk_type=Defaults.ENHANCED_SSD,
                sys_disk_type=Defaults.ENHANCED_SSD,
                instance_type=master_instance_type.instance_type.instance_name,
                disk_count=master_instance_type.data_disk_count,
                disk_capacity=master_instance_type.data_disk_size_gb,
                sys_disk_capacity=master_instance_type.sys_disk_size_gb,
                node_count=master_instance_type.node_count,
                host_group_name=Defaults.MASTER_HOST_NAME,
            ),
            emr_models.CreateClusterV2RequestHostGroup(
                host_group_type="CORE",
                disk_type=Defaults.ENHANCED_SSD,
                sys_disk_type=Defaults.ENHANCED_SSD,
                instance_type=core_instance_type.instance_type.instance_name,
                disk_count=core_instance_type.data_disk_count,
                disk_capacity=core_instance_type.data_disk_size_gb,
                sys_disk_capacity=core_instance_type.sys_disk_size_gb,
                node_count=core_instance_type.node_count,
                host_group_name="emr-worker",
            ),
        ]

        tags = []
        for key, value in cluster_tags.items():
            tags.append(emr_models.CreateClusterV2RequestTag(key=key, value=value))

        # need use prod oss bucket
        log_dir = f"oss://{log_bucket_dir}.oss-cn-shanghai-internal.aliyuncs.com/airflow-logs/{cluster_name}"
        config = [
            emr_models.CreateClusterV2RequestConfig(
                service_name="YARN",
                file_name="capacity-scheduler",
                config_key="xml-direct-to-file-content",
                config_value="<configuration>\
  <property>\
    <name>yarn.scheduler.capacity.maximum-applications</name>\
    <value>10000</value>\
    <description>Maximum number of applications that can be pending and running.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>\
    <value>0.8</value>\
    <description>Maximum percent of resources in the cluster which can be used to run application masters i.e. controls number of concurrent running applications.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.resource-calculator</name>\
    <value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value>\
    <description>The ResourceCalculator implementation to be used to compare Resources in the scheduler.The default i.e. DefaultResourceCalculator only uses Memory while DominantResourceCalculator uses dominant-resource to compare multi-dimensional resources such as Memory, CPU etc.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.queues</name>\
    <value>default</value>\
    <description>The queues at the this level (root is the root queue).</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.default.capacity</name>\
    <value>100</value>\
    <description>Default queue target capacity.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.default.user-limit-factor</name>\
    <value>1</value>\
    <description>Default queue user limit a percentage from 0.0 to 1.0.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>\
    <value>100</value>\
    <description>The maximum capacity of the default queue.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.default.state</name>\
    <value>RUNNING</value>\
    <description>The state of the default queue. State can be one of RUNNING or STOPPED.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.default.acl_submit_applications</name>\
    <value>*</value>\
    <description>The ACL of who can submit jobs to the default queue.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.root.default.acl_administer_queue</name>\
    <value>*</value>\
    <description>The ACL of who can administer jobs on the default queue.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.node-locality-delay</name>\
    <value>-1</value>\
    <description>Number of missed scheduling opportunities after which the CapacityScheduler attempts to schedule rack-local containers. Typically this should be set to number of nodes in the cluster.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.queue-mappings</name>\
    <value></value>\
    <description>A list of mappings that will be used to assign jobs to queues. The syntax for this list is [u|g]:[name]:[queue_name][,next mapping]* Typically this list will be used to map users to queues,for example, u:%user:%user maps all users to queues with the same name as the user.</description>\
  </property>\
  <property>\
    <name>yarn.scheduler.capacity.queue-mappings-override.enable</name>\
    <value>false</value>\
    <description>If a queue mapping is present, will it override the value specified by the user? This can be used by administrators to place jobs in queues that are different than the one specified by the user. The default is false.</description>\
  </property>\
</configuration>",
            ),
            emr_models.CreateClusterV2RequestConfig(
                service_name="YARN",
                file_name="yarn-site",
                config_key="yarn.nodemanager.remote-app-log-dir",
                config_value=log_dir + "/yarn-apps-logs",
            ),
            emr_models.CreateClusterV2RequestConfig(
                service_name="YARN",
                file_name="mapred-site",
                config_key="mapreduce.jobhistory.done-dir",
                config_value=log_dir + "/job-history/done",
            ),
            emr_models.CreateClusterV2RequestConfig(
                service_name="YARN",
                file_name="mapred-site",
                config_key="mapreduce.jobhistory.intermediate-done-dir",
                config_value=log_dir + "/job-history/done_intermediate",
            ),
            emr_models.CreateClusterV2RequestConfig(
                service_name="SPARK",
                file_name="spark-defaults",
                config_key="spark_eventlog_dir",
                config_value=log_dir + "/spark-history",
            ),
            emr_models.CreateClusterV2RequestConfig(
                service_name="SPARK",
                file_name="spark-defaults",
                config_key="spark.history.fs.logDirectory",
                config_value=log_dir + "/spark-history",
            ),
        ]

        if emr_default_config is not None:
            config.extend(emr_default_config)

        create_cluster_request = CreateClusterV2Request(
            region_id=region_info["region_id"],
            zone_id=region_info["zone_id"],
            vpc_id=region_info["vpc_id"],
            v_switch_id=region_info["v_switch_id"],
            security_group_id="sg-uf62wlh0c24rmz9i3h0c",
            charge_type=Defaults.CHARGE_TYPE_POST_PAID,
            cluster_type=Defaults.CLUSTER_TYPE_HADOOP,
            net_type=Defaults.NET_TYPE_VPC,
            use_local_meta_db=False,
            option_soft_ware_list=["LIVY", "ZOOKEEPER"],
            user_defined_emr_ecs_role="AliCloud-Airflow-Spark-Ram-Role",
            bootstrap_action=self._bootstrap_actions(),
            # xml-direct-to-file-content override is for 'yarn.scheduler.capacity.maximum-am-resource-percent',
            # set it to 0.8 to solve error "Hadoop: maximum-am-resource-percent is insufficient to start a single
            # application"
            config=config,
            host_group=host_group,
            tag=tags,
            emr_ver=emr_version,
            name=cluster_name,
        )
        return create_cluster_request
