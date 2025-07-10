from dataclasses import dataclass
from typing import Optional, Any
from psycopg2.extras import Json

from airflow.utils.context import Context

from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.cloud_provider import CloudProvider, AwsCloudProvider, AzureCloudProvider, AliCloudProvider
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.metrics.metric_db_client import MetricDBClient
from ttd.ttdenv import TtdEnvFactory

CLUSTER_STARTUP_REQUESTED_SPROC = 'metrics.cluster_startup_requested_v2'
CLUSTER_STARTUP_CONCLUDED_SPROC = 'metrics.cluster_startup_concluded_v2'
CLUSTER_TERMINATED_SPROC = 'metrics.cluster_termination_requested'


@dataclass
class ClusterTaskData:
    cluster_task_name: str
    env: str
    cloud_provider: CloudProvider
    cores_request: int
    memory_request: float
    instance_fleets: list[dict[str, Any]]
    cluster_version: Optional[str] = None


class ClusterLifecycleMetricPusher(MetricDBClient):

    def cluster_startup_requested(self, cluster_id: str, context: Context, cluster_task_data: ClusterTaskData) -> None:
        parameters = [
            cluster_id,
            str(cluster_task_data.cloud_provider),
            cluster_task_data.cluster_task_name,
            context["dag"].dag_id,
            context["run_id"],
            cluster_task_data.env,
            cluster_task_data.cores_request,
            cluster_task_data.memory_request,
            Json(cluster_task_data.instance_fleets),
            cluster_task_data.cluster_version,
        ]
        self.execute_sproc(CLUSTER_STARTUP_REQUESTED_SPROC, parameters)

    def cluster_startup_concluded(
        self,
        cluster_id: str,
        context: Context,
        success: bool,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        total_cores: Optional[int] = None,
        total_memory: Optional[int] = None,
        total_disk: Optional[int] = None,
        instance_counts: Optional[dict[str, int]] = None,
    ) -> None:
        parameters = [
            cluster_id,
            context["run_id"],
            success,
            error_code,
            error_message,
            total_cores,
            total_memory,
            total_disk,
            Json(instance_counts) if instance_counts is not None else None,
        ]
        self.execute_sproc(CLUSTER_STARTUP_CONCLUDED_SPROC, parameters)

    def cluster_termination_requested(
        self,
        cluster_id: str,
        context: Context,
    ) -> None:
        parameters = [
            cluster_id,
            context["run_id"],
        ]
        self.execute_sproc(CLUSTER_TERMINATED_SPROC, parameters)


class ClusterTaskDataBuilder:

    def __init__(self, cluster_task_name: str, cluster_version: Optional[str] = None):
        self.cluster_task_name = cluster_task_name
        self.env = TtdEnvFactory.get_from_system().execution_env
        self.cloud_provider: Optional[CloudProvider] = None
        self.cores_request: Optional[int] = None
        self.memory_request: Optional[float] = None
        self.instance_fleets: Optional[list[dict[str, Any]]] = None
        self.cluster_version: Optional[str] = cluster_version

    def for_aws(
        self,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes,
        core_fleet_instance_type_configs: EmrFleetInstanceTypes,
    ) -> "ClusterTaskDataBuilder":
        self.cloud_provider = AwsCloudProvider()
        self.cores_request = (
            core_fleet_instance_type_configs.get_scaled_total_cores() + master_fleet_instance_type_configs.get_scaled_total_cores()
        )
        self.memory_request = (
            core_fleet_instance_type_configs.get_scaled_total_memory() + master_fleet_instance_type_configs.get_scaled_total_memory()
        )

        self.instance_fleets = [
            self._instance_fleet_config("master2", master_fleet_instance_type_configs),
            self._instance_fleet_config("core2", core_fleet_instance_type_configs)
        ]

        return self

    def _instance_fleet_config(self, fleet_type: str, fleet_config: EmrFleetInstanceTypes):
        return {
            "fleet_type":
            fleet_type,
            "capacity":
            fleet_config.on_demand_weighted_capacity + fleet_config.spot_weighted_capacity,
            "instances": [{
                "instance_type": instance.instance_name,
                "weight": instance.weighted_capacity,
                "priority": instance.priority
            } for instance in fleet_config.instance_types]
        }

    def for_azure(self, vm_config: HDIVMConfig) -> "ClusterTaskDataBuilder":
        self.cloud_provider = AzureCloudProvider()
        self.cores_request = (
            vm_config.num_headnode * vm_config.headnode_type.cores + vm_config.num_workernode * vm_config.workernode_type.cores
        )
        self.memory_request = (
            vm_config.num_headnode * vm_config.headnode_type.memory + vm_config.num_workernode * vm_config.workernode_type.memory
        )

        self.instance_fleets = [
            self._basic_instance_configuration("head", vm_config.num_headnode, vm_config.headnode_type.instance_name),
            self._basic_instance_configuration("worker", vm_config.num_workernode, vm_config.workernode_type.instance_name)
        ]

        return self

    def for_alicloud(
        self,
        master_instance_type: ElDoradoAliCloudInstanceTypes,
        core_instance_type: ElDoradoAliCloudInstanceTypes,
    ) -> "ClusterTaskDataBuilder":
        self.cloud_provider = AliCloudProvider()

        self.cores_request = (
            core_instance_type.instance_type.cores * core_instance_type.node_count +
            master_instance_type.instance_type.cores * master_instance_type.node_count
        )
        self.memory_request = (
            core_instance_type.instance_type.memory * core_instance_type.node_count +
            master_instance_type.instance_type.memory * master_instance_type.node_count
        )

        self.instance_fleets = [
            self._basic_instance_configuration("master", master_instance_type.node_count, master_instance_type.instance_type.instance_name),
            self._basic_instance_configuration("core", core_instance_type.node_count, core_instance_type.instance_type.instance_name)
        ]

        return self

    def _basic_instance_configuration(self, fleet_type: str, capacity: int, instance_type: str):
        return {"fleet_type": fleet_type, "capacity": capacity, "instances": [{"instance_type": instance_type, "weight": 1}]}

    def build(self) -> ClusterTaskData:
        if self.cores_request is None or self.memory_request is None or self.cloud_provider is None or self.instance_fleets is None:
            raise ValueError("Incomplete configuration. Please configure resources for a cloud provider.")

        return ClusterTaskData(
            cluster_task_name=self.cluster_task_name,
            env=self.env,
            cloud_provider=self.cloud_provider,
            cores_request=self.cores_request,
            memory_request=self.memory_request,
            instance_fleets=self.instance_fleets,
            cluster_version=self.cluster_version,
        )
