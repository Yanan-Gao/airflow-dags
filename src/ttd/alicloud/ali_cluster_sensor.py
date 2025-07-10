from typing import Dict, Any, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from ttd.alicloud.ali_hook import AliEMRHook
from ttd.alicloud.ali_hook import Defaults
from ttd.metrics.cluster import ClusterLifecycleMetricPusher


class AliClusterSensor(BaseSensorOperator):
    """
    Sensor operator for monitoring a AliCloud cluster.

    @param cluster_id: The id of the cluster to be monitored
    @type cluster_id: str
    @param resource_group: The resource group of the cluster to be monitored
    @type resource_group: str
    @param poke_interval: Time in seconds that the sensor should wait in between each tries
    @type poke_interval: int
    """

    template_fields = ["cluster_id"]

    def __init__(self, cluster_id: str, region: Optional[str] = Defaults.CN4_REGION_ID, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region = region
        self.cluster_id = cluster_id
        self.total_cores: Optional[int] = None
        self.total_memory: Optional[int] = None
        self.total_disk: Optional[int] = None

    def _set_allocated_resources(self, ali_hook: AliEMRHook) -> None:
        total_cores = 0
        total_memory = 0
        total_disk = 0

        try:
            response = ali_hook.describe_cluster(region_id=self.region, cluster_id=self.cluster_id)
            for host_group in response.cluster_info.host_group_list.host_group:
                node_count = host_group.node_count

                total_cores += host_group.cpu_core * node_count
                total_memory += host_group.memory_capacity * node_count
                total_disk += host_group.disk_capacity * host_group.disk_count * node_count

            self.total_cores = total_cores
            self.total_memory = total_memory
            self.total_disk = total_disk
        except Exception as e:
            self.log.error("There was an issue with the retrieval of the allocated resources")
            self.log.error(e, exc_info=True)

    def poke(self, context: Dict[str, Any]) -> bool:  # type: ignore
        ali_hook = AliEMRHook()
        self.log.info("get_cluster_state clusterId:" + str(self.cluster_id))
        state = ali_hook.get_cluster_state(region_id=self.region, cluster_id=self.cluster_id)
        self.log.info("Current state:" + str(state))
        # all status of cluster: https://www.alibabacloud.com/help/en/e-mapreduce/latest/queries-the-basic-information-of-a-cluster
        if state == "IDLE" or state == "RUNNING":
            self._set_allocated_resources(ali_hook)
            return True

        if state == "RELEASING" or state == "RELEASED":
            raise AirflowException("The cluster is currently being removed.")

        if state == "ABNORMAL":
            # release cluster if status is not normal
            self._release_cluster(ali_hook)
            ClusterLifecycleMetricPusher().cluster_startup_concluded(
                cluster_id=self.cluster_id, context=context, success=False, error_code='ABNORMAL'
            )
            raise AirflowException("Cluster abnormal.")

        if state == "CREATE_FAILED":
            self._release_cluster(ali_hook)
            ClusterLifecycleMetricPusher().cluster_startup_concluded(
                cluster_id=self.cluster_id, context=context, success=False, error_code='CREATE_FAILED'
            )
            raise AirflowException("Cluster VM allocation failed.")

        return False

    def _release_cluster(self, ali_hook):
        ali_hook.delete_cluster(region_id=self.region, cluster_id=self.cluster_id)

    def post_execute(self, context, result=None):
        ClusterLifecycleMetricPusher().cluster_startup_concluded(
            cluster_id=self.cluster_id,
            context=context,
            success=True,
            total_cores=self.total_cores,
            total_memory=self.total_memory,
            total_disk=self.total_disk
        )
