from typing import Dict, Any, Optional

from airflow.models import BaseOperator
from ttd.alicloud.ali_hook import Defaults
from ttd.alicloud.ali_hook import AliEMRHook
from ttd.metrics.cluster import ClusterLifecycleMetricPusher


class AliDeleteClusterOperator(BaseOperator):
    """
    An operator which deletes an AliCloud EMR cluster.
    @param cluster_id: The id of the cluster to be deleted.
    @param region: str
    """

    template_fields = ["cluster_id"]

    def __init__(self, cluster_id: str, region: Optional[str] = Defaults.CN4_REGION_ID, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_id = region
        self.cluster_id = cluster_id

    def execute(self, context: Dict[str, Any]):  # type: ignore
        ali_hook = AliEMRHook()

        self.log.info("Executing delete cluster operator")
        ali_hook.delete_cluster(region_id=self.region_id, cluster_id=self.cluster_id)
        self.log.info("Finished executing delete cluster operator")
        ClusterLifecycleMetricPusher().cluster_termination_requested(self.cluster_id, context)
