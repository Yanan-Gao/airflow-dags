from typing import Optional

from ttd.livy_base_hook import LivyBaseHook
from ttd.alicloud.ali_hook import AliEMRHook
from ttd.alicloud.ali_hook import Defaults


class AliLivyHook(LivyBaseHook):

    def __init__(self, cluster_id: str, region_id: Optional[str] = Defaults.CN4_REGION_ID):
        self.cluster_id = cluster_id
        self.region_id = region_id
        super().__init__()

    def get_endpoint(self) -> str:
        ali_hook = AliEMRHook()
        ali_livy_endpoint = ali_hook.get_livy_endpoint(region_id=self.region_id, cluster_id=self.cluster_id)
        self.log.info(f"Livy endpoint for cluster {self.cluster_id}: {ali_livy_endpoint}")
        return ali_livy_endpoint
