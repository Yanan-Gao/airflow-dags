from typing import Dict, Any

from ttd.eldorado.aws.cluster_configs.base import EmrConf


class MaximizeResourceAllocationConf(EmrConf):

    def to_dict(self) -> Dict[str, Any]:
        return {"Classification": "spark", "Configurations": [], "Properties": {"maximizeResourceAllocation": "true"}}
