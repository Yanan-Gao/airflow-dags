from typing import Optional

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from math import floor

from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConf, EmrConfiguration


class RollingHistoryServerConf(EmrConf):
    # According to the best practices for spark history server, for large jobs, to avoid OOM exceptions,
    # The rolling event log should be used. Furthermore, a retention policy can be set on logs if users want to make the UI
    # for spark history server faster.
    # finally, the number of replay threads can be configured according to the number of cores on the master fleet,
    # from 25% closer to 90%

    def __init__(
        self, master_fleet_type: Optional[EmrFleetInstanceTypes] = None, max_files_to_retain: Optional[int] = None, core_util: float = .25
    ):
        self.CONFIGURATION: EmrConfiguration = {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.eventLog.rolling.enabled": "true",
            },
        }
        if max_files_to_retain is not None:
            self.CONFIGURATION["Properties"]["spark.history.fs.eventLog.rolling.maxFilesToRetain"] = str(max_files_to_retain)
        if master_fleet_type is not None:
            min_cores_on_master = min(instance.cores for instance in master_fleet_type.instance_types)
            cores_to_use_for_history: int = max(1, floor(core_util * min_cores_on_master))
            self.CONFIGURATION["Properties"]["spark.history.fs.numReplayThreads"] = str(cores_to_use_for_history)

    def to_dict(self) -> EmrConfiguration:
        return self.CONFIGURATION
