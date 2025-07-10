from dataclasses import dataclass
from enum import Enum

from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd


class InstanceTypeCore(Enum):
    MEMORY_OPTIMIZED_FLEET_WITH_NVM = [
        R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
        R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
    ]
    MEMORY_OPTIMIZED_FLEET_SETTINGS = [
        R5.r5_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5.r5_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
        R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
        R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
    ]


class InstanceTypeMaster(Enum):
    INSTANCE_TYPE_MASTER_FLEET_R5_8x = [R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)]
    INSTANCE_TYPE_MASTER_FLEET_R5_16X = [R5.r5_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)]


@dataclass(frozen=True)
class ClusterSettings:
    name: str = "unknown"
    partition_count: int = 12000
    weighted_capacity: int = 60 * 96
    instance_type_master: InstanceTypeMaster = InstanceTypeMaster.INSTANCE_TYPE_MASTER_FLEET_R5_8x
    instance_type_core: InstanceTypeCore = InstanceTypeCore.MEMORY_OPTIMIZED_FLEET_WITH_NVM
    spark_executor_memory: str = "205G"
