from dags.forecast.sketches.randomly_sampled_avails.constants import EMR_CLUSTER_SUFFIX, \
    STANDARD_CLUSTER_TAGS, EMR_6_VERSION, CLUSTER_ADDITIONAL_PROPERTIES
from dags.forecast.sketches.randomly_sampled_avails.utils import get_core_fleet_instance_type_configs
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask

_ON_DEMAND_WEIGHT = 10
_NAME = 'TargetingDataAggregateMergeCluster'
_ADDITIONAL_CLUSTER_TAGS = {"Process": "DataElementsHMH"}
_MASTER_FLEET_INSTANCE_TYPE_CONFIGS = get_core_fleet_instance_type_configs(
    instance_types=[M6g.m6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(64)],
    on_demand_capacity=1,
)

_EXTRA_DISK_CORE_FLEET_INSTANCE_TYPE_CONFIGS = [
    R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2).with_ebs_size_gb(1024),
    R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4).with_ebs_size_gb(2048)
]


class TargetingDataAggregateMergeCluster(EmrClusterTask):

    def __init__(self):
        super().__init__(
            name=_NAME + EMR_CLUSTER_SUFFIX,
            core_fleet_instance_type_configs=
            get_core_fleet_instance_type_configs(_ON_DEMAND_WEIGHT, _EXTRA_DISK_CORE_FLEET_INSTANCE_TYPE_CONFIGS),
            master_fleet_instance_type_configs=_MASTER_FLEET_INSTANCE_TYPE_CONFIGS,
            emr_release_label=EMR_6_VERSION,
            enable_prometheus_monitoring=True,
            cluster_tags={
                **STANDARD_CLUSTER_TAGS,
                **_ADDITIONAL_CLUSTER_TAGS
            },
            additional_application_configurations=CLUSTER_ADDITIONAL_PROPERTIES,
            retries=0,
        )
