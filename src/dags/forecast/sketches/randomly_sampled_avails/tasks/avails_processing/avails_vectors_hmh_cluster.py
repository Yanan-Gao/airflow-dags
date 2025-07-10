from dags.forecast.sketches.randomly_sampled_avails.constants import STANDARD_MASTER_FLEET_INSTANCE_TYPE_CONFIGS, \
    STANDARD_CLUSTER_TAGS, EMR_CLUSTER_SUFFIX, EMR_6_VERSION, CLUSTER_ADDITIONAL_PROPERTIES
from dags.forecast.sketches.randomly_sampled_avails.utils import get_core_fleet_instance_type_configs
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask

_ON_DEMAND_WEIGHT = 3840
_NAME = "AvailsVectorsHMHCluster"
_ADDITIONAL_CLUSTER_TAGS = {"Process": "AvailsVectorsHMH"}

CORE_FLEET_INSTANCE_TYPE_CONFIGS = [
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
]


class AvailsVectorsHMHCluster(EmrClusterTask):

    def __init__(self):
        super().__init__(
            name=_NAME + EMR_CLUSTER_SUFFIX,
            core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(_ON_DEMAND_WEIGHT, CORE_FLEET_INSTANCE_TYPE_CONFIGS),
            master_fleet_instance_type_configs=STANDARD_MASTER_FLEET_INSTANCE_TYPE_CONFIGS,
            emr_release_label=EMR_6_VERSION,
            enable_prometheus_monitoring=True,
            cluster_tags={
                **STANDARD_CLUSTER_TAGS,
                **_ADDITIONAL_CLUSTER_TAGS
            },
            additional_application_configurations=CLUSTER_ADDITIONAL_PROPERTIES,
            retries=0,
        )
