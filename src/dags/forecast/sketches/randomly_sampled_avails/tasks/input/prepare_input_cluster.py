from dags.forecast.sketches.randomly_sampled_avails.constants import EMR_CLUSTER_SUFFIX, EMR_6_VERSION, \
    STANDARD_CLUSTER_TAGS, CLUSTER_ADDITIONAL_PROPERTIES
from dags.forecast.sketches.randomly_sampled_avails.utils import get_core_fleet_instance_type_configs
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask

_ON_DEMAND_WEIGHT = 5760
_NAME = "PrepareInputsCluster"
_ADDITIONAL_CLUSTER_TAGS = {"Process": "PrepareInputs"}

INPUTS_CLUSTER_MASTER_FLEET_INSTANCE_TYPE_CONFIGS = get_core_fleet_instance_type_configs(
    on_demand_capacity=1,
    instance_types=[R5.r5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(64)],
)


class PrepareInputsCluster(EmrClusterTask):

    def __init__(self):
        super().__init__(
            name=_NAME + EMR_CLUSTER_SUFFIX,
            core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(_ON_DEMAND_WEIGHT),
            master_fleet_instance_type_configs=INPUTS_CLUSTER_MASTER_FLEET_INSTANCE_TYPE_CONFIGS,
            emr_release_label=EMR_6_VERSION,
            enable_prometheus_monitoring=True,
            cluster_tags={
                **STANDARD_CLUSTER_TAGS,
                **_ADDITIONAL_CLUSTER_TAGS
            },
            additional_application_configurations=CLUSTER_ADDITIONAL_PROPERTIES,
            retries=0,
        )
