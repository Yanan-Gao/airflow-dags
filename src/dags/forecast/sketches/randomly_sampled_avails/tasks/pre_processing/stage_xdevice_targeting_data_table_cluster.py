from dags.forecast.sketches.randomly_sampled_avails.constants import EMR_CLUSTER_SUFFIX, \
    STANDARD_MASTER_FLEET_INSTANCE_TYPE_CONFIGS, EMR_6_VERSION, STANDARD_CLUSTER_TAGS, CLUSTER_ADDITIONAL_PROPERTIES
from dags.forecast.sketches.randomly_sampled_avails.utils import get_core_fleet_instance_type_configs
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask

_NAME = "StageXDeviceTargetingDataTableCluster"
_ON_DEMAND_WEIGHT = 960
_TAGS = {"Process": "StageXDeviceTargetingDataTable"}


class StageXDeviceTargetingDataTableCluster(EmrClusterTask):

    def __init__(self):
        super().__init__(
            name=_NAME + EMR_CLUSTER_SUFFIX,
            core_fleet_instance_type_configs=get_core_fleet_instance_type_configs(_ON_DEMAND_WEIGHT),
            master_fleet_instance_type_configs=STANDARD_MASTER_FLEET_INSTANCE_TYPE_CONFIGS,
            emr_release_label=EMR_6_VERSION,
            enable_prometheus_monitoring=True,
            cluster_tags={
                **STANDARD_CLUSTER_TAGS,
                **_TAGS
            },
            additional_application_configurations=CLUSTER_ADDITIONAL_PROPERTIES,
            retries=0,
        )
