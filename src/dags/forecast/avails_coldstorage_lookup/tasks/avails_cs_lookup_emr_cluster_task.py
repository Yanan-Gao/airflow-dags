from dags.forecast.avails_coldstorage_lookup.constants import SHORT_DAG_NAME
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.el_dorado.v2.emr import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.slack.slack_groups import FORECAST

# TODO: This should be part of a common library/util
_BOOTSTRAP_SCRIPT_LOCATION_AWS = 's3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/coldstoragereader'
_BOOTSTRAP_SCRIPT_AWS = f'{_BOOTSTRAP_SCRIPT_LOCATION_AWS}/install-start-iftop.sh'

_EMR_RELEASE_LABEL = AwsEmrVersions.AWS_EMR_SPARK_3_3
_EMR_CLUSTER_SUFFIX = '_cluster_aws'
_EMR_MANAGED_MASTER_SECURITY_GROUP = 'sg-008678553e48f48a3'
_EMR_MANAGED_SLAVE_SECURITY_GROUP = 'sg-02fa4e26912fd6530'
_SERVICE_ACCESS_SECURITY_GROUP = 'sg-0b0581bc37bcac50a'
_CLUSTER_TAGS = {'Team': FORECAST.team.jira_team}
_EC2_SUBNET_IDS = [EmrSubnets.Private.useast_emr_1a()]
_MASTER_FLEET_INSTANCE_TYPE_CONFIGS = EmrFleetInstanceTypes(
    instance_types=[R5.r5_2xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)
_CORE_FLEET_INSTANCE_TYPE_CONFIGS = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(2),
        R5.r5_4xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1),
        R5a.r5a_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(2),
        R5.r5_16xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(4)
    ],
    on_demand_weighted_capacity=32
)


class AvailsCsLookupEmrClusterTask(EmrClusterTask):
    """
    Class to encapsulate the EMRClusterTask creation for Avails ColdStorage Sync.

    More info: https://atlassian.thetradedesk.com/confluence/display/EN/Universal+Forecasting+Walmart+Data+Sovereignty
    """

    def __init__(self):
        super().__init__(
            name=SHORT_DAG_NAME + _EMR_CLUSTER_SUFFIX,
            core_fleet_instance_type_configs=_CORE_FLEET_INSTANCE_TYPE_CONFIGS,
            master_fleet_instance_type_configs=_MASTER_FLEET_INSTANCE_TYPE_CONFIGS,
            emr_release_label=_EMR_RELEASE_LABEL,
            ec2_subnet_ids=_EC2_SUBNET_IDS,
            emr_managed_master_security_group=_EMR_MANAGED_MASTER_SECURITY_GROUP,
            emr_managed_slave_security_group=_EMR_MANAGED_SLAVE_SECURITY_GROUP,
            service_access_security_group=_SERVICE_ACCESS_SECURITY_GROUP,
            bootstrap_script_actions=self._build_bootstrap_script_actions(),
            enable_prometheus_monitoring=True,
            cluster_tags=_CLUSTER_TAGS
        )

    def _build_bootstrap_script_actions(self):
        args = [_BOOTSTRAP_SCRIPT_LOCATION_AWS, 'sample_avails_coldstorage_aerospike_network_traffic', 'sampledavails']
        return [ScriptBootstrapAction(_BOOTSTRAP_SCRIPT_AWS, args)]
