from dags.forecast.sketches.randomly_sampled_avails.constants import EMR_CLUSTER_SUFFIX, \
    STANDARD_MASTER_FLEET_INSTANCE_TYPE_CONFIGS, EMR_6_VERSION, STANDARD_CLUSTER_TAGS, CLUSTER_ADDITIONAL_PROPERTIES
from dags.forecast.sketches.randomly_sampled_avails.utils import get_core_fleet_instance_type_configs
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask

_NAME = "WriteToAerospikeCluster"
_ON_DEMAND_WEIGHT = 320
_TAGS = {"Process": "WriteToAerospike"}
_EMR_MANAGED_MASTER_SECURITY_GROUP = "sg-008678553e48f48a3"  # ElasticMapReduce-Master-Private
_EMR_MANAGED_SLAVE_SECURITY_GROUP = "sg-02fa4e26912fd6530"  # ElasticMapReduce-Slave-Private
_SERVICE_ACCESS_SECURITY_GROUP = "sg-0b0581bc37bcac50a"  # ElasticMapReduce-ServiceAccess
_EC2_SUBNET_IDS = ["subnet-0e82171b285634d41"]  # Aerospike cluster subnet


class WriteToAerospikeCluster(EmrClusterTask):

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
            emr_managed_master_security_group=_EMR_MANAGED_MASTER_SECURITY_GROUP,
            emr_managed_slave_security_group=_EMR_MANAGED_MASTER_SECURITY_GROUP,
            service_access_security_group=_SERVICE_ACCESS_SECURITY_GROUP,
            ec2_subnet_ids=_EC2_SUBNET_IDS,
            additional_application_configurations=CLUSTER_ADDITIONAL_PROPERTIES,
            retries=0,
        )
