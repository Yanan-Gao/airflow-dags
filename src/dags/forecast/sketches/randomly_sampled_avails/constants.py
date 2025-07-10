from typing import List, Tuple

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import FORECAST

# X Com keys used throughout the DAG
# Finding DOW runs to use
RAM_GENERATION_TIMESTAMP_KEY = 'ram_generation_timestamp'
RAM_GENERATION_ISO_WEEKDAY_KEY = 'ram_generation_iso_weekday'
LAST_GOOD_ISO_WEEKDAYS_KEY = 'last_good_iso_weekdays'
# Aerospike locking unlocking sets
INACTIVE_AEROSPIKE_SET_KEY_AEROSPIKE = 'inactive_aerospike_set_aerospike'
INACTIVE_AEROSPIKE_SET_NUMBER_KEY_AEROSPIKE = 'inactive_aerospike_set_number_aerospike'
AEROSPIKE_GEN_KEY = 'aerospike_gen'
# For some reason using hostname here does not work. See https://thetradedesk.slack.com/archives/C458TPU56/p1691676182722599
AEROSPIKE_HOSTS = '{{macros.ttd_extras.resolve_consul_url("ttd-ramv.aerospike.service.useast.consul", limit=10, port=3000)}}'
RAM_NAMESPACE = 'ttd-ramv'
METADATA_SET_NAME = 'metadata'
RAM_SET_NAME = 'rv3'
AEROSPIKE_USE_BUFFERED_WRITES = 'true'
UPDATE_AEROSPIKE_SET_TASK_ID = 'update_active_aerospike_set'
LOOKUP_INACTIVE_AEROSPIKE_TASK_ID = 'lookup_inactive_aerospike_set'
# Below are experimentally chosen parameters for optimal BufferedWrites operation
AEROSPIKE_TRANSACTION_RATE = '28200'
AEROSPIKE_MAX_ASYNC_CONNECTIONS_PER_NODE = '100'

ETL_BASED_FORECASTS_JAR_PATH = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
AZURE_JAR_PATH = 'abfs://ttd-build-artefacts@ttdartefacts.blob.core.windows.net/etl-based-forecasts/master/latest/jars/etl-forecast-jobs.jar'

# constants used in multiple steps
HMH_P = 12
HMH_R = 52

EMR_6_VERSION = AwsEmrVersions.AWS_EMR_SPARK_3_3  # for spark 3

DAG_NAME = 'forecasting-ram-pipeline-v2'
EMR_CLUSTER_SUFFIX = '_cluster_aws'

# Instance fleet configs
STANDARD_MASTER_FLEET_INSTANCE_TYPE_CONFIGS = EmrFleetInstanceTypes(
    instance_types=[R5.r5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
)

STANDARD_CORE_FLEET_INSTANCE_TYPE_CONFIGS = [
    R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
]

STANDARD_CLUSTER_TAGS = {
    "Team": FORECAST.team.jira_team,
}

CLUSTER_ADDITIONAL_PROPERTIES: list[dict[str, dict[str, str] | str]] = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.resourcemanager.am.max-attempts": "1"
    },
}]
GLOBAL_JVM_SETTINGS_LIST_AZURE = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
GLOBAL_JVM_SETTINGS_LIST = [('openlineage.enable', 'false')]
GLOBAL_SPARK_OPTIONS_LIST_AZURE: List[Tuple[str, str]] = []
GLOBAL_SPARK_OPTIONS_LIST = [("executor-memory", "27000M"), ("executor-cores", "4"),
                             ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=27000M"),
                             ("conf", "spark.driver.cores=8"), ("conf", "spark.driver.maxResultSize=6G"),
                             ("conf", "spark.network.timeout=300"), ("conf", "spark.shuffle.registration.timeout=90000")]
