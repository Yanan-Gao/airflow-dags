from datetime import datetime
from dags.omniux.utils import get_jar_file_path
from dags.cmo.utils.fleet_batch_config import getMasterFleetInstances, EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances
from dags.invmkt.expand_inventory.common import build_create_version_task, \
    build_set_version_to_complete_task, CREATE_VERSION_TASK_ID, VERSION_ID_KEY
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration
from ttd.slack.slack_groups import OMNIUX
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask

jar_path = get_jar_file_path()
start_date = datetime(2025, 6, 17, 0, 0, 0)
schedule_interval = "0 12 * * *"  # Run daily at 12:00 UTC

job_name = "omniux-deal-channel-avail-match-calculation"
dataloader_name = "s3_to_mongodb_dataloader"
cluster_tags = {"Team": OMNIUX.team.jira_team}

class_name = "com.thetradedesk.ctv.upstreaminsights.pipelines.dealchannelmatch.DealChannelMatchJob"
dataloader_class_name = "com.thetradedesk.ctv.upstreaminsights.pipelines.dealchannelmatch.DealChannelMatchS3ToMongoLoaderJob"

runDateTime = "{{ data_interval_start.strftime(\"%Y-%m-%dT00:00:00\") }}"

env = TtdEnvFactory.get_from_system()

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=schedule_interval,
    retries=0,
    slack_channel=OMNIUX.team.alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[OMNIUX.team.name, "DealChannelAvailMatch"],
    run_only_latest=True,
)

dag = ttd_dag.airflow_dag

datasets = [
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics",
        data_name="DealAggregatedAvails",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=2,
        success_file=None,
    )
]

check_required_datasets = OpTask(
    op=DatasetCheckSensor(
        task_id='deal-aggregated-avails-available',
        datasets=datasets,
        ds_date="{{ data_interval_start.strftime('%Y-%m-%d 00:00:00')}}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

master_fleet_instance_type_configs = getMasterFleetInstances(EmrInstanceClasses.NetworkOptimized, EmrInstanceSizes.TwoX)
core_fleet_instance_type_configs = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.SixteenX, instance_capacity=15)

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    retries=0,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

java_options = [
    ("runDateTime", runDateTime),
    ("rootDealChannelMatchPath", "s3://ttd-inventory-recommendations-metadata"),
    ("percentageBuckets", "0,1;1,5;5,10;10,25;25,50;50,90;90,101"),
    ("writeTargetedAdGroupPrivateContract", "false"),  # set to "true" to debug
    ("excludeHiddenBidLists", "false"),
    ("includeIsAllAllowedToBuyPrivateContract", "false"),
    ("excludeNonKokaiAdGroups", "true"),
    ("excludeAlwaysOnAdGroups", "true")
]

job_task = EmrJobTask(
    name=job_name + "-job-task",
    retries=0,
    class_name=class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=java_options
)

cluster_task.add_parallel_body_task(job_task)

DEAL_CHANNEL_AVAIL_MATCH_INVENTORY_RECOMMENDATIONS_BATCH_TYPE_ID = 4
MONGODB_ACCOUNT = 'ttd-recommendations' if env == TtdEnvFactory.prod else 'pap-invmkt-test-east'
MONGODB_DATABASE = MONGODB_ACCOUNT if env == TtdEnvFactory.prod else 'pap-test'
MONGODB_COLLECTION = 'RawRecommendations' if env == TtdEnvFactory.prod else 'Source'
MONGODB_VERSION_COLLECTION = 'Versions'
MONGODB_SECRET_KEY = 'cosmosdb-eastus-ttd-recommendations-azure-managed'
# Even with server-side retry enabled, limit batch size to reduce server request time-out error
# prod DB has 10x more RU/s compared to prod test
MAX_BATCH_SIZE = f"{500}" if env == TtdEnvFactory.prod else f'{50}'
MONGODB_TTL_DURATION_IN_SECONDS = f"{14 * 24 * 60 * 60}"  # 14 days in seconds

s3_to_mongodb_dataloader_cluster = EmrClusterTask(
    name=f"{dataloader_name}-cluster",
    retries=0,
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, 1),
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
)

version_id = f"{{{{ task_instance.xcom_pull(task_ids='{CREATE_VERSION_TASK_ID}', key='{VERSION_ID_KEY}') }}}}"
dataloader_java_options = [
    # ==== MongoDB settings ====
    ("TargetMongoDBAccount", MONGODB_ACCOUNT),
    ("TargetMongoDBDatabase", MONGODB_DATABASE),
    ("TargetMongoDBCollection", MONGODB_COLLECTION),
    ("TargetMongoDBVersionCollection", MONGODB_VERSION_COLLECTION),
    ("TargetMongoDBSecretKey", MONGODB_SECRET_KEY),
    ("MaxBatchSize", MAX_BATCH_SIZE),
    ("TTLDurationInSecondsKey", MONGODB_TTL_DURATION_IN_SECONDS),
    # ==== Other settings ====
    ("runDateTime", runDateTime),
    ("VersionId", version_id),
    # ("VersionId", "0"),  # Used in prod test
    # ==== Prod-test only setting ====
    # ("ttd.ds.AdGroupChannelMatchDataSet.isInChain", "true"),
    # ("exportPercentageBuckets", "0,1"),
    ("TestMaxRecords", "15000"),
    ("TestMongoDBUsername", "pap-invmkt-test-east"),
    ("TestMongoDBPassword", "<ENTER_PROD_TEST_PASSWORD>"),
]

dataloader_spark_options = [("packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")]

s3_to_mongodb_dataloader_task = EmrJobTask(
    name=f"{dataloader_name}-job-task",
    retries=0,
    class_name=dataloader_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=dataloader_spark_options,
    eldorado_config_option_pairs_list=dataloader_java_options
)

s3_to_mongodb_dataloader_cluster.add_parallel_body_task(s3_to_mongodb_dataloader_task)

create_version = build_create_version_task(DEAL_CHANNEL_AVAIL_MATCH_INVENTORY_RECOMMENDATIONS_BATCH_TYPE_ID)
set_version_to_complete = build_set_version_to_complete_task(DEAL_CHANNEL_AVAIL_MATCH_INVENTORY_RECOMMENDATIONS_BATCH_TYPE_ID)
create_version_task = OpTask(op=create_version)
set_version_to_complete_task = OpTask(op=set_version_to_complete)

ttd_dag >> check_required_datasets >> cluster_task
if env == TtdEnvFactory.prod:
    cluster_task >> create_version_task >> s3_to_mongodb_dataloader_cluster >> set_version_to_complete_task
else:
    cluster_task >> s3_to_mongodb_dataloader_cluster
