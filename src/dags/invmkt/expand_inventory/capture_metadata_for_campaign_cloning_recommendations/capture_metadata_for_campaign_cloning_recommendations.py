from datetime import timedelta
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from datetime import datetime
from dags.invmkt.expand_inventory.common import get_is_prod_enviroment, \
    build_s3_destination_uri_task, BUILD_S3_DESTINATION_URI_TASK_ID, \
    S3_DEST_URI_KEY, INVMKT_TEAM, INVMKT_ALARM_SLACK_CHANNEL, build_create_version_task, \
    build_set_version_to_complete_task, CREATE_VERSION_TASK_ID, VERSION_ID_KEY
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE
from ttd.tasks.op import OpTask

DAG_ID = 'capture_metadata_for_campaign_cloning_recommendations'
STORAGE_BUCKET = 'ttd-inventory-recommendations-metadata'
DOMAIN_NAME = 'campaign_cloning'
S3_DATASET_NAME = 'properties'
MONGODB_DATABASE = 'ttd-recommendations'
RAW_RECOMMENDATIONS_MONGODB_COLLECTION = 'RawRecommendations'
MONGODB_SECRET_KEY = 'cosmosdb-eastus-ttd-recommendations-azure-managed'
EXECUTABLE_PATH = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/ds/libs/expandinventoryselection-assembly.jar"
MONGODB_TTL_DURATION_IN_SECONDS = 14 * 24 * 60 * 60  # 14 days in seconds
CAMPAIGN_CLONING_QUALIFIED_INVENTORY_RECOMMENDATIONS_BATCH_TYPE_ID = 3
MAX_BATCH_SIZE = '1024'

job_schedule_interval = timedelta(days=7)
job_start_date = datetime(2024, 6, 24, 00, 00)

is_prod_environment = get_is_prod_enviroment()

capture_metadata_for_campaign_cloning_recommendations_dag = TtdDag(
    dag_id=DAG_ID,
    schedule_interval=job_schedule_interval if is_prod_environment else None,
    start_date=job_start_date,
    retries=2,
    tags=[INVMKT_TEAM],
    run_only_latest=True,
    max_active_runs=1,
    slack_channel=INVMKT_ALARM_SLACK_CHANNEL
)
dag = capture_metadata_for_campaign_cloning_recommendations_dag.airflow_dag

build_s3_destination_uri_task = build_s3_destination_uri_task(STORAGE_BUCKET, DOMAIN_NAME, S3_DATASET_NAME, is_prod_environment)

capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_cluster = EmrClusterTask(
    name="capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_cluster",
    cluster_tags={
        'Team': INVENTORY_MARKETPLACE.team.jira_team,
    },
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            I3.i3_8xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            I3.i3_16xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=20,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
)

capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_task = EmrJobTask(
    name="capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_task",
    class_name="com.thetradedesk.ds.libs.expandinventoryselection.spark.CaptureMetadataForCampaignCloningRecommendationsJob",
    executable_path=EXECUTABLE_PATH,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ("VersionId", f"{{{{ task_instance.xcom_pull(task_ids='{CREATE_VERSION_TASK_ID}', key='{VERSION_ID_KEY}') }}}}"),
        ("S3DestinationUri", f"{{{{ task_instance.xcom_pull(task_ids='{BUILD_S3_DESTINATION_URI_TASK_ID}', key='{S3_DEST_URI_KEY}') }}}}")
    ],
)

s3_to_mongodb_dataloader_cluster = EmrClusterTask(
    name="s3_to_mongodb_dataloader_cluster",
    cluster_tags={
        'Team': INVENTORY_MARKETPLACE.team.jira_team,
    },
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            M5.m5_xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            M5.m5_xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=2,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
    retries=0
)

s3_to_mongodb_dataloader_task = EmrJobTask(
    name="s3_to_mongodb_dataloader_task",
    class_name="com.thetradedesk.ds.libs.expandinventoryselection.spark.S3ToMongoDBDataLoaderJob",
    executable_path=EXECUTABLE_PATH,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=[("packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")],
    eldorado_config_option_pairs_list=[
        ("S3DestinationUri", f"{{{{ task_instance.xcom_pull(task_ids='{BUILD_S3_DESTINATION_URI_TASK_ID}', key='{S3_DEST_URI_KEY}') }}}}"),
        ("TTLDurationInSecondsKey", MONGODB_TTL_DURATION_IN_SECONDS), ("TargetMongoDBDatabase", MONGODB_DATABASE),
        ("TargetMongoDBCollection", RAW_RECOMMENDATIONS_MONGODB_COLLECTION), ("TargetMongoDBSecretKey", MONGODB_SECRET_KEY),
        ("MaxBatchSizeKey", MAX_BATCH_SIZE)
    ],
)

capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_cluster.add_parallel_body_task(
    capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_task
)
s3_to_mongodb_dataloader_cluster.add_parallel_body_task(s3_to_mongodb_dataloader_task)

create_version_task = build_create_version_task(CAMPAIGN_CLONING_QUALIFIED_INVENTORY_RECOMMENDATIONS_BATCH_TYPE_ID)
set_version_to_complete_task = build_set_version_to_complete_task(CAMPAIGN_CLONING_QUALIFIED_INVENTORY_RECOMMENDATIONS_BATCH_TYPE_ID)

capture_metadata_for_campaign_cloning_recommendations_dag >> OpTask(op=build_s3_destination_uri_task) >> OpTask(
    op=create_version_task
) >> capture_and_persist_metadata_for_campaign_cloning_recommendations_to_s3_cluster >> s3_to_mongodb_dataloader_cluster >> OpTask(
    op=set_version_to_complete_task
)
