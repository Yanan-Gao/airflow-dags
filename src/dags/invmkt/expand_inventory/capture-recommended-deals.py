from datetime import timedelta

from dags.hpc.cloud_storage_to_sql_db_task.cloud_storage_to_sql_db_task import create_cloud_storage_to_sql_db_task, CloudStorageToSqlColumn
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask
from ttd.eldorado.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import hpc
from datetime import datetime
from dags.invmkt.expand_inventory.common import build_s3_destination_uri_task, \
    get_is_prod_enviroment, BUILD_S3_DESTINATION_URI_TASK_ID, \
    build_create_version_task, build_cleanup_old_batches_task, S3_DEST_URI_BUCKET_KEY, S3_DEST_URI_PREFIX_KEY, \
    CREATE_VERSION_TASK_ID, VERSION_ID_KEY, \
    S3_DEST_URI_KEY, build_set_version_to_complete_task, PROVISIONING_DB_NAME, EIS_DOMAIN_NAME

DAG_ID = 'capture-recommended-deals'
job_schedule_interval = timedelta(days=7)
job_start_date = datetime(2024, 3, 19, 00, 00)
INVMKT_TEAM = 'INVMKT'
INVMKT_ALARM_SLACK_CHANNEL = 'scrum-invmkt-alarms'
is_prod_environment = get_is_prod_enviroment()

capture_recommended_deals_dag = TtdDag(
    dag_id=DAG_ID,
    schedule_interval=job_schedule_interval if is_prod_environment else None,
    start_date=job_start_date,
    retries=3,
    tags=[INVMKT_TEAM, 'MSSQL'],
    run_only_latest=True,
    max_active_runs=1,
    slack_channel=INVMKT_ALARM_SLACK_CHANNEL
)

dag = capture_recommended_deals_dag.airflow_dag

STORAGE_BUCKET = "ttd-inventory-recommendations-metadata"
S3_DATASET_NAME = 'recommended_deals'

build_s3_destination_uri_task = build_s3_destination_uri_task(STORAGE_BUCKET, EIS_DOMAIN_NAME, S3_DATASET_NAME, is_prod_environment)

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3.i3_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=20
)

# First EMR version that supports Spark 3.2.1 which is used by sthe expand-inventory-selection package.
EMR_RELEASE = 'emr-6.7.0'
cluster_task = EmrClusterTask(
    name='capture_recommended_deals_cluster',
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=EMR_RELEASE,
    cluster_tags={"Team": INVMKT_TEAM},
    enable_prometheus_monitoring=True,
)

capture_and_persist_recommended_deals_to_s3_task = EmrJobTask(
    name="capture_and_persist_recommended_deals_to_s3_task",
    class_name="com.thetradedesk.ds.libs.expandinventoryselection.spark.CaptureAdGroupsRecommendedDealsJob",
    executable_path=
    "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/ds/libs/expandinventoryselection-assembly.jar",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ("VersionId", f"{{{{ task_instance.xcom_pull(task_ids='{CREATE_VERSION_TASK_ID}', key='{VERSION_ID_KEY}') }}}}"),
        ("S3DestinationUri", f"{{{{ task_instance.xcom_pull(task_ids='{BUILD_S3_DESTINATION_URI_TASK_ID}', key='{S3_DEST_URI_KEY}') }}}}")
    ],
)
cluster_task.add_parallel_body_task(capture_and_persist_recommended_deals_to_s3_task)

RECOMMENDED_DEALS_BATCH_TYPE_ID = 1
create_version_task = build_create_version_task(RECOMMENDED_DEALS_BATCH_TYPE_ID)
"""
Branch name to be used for sandbox, can be just release branch but there must be an ongoing branch deployment for it.
"""
test_branch_for_task_service = 'release-2024.07.16'
PERSISTENCE_TASK_NAME = "persist_recommended_deals_to_provisioning"

DATASET_NAME = "inventory_selection_recommended_deals"
PERSISTENCE_STAGING_BATCH_SIZE = 50_000
PERSISTENCE_SLEEP_BETWEEN_BATCHES_IN_SECONDS = 5
PERSISTENCE_PROCESS_TIMEOUT_IN_SECONDS = 12 * 60 * 60
persist_recommended_deals_to_provisioning_task = create_cloud_storage_to_sql_db_task(
    name=PERSISTENCE_TASK_NAME,
    scrum_team=hpc,
    dataset_name=DATASET_NAME,
    storage_bucket=f"{{{{ task_instance.xcom_pull(task_ids='{BUILD_S3_DESTINATION_URI_TASK_ID}', key='{S3_DEST_URI_BUCKET_KEY}') }}}}",
    storage_key_prefix=f"{{{{ task_instance.xcom_pull(task_ids='{BUILD_S3_DESTINATION_URI_TASK_ID}', key='{S3_DEST_URI_PREFIX_KEY}') }}}}",
    destination_database_name=PROVISIONING_DB_NAME,
    destination_table_schema_name="dbo",
    destination_table_name="InventorySelectionRecommendedDeal",
    column_mapping=[
        CloudStorageToSqlColumn("AdGroupId", "AdGroupId"),
        CloudStorageToSqlColumn("PrivateContractId", "PrivateContractId"),
        CloudStorageToSqlColumn("approxPast7DayAvails", "AvailsCount"),
        CloudStorageToSqlColumn("VersionId", "VersionId"),
    ],
    insert_to_staging_table_batch_size=PERSISTENCE_STAGING_BATCH_SIZE,
    upsert_to_destination_table_batch_size=PERSISTENCE_STAGING_BATCH_SIZE,
    upsert_to_destination_table_batch_delay=PERSISTENCE_SLEEP_BETWEEN_BATCHES_IN_SECONDS,
    upsert_to_destination_table_timeout=PERSISTENCE_PROCESS_TIMEOUT_IN_SECONDS,
    task_execution_timeout=timedelta(seconds=PERSISTENCE_PROCESS_TIMEOUT_IN_SECONDS * 2),
    use_sandbox=not is_prod_environment,
    branch_name=test_branch_for_task_service if not is_prod_environment else None
)

set_version_to_complete_task = build_set_version_to_complete_task(RECOMMENDED_DEALS_BATCH_TYPE_ID)

CLEANUP_SPROC_NAME = 'prc_DeleteFromInventorySelectionRecommendedDeal'
CLEANUP_BATCH_SIZE = 1_000_000
CLEANUP_SLEEP_BETWEEN_BATCHES_IN_SECONDS = 3

cleanup_old_batches_task = build_cleanup_old_batches_task(CLEANUP_SPROC_NAME, CLEANUP_BATCH_SIZE, CLEANUP_SLEEP_BETWEEN_BATCHES_IN_SECONDS)
capture_recommended_deals_dag >> OpTask(op=build_s3_destination_uri_task) >> OpTask(op=create_version_task) >> cluster_task

capture_and_persist_recommended_deals_to_s3_task >> persist_recommended_deals_to_provisioning_task >> OpTask(
    op=set_version_to_complete_task
) >> OpTask(op=cleanup_old_batches_task)
