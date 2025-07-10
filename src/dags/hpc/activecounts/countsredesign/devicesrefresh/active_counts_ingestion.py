from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import dags.hpc.constants as constants
import dags.hpc.utils as hpc_utils
from dags.hpc.cloud_storage_to_sql_db_task.cloud_storage_to_sql_db_task import \
    create_cloud_storage_to_sql_db_task_with_airflow_dataset
from dags.hpc.cloud_storage_to_sql_db_task.utils import CloudStorageToSqlColumn
from dags.hpc.counts_datasources import CountsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import hpc
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'active-counts-ingestion'
cadence_in_hours = "{{ dag_run.conf.get('cadence_in_hours') }}"
job_start_date = datetime(2025, 3, 14, 1, 0)
active_targeting_data_ids_per_file_count = 10000

# Prod Variables
schedule = None  # triggered by counts-devices-refresh dag
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL
file_url = constants.TARGETING_DATA_FINAL_S3
taskservice_branch_name = None

# Test Variables
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-6703-write-active-targeting-data-ids/latest/eldorado-hpc-assembly.jar"
# file_url = constants.TARGETING_DATA_STAGING_S3
# taskservice_branch_name = "sjh-HPC-6619-export-trend-count-release"
# Use CountsDatasources.active_targeting_data_ids_aws.with_env(TestEnv()) for test
# Change SegmentCounts.DataSetEnvironment to Test

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/ZoBAG',
    tags=[hpc.jira_team],
)
adag = dag.airflow_dag

###########################################
# Check the SQS Queue
###########################################
is_sqs_queue_empty_task = OpTask(
    op=PythonOperator(
        task_id="is_sqs_queue_empty",
        python_callable=hpc_utils.is_sqs_queue_empty,
        op_kwargs={
            'queue_name': "sns-s3-useast-logs-2-targetingdataidsforsibcounts-collected-queue",
            'region_name': constants.DEFAULT_AWS_REGION
        }
    )
)

###########################################
# Steps
###########################################
run_date_time = '{{ dag_run.start_date.strftime("%Y-%m-%dT%H:00:00") }}'

master_instance_types = [C7g.c7g_xlarge().with_fleet_weighted_capacity(1), C7g.c7g_2xlarge().with_fleet_weighted_capacity(1)]
core_instance_types = [C7g.c7g_4xlarge().with_fleet_weighted_capacity(1)]

eldorado_config_option_pairs_list = [
    ('datetime', run_date_time), ('cadenceInHours', cadence_in_hours),
    ('activeTargetingDataIdsPerFileCount', active_targeting_data_ids_per_file_count), ('fileUrl', file_url),
    ('generation', int(timezone.utcnow().replace(tzinfo=timezone.utc).timestamp())),
    ('triggerTargetingDataCountsSqsProcessor', f"{{{{ task_instance.xcom_pull('{is_sqs_queue_empty_task.task_id}') | string | lower }}}}")
]

additional_args_option_pairs_list = [('conf', 'spark.yarn.maxAppAttempts=1')]

task_name = 'write-active-targeting-data-ids'
class_name = 'com.thetradedesk.jobs.activecounts.countsredesign.writeactivetargetingdataids.WriteActiveTargetingDataIds'

cluster_task = EmrClusterTask(
    name=task_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=core_instance_types,
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    region_name="us-east-1"
)

job_task = EmrJobTask(
    name=task_name,
    executable_path=aws_jar,
    class_name=class_name,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
    timeout_timedelta=timedelta(hours=24),
    configure_cluster_automatically=True,
)

cluster_task.add_sequential_body_task(job_task)

segment_counts_op_task = OpTask(
    op=TaskServiceOperator(
        task_name="SegmentCountsTask",
        scrum_team=hpc,
        task_config_name="SegmentCountsTaskConfig",
        resources=TaskServicePodResources.large(),
        task_execution_timeout=timedelta(days=1),
        branch_name=taskservice_branch_name,
        configuration_overrides={
            "SegmentCounts.FileBatchSize": "10000",
            "SegmentCounts.DataSetEnvironment": "Prod",
            "SegmentCounts.RequestBatchSize": "200",
            "SegmentCounts.MaxParallelCountsServiceRequests": "200",
            "SegmentCounts.RunDateTime": run_date_time,
        }
    )
)

###########################################
#   Push Active Counts To TargetingDataUniquesV2
###########################################

active_counts_column_mapping = [
    CloudStorageToSqlColumn('TargetingDataId', 'TargetingDataId'),
    CloudStorageToSqlColumn('Device', 'Uniques'),
    CloudStorageToSqlColumn('Web', 'UniquesWeb'),
    CloudStorageToSqlColumn('InApp', 'UniquesInApp'),
    CloudStorageToSqlColumn('Ctv', 'UniquesConnectedTv'),
    CloudStorageToSqlColumn('Person', 'Individuals'),
    CloudStorageToSqlColumn('Household', 'Households'),
    CloudStorageToSqlColumn('LastGeneratedAt', 'LastUpdatedAt')
]

push_active_counts_to_provisioning_task = create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name="push-active-counts-to-provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.segment_counts_export,
    dataset_partitioning_args=CountsDatasources.segment_counts_export.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime("%Y-%m-%d %H:00:00") }}'
    ),
    destination_database_name="Provisioning",
    destination_table_schema_name="dbo",
    destination_table_name="TargetingDataUniquesV2",
    column_mapping=active_counts_column_mapping,
    insert_to_staging_table_timeout=timedelta(hours=1).seconds,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=1,
    upsert_to_destination_table_timeout=timedelta(hours=8).seconds,
    resources=TaskServicePodResources.medium(),
    branch_name=taskservice_branch_name,
    task_retries=1
)

push_active_counts_to_targetingdb_task = create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name="push-active-counts-to-targetingdb",
    scrum_team=hpc,
    dataset=CountsDatasources.segment_counts_export,
    dataset_partitioning_args=CountsDatasources.segment_counts_export.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime("%Y-%m-%d %H:00:00") }}'
    ),
    destination_database_name="Targeting",
    destination_table_schema_name="dbo",
    destination_table_name="TargetingDataUniquesV2",
    column_mapping=active_counts_column_mapping,
    insert_to_staging_table_timeout=timedelta(hours=1).seconds,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=20,  # making it 20 to help with the load on Algolia
    upsert_to_destination_table_timeout=timedelta(hours=8).seconds,
    resources=TaskServicePodResources.medium(),
    branch_name=taskservice_branch_name,
    task_retries=1
)

###########################################
#   Push Active Counts Trend To Provisioning
###########################################

active_counts_trend_column_mapping = [
    CloudStorageToSqlColumn('TargetingDataId', 'TargetingDataId'),
    CloudStorageToSqlColumn('ActiveCountTrend', 'ActiveCountsTrend'),
    CloudStorageToSqlColumn('LastGeneratedAt', 'LastGeneratedAt')
]

push_active_counts_trend_to_provisioning_task = create_cloud_storage_to_sql_db_task_with_airflow_dataset(
    name="push-active-counts-trend-to-provisioning",
    scrum_team=hpc,
    dataset=CountsDatasources.segment_counts_export,
    dataset_partitioning_args=CountsDatasources.segment_counts_export.get_partitioning_args(
        ds_date='{{ dag_run.start_date.strftime("%Y-%m-%d %H:00:00") }}'
    ),
    destination_database_name="Provisioning",
    destination_table_schema_name="dbo",
    destination_table_name="ActiveCountsTrend",
    column_mapping=active_counts_trend_column_mapping,
    insert_to_staging_table_timeout=timedelta(hours=1).seconds,
    upsert_to_destination_table_batch_size=10000,
    upsert_to_destination_table_batch_delay=0.5,
    upsert_to_destination_table_timeout=timedelta(hours=2).seconds,
    ensure_consistency_between_source_and_destination=True,
    resources=TaskServicePodResources.medium(),
    branch_name=taskservice_branch_name,
    task_retries=1
)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> is_sqs_queue_empty_task >> cluster_task >> segment_counts_op_task
segment_counts_op_task >> push_active_counts_to_provisioning_task >> final_dag_check
segment_counts_op_task >> push_active_counts_to_targetingdb_task >> final_dag_check
segment_counts_op_task >> push_active_counts_trend_to_provisioning_task >> final_dag_check
