"""
Perform REDS concatenation via Spark
"""

import json
import logging
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator

from dags.datasrvc.materialized_gating.materialized_gating_sensor import MaterializedGatingSensor
from dags.datasrvc.reds.redsfeed import RedsFeed

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from collections import defaultdict
from typing import DefaultDict
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

from ttd.ttdprometheus import get_or_register_gauge, push_all
from prometheus_client.metrics import Gauge

testing = False


def choose(prod, test):
    return test if testing else prod


prefix = choose(prod='', test='dev-')
job_name = f'{prefix}etl-reds-concat'
aws_conn_id = 'aws_default'
lwdb_conn_id = 'lwdb-datasrvc'
provisioning_conn_id = 'provdb-readonly'

reds_bucket_jar = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
reds_bucket_workitem = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
reds_bucket_destination_override = choose(prod=None, test='thetradedesk-useast-partner-datafeed-test2')
reds_bucket_source_for_concatenated = choose(
    prod='thetradedesk-useast-partner-datafeed-scrubber-new-source', test='thetradedesk-useast-partner-datafeed-scrubber-new-source'
)

CONCAT_SLA_IN_SECONDS = 4500  # 75 mins

pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 8, 28, hour=4),
    on_failure_callback=dag_post_to_slack_callback(dag_name=job_name, step_name='', slack_channel='#scrum-data-services-alarms'),
    schedule_interval='0 * * * *',
    max_active_runs=8,
    retries=20,
    retry_delay=timedelta(minutes=30),
    tags=['DATASRVC', 'SQL']
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag


def _get_concat_reds_feeds(**context):
    dt = context['logical_date'].replace(tzinfo=None)
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    conn = sql_hook.get_conn()
    feeds = [
        feed for feed in RedsFeed.all(conn) if feed.has_concat and (
            feed.status == 'Active' or
            (feed.status == 'Disabled' and (feed.max_disable_date is None or dt < feed.max_disable_date + timedelta(hours=1)))
        )
    ]
    context['ti'].xcom_push(key='feeds', value=feeds)


def _create_spark_config(**context):
    dt = context['logical_date'].replace(tzinfo=None)
    date = str(dt.date())
    hour = str(dt.hour)

    feeds = context['ti'].xcom_pull(task_ids='get_concat_reds_feeds', key='feeds')
    configs = []
    # Rebuild the list of RedsFeed objects
    rebuilt_feeds = [RedsFeed(*feed) for feed in feeds]
    for feed in rebuilt_feeds:
        options = {
            'FeedId': feed.feed_id,
            'FeedVersion': feed.version,
            'FeedType': feed.feed_type_name.lower(),
            'Prefix': f'{feed.source_path}/date={date}/hour={hour}',
            'Include': feed.get_date_hour_grouping_regex(dt),
            'ShouldConcatenate': True,
            'ConcatenationPrefix': feed.get_date_hour_concat_prefix(dt),
            'Date': date,
            'Hour': hour,
            'SourceBucket': feed.bucket,
            'SourceBucketForConcatenated': reds_bucket_source_for_concatenated,
            'DestinationBucket': reds_bucket_destination_override or feed.bucket,
            'ShouldAddHeader': bool(feed.header),
        }
        if feed.header:
            options['Schema'] = feed.header
        configs.append(json.dumps(options))

    data = '\n'.join(configs)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%S')
    s3_object_path = f'reds-concat/{dt.year}/{dt.month}/{dt.day}/{prefix}{date}T{hour}-{now}.json'
    s3_uri = f's3://{reds_bucket_workitem}/{s3_object_path}'
    out = s3_uri.replace('.json', '.result.json')

    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    s3_hook.load_string(data, s3_object_path, reds_bucket_workitem)
    context['ti'].xcom_push(key='workitem_input', value=s3_uri)
    context['ti'].xcom_push(key='workitem_output', value=out)
    print(s3_uri)
    print(out)


def _validate_spark_output(**context):
    out = context['ti'].xcom_pull(dag_id=job_name, task_ids='create_spark_config', key='workitem_output')

    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    obj = s3_hook.get_key_object(out)

    results = [json.loads(line) for line in obj.get()['Body'].iter_lines()]
    failures = [feed for feed in results if not feed.get('Completed', False) or feed.get('FailedKeys', 0)]
    if failures:
        count = len(failures)
        for i, feed in enumerate(failures, start=1):
            print(f'({i}/{count}) Failed to concat feed_id={feed.get("FeedId", "Unknown")}: {feed.get("Exception", "Unknown")}')
        raise AirflowFailException(f'Failed to concat {count} feeds')

    logging.info('Validating that all results have user metadata attached')
    for result in results:
        if result['ProcessedKeys'] == 0:
            continue
        bucket = result['DestinationBucket']
        key = result['ConcatenationPrefix'].replace('<index>', '0')
        obj = s3_hook.get_key_object(key, bucket)
        metadata = obj.get()['Metadata']
        if 'feed-id' not in metadata:
            raise AirflowFailException(f'Missing user metadata for s3://{bucket}/{key}')
        if result['FeedId'] != metadata['feed-id']:
            raise AirflowFailException(f'Expected FeedId {result["FeedId"]}, found {metadata["feed-id"]}')

    # Register metrics
    # Counter is unsupported, use gauge instead
    reds_concat_feed_processed_gauge: Gauge = get_or_register_gauge(
        job_name, "reds_concat_feed_processed_count", "Counts the number of REDS feed IDs with a non-zero processed key.", ["feed_type"]
    )

    # Counter is unsupported, use gauge instead
    reds_concat_feed_processed_within_sla_gauge: Gauge = get_or_register_gauge(
        job_name, "reds_concat_feed_processed_within_sla", "Counts the number of REDS feed IDs processed within the SLA.", ["feed_type"]
    )

    concat_min_latency_gauge: Gauge = get_or_register_gauge(
        job_name, "reds_concat_min_latency_seconds", "Minimum latency of REDS concatenation feed per run in seconds.", ["feed_type"]
    )

    concat_max_latency_gauge: Gauge = get_or_register_gauge(
        job_name, "reds_concat_max_latency_seconds", "Maximum latency of REDS concatenation feed per run in seconds.", ["feed_type"]
    )

    concat_avg_latency_gauge: Gauge = get_or_register_gauge(
        job_name, "reds_concat_avg_latency_seconds", "Average latency of REDS concatenation feed per run in seconds.", ["feed_type"]
    )

    hour_start_time = context['data_interval_start'].replace(tzinfo=None)
    hour_close_time = (hour_start_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    print(f'Hour start time: {hour_start_time.strftime("%Y-%m-%d %H:%M")}')
    print(f'Hour close time: {hour_close_time.strftime("%Y-%m-%d %H:%M")}')

    # Dictionary where keys represent feed types and values track counts
    reds_concat_feed_processed_count: DefaultDict[str, int] = defaultdict(int)
    reds_concat_feed_processed_within_sla_count: DefaultDict[str, int] = defaultdict(int)
    # Initialize counters to 0, set metrics to 0 when none meets SLA
    for feed_type in {result['FeedType'] for result in results}:
        reds_concat_feed_processed_count[feed_type] = 0
        reds_concat_feed_processed_within_sla_count[feed_type] = 0

    # Dictionary where keys represent feed types and values store latency per feed
    latency_data = defaultdict(list)

    for result in results:
        if result['ProcessedKeys'] == 0:
            continue

        # Calculate latency and update metrics
        finalized_time = datetime.strptime(result['FinalizedTime'], "%Y-%m-%d %H:%M:%S")
        latency = (finalized_time - hour_close_time).total_seconds()
        latency_data[result['FeedType']].append(latency)

        if latency <= CONCAT_SLA_IN_SECONDS:
            reds_concat_feed_processed_within_sla_count[result['FeedType']] += 1
        reds_concat_feed_processed_count[result['FeedType']] += 1

    print(f'reds_concat_feed_processed_within_sla_count: {reds_concat_feed_processed_within_sla_count}')
    print(f'reds_concat_feed_processed_count: {reds_concat_feed_processed_count}')

    for feed_type, count in reds_concat_feed_processed_count.items():
        reds_concat_feed_processed_gauge.labels(feed_type).set(count)

    for feed_type, count in reds_concat_feed_processed_within_sla_count.items():
        reds_concat_feed_processed_within_sla_gauge.labels(feed_type).set(count)

    # Calculate and record min, max, and avg latencies for each feed type
    for feed_type, latencies in latency_data.items():
        if latencies:
            min_latency = min(latencies)
            max_latency = max(latencies)
            avg_latency = sum(latencies) / len(latencies)
            concat_min_latency_gauge.labels(feed_type).set(min_latency)
            concat_max_latency_gauge.labels(feed_type).set(max_latency)
            concat_avg_latency_gauge.labels(feed_type).set(avg_latency)

            print(f'{feed_type} min latency: {min_latency}')
            print(f'{feed_type} max latency: {max_latency}')
            print(f'{feed_type} avg latency: {avg_latency}')

    # Push metrics to prometheus
    push_all(job_name)


# Define all operators here
reds_data_ready_sensor = OpTask(op=MaterializedGatingSensor(task_id="reds_data_ready_sensor", dag=dag))
sensor = reds_data_ready_sensor.first_airflow_op()
if isinstance(sensor, MaterializedGatingSensor):
    sensor.add_dependency(
        TaskId="dbo.fn_enum_Task_RawEventDataStream()",
        LogTypeId="dbo.fn_enum_LogType_BidRequest()",
        GrainId="dbo.fn_enum_TaskBatchGrain_Hourly()"
    )
    sensor.add_dependency(
        TaskId="dbo.fn_enum_Task_RawEventDataStream()",
        LogTypeId="dbo.fn_enum_LogType_BidFeedback()",
        GrainId="dbo.fn_enum_TaskBatchGrain_Hourly()"
    )
    sensor.add_dependency(
        TaskId="dbo.fn_enum_Task_RawEventDataStream()",
        LogTypeId="dbo.fn_enum_LogType_ClickTracker()",
        GrainId="dbo.fn_enum_TaskBatchGrain_Hourly()"
    )
    sensor.add_dependency(
        TaskId="dbo.fn_enum_Task_RawEventDataStream()",
        LogTypeId="dbo.fn_enum_LogType_ConversionTracker()",
        GrainId="dbo.fn_enum_TaskBatchGrain_Hourly()"
    )
    sensor.add_dependency(
        TaskId="dbo.fn_enum_Task_RawEventDataStream()",
        LogTypeId="dbo.fn_enum_LogType_VideoEvent()",
        GrainId="dbo.fn_enum_TaskBatchGrain_Hourly()"
    )

get_concat_reds_feeds = OpTask(
    op=PythonOperator(
        task_id='get_concat_reds_feeds',
        python_callable=_get_concat_reds_feeds,
        provide_context=True,
        dag=dag,
    )
)

create_spark_config = OpTask(
    op=PythonOperator(
        task_id='create_spark_config',
        python_callable=_create_spark_config,
        provide_context=True,
        dag=dag,
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_2xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=10,
)

reds_concat_cluster = EmrClusterTask(
    name=f'{prefix}reds_concat_cluster',
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        'Team': DATASRVC.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_auto_terminates=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_1
)

reds_concat_step = EmrJobTask(
    cluster_specs=reds_concat_cluster.cluster_specs,
    name=f'{prefix}reds_concat_step',
    class_name='com.thetradedesk.reds.scrubber.RedsScrubber',
    executable_path=f's3://{reds_bucket_jar}/reds-scrubbing/uber-REDS_S3_File_Scrubbing-2.0-SNAPSHOT.jar',
    command_line_arguments=[
        '--concatOnlyMode', 'true', '--testMode', 'false', '--timeoutInMinutes', '480', '--outputUrl',
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_output") }}}}',
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_input") }}}}'
    ],
    configure_cluster_automatically=True,
    timeout_timedelta=timedelta(hours=8)
)

validate_spark_output = OpTask(
    op=PythonOperator(
        task_id='validate_spark_output',
        python_callable=_validate_spark_output,
        provide_context=True,
        dag=dag,
    )
)

reds_concat_cluster.add_sequential_body_task(reds_concat_step)

pipeline >> reds_data_ready_sensor >> get_concat_reds_feeds >> create_spark_config >> reds_concat_cluster >> validate_spark_output
