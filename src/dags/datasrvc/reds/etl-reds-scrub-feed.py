import json
from datetime import datetime, timedelta, timezone
from typing import List

from airflow.exceptions import AirflowFailException
import logging

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dags.datasrvc.reds.redsfeed import RedsFeed
from dags.datasrvc.utils.common import is_prod
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdprometheus import get_or_register_gauge, push_all
from ttd.ttdslack import dag_post_to_slack_callback
import pendulum

testing = not is_prod()


def choose(prod, test):
    return test if testing else prod


provisioning_conn_id = choose(prod='provdb-readonly', test='provdb-readonly')
scrub_bucket_jar = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_bucket_workitem = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_parent_bucket = choose(prod='thetradedesk-useast-partner-datafeed', test='thetradedesk-useast-partner-datafeed-test2')
scrub_source_bucket = choose(
    prod='thetradedesk-useast-partner-datafeed-scrubber-new-source', test='thetradedesk-useast-partner-datafeed-test2'
)
scrub_destination_bucket = choose(prod='thetradedesk-useast-partner-datafeed', test='thetradedesk-useast-partner-datafeed-test2')
scrub_parallelism = '3'
scrub_partitions = '24000'
scrub_timeout_in_minutes = '360'
scrub_copy_files_from_parent = 'true'
prefix = choose(prod='', test='dev-')
aws_conn_id = 'aws_default'
job_name = f'{prefix}etl-reds-scrub'
max_scrub_hours = 6


def get_success_gauge():
    return get_or_register_gauge(job_name, "reds_feed_scrub_pii_success", "Reds Feeds PII scrubbing success", [])


def get_failure_gauge():
    return get_or_register_gauge(job_name, "reds_feed_scrub_pii_failure", "Reds Feeds PII scrubbing failure", [])


pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 1),
    run_only_latest=False,
    schedule_interval=f'0 */{max_scrub_hours} * * *',
    max_active_runs=10,
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=['DATASRVC', 'SQL'],
    slack_channel='#scrum-data-services-alarms',
    slack_tags=DATASRVC.team.jira_team,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag


def _get_scrub_feeds(**context):
    dt = context['logical_date']

    dt_slot = dt.replace(hour=(dt.hour // max_scrub_hours) * max_scrub_hours, minute=0, second=0, microsecond=0)
    print(f"dt_slot: {dt_slot}")
    dt_slot_end = dt_slot + timedelta(hours=max_scrub_hours)
    print(f"dt_slot_end: {dt_slot_end}")
    tz = pendulum.timezone('UTC')

    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    conn = sql_hook.get_conn()

    feeds = [
        feed.to_dict() for feed in RedsFeed.all(conn)
        if feed.has_scrub and feed.min_enable_date and tz.convert(feed.min_enable_date) < dt_slot_end -
        timedelta(days=feed.unscrubbed_pii_period_in_days) and
        (feed.max_disable_date is None or tz.convert(feed.max_disable_date) > dt_slot - timedelta(days=feed.unscrubbed_pii_period_in_days))
    ]
    context['ti'].xcom_push(key='feeds', value=feeds)


def _create_spark_config(**context):
    dt = context['logical_date']

    dt_slot = dt.replace(hour=(dt.hour // max_scrub_hours) * max_scrub_hours, minute=0, second=0, microsecond=0)
    print(f"dt_slot: {dt_slot}")

    feeds = context['ti'].xcom_pull(task_ids='get_scrub_feeds', key='feeds')
    # Rebuild the list of RedsFeed objects
    rebuilt_feeds = [RedsFeed.from_dict(feed) for feed in feeds]
    if testing:
        print('Testing mode: taking only 2 feeds')
        rebuilt_feeds = rebuilt_feeds[:2]
    configs = []

    for feed in rebuilt_feeds:
        print(f"feed: {feed}")
        job_start_scrub = dt_slot - timedelta(days=feed.unscrubbed_pii_period_in_days)
        job_end_scrub = job_start_scrub + timedelta(hours=max_scrub_hours - 1)
        logging.info(
            f'Scrub job for feed {feed.feed_id} : {job_start_scrub.strftime("%m/%d/%Y - %H:%M")} to {job_end_scrub.strftime("%m/%d/%Y - %H:%M")}'
        )
        curr = job_start_scrub
        while curr <= job_end_scrub:
            date = str(curr.date())
            hour = str(curr.hour)

            options = {
                'FeedId': feed.feed_id,
                'FeedVersion': feed.version,
                'FeedType': feed.feed_type_name.lower(),
                'Prefix': f'{feed.source_path}/date={date}/hour={hour}',
                'Include': feed.include,
                'ShouldConcatenate': feed.has_concat,
                'ConcatenationPrefix': feed.get_date_hour_concat_prefix(curr),
                'Date': date,
                'Hour': hour,
                'ParentBucket': choose(feed.bucket, scrub_parent_bucket),
                'ShouldAddHeader': bool(feed.header),
                'Schema': feed.schema_list,
            }
            configs.append(json.dumps(options))
            curr = curr + timedelta(hours=1)
    if len(configs) == 0:
        raise AirflowFailException('No feed found for scrubbing')

    data = '\n'.join(configs)
    now_dt = datetime.now(timezone.utc)
    now = now_dt.strftime('%Y-%m-%dT%H-%M-%S')

    date = str(dt.date())
    hour = str(dt.hour)

    s3_object_path = f'reds-scrubbing/{dt.year}/{dt.month}/{dt.day}/{prefix}{date}T{hour}-{now}.json'
    s3_uri = f's3://{scrub_bucket_workitem}/{s3_object_path}'
    out = s3_uri.replace('.json', '.result.json')

    print(f's3_uri: {s3_uri}')
    print(f'out url: {out}')

    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)

    s3_hook.load_string(data, s3_object_path, bucket_name=scrub_bucket_workitem)
    logging.info(f'{len(configs)} work items written to {s3_uri}. out: {out}')
    context['ti'].xcom_push(key='workitem_input', value=s3_uri)
    context['ti'].xcom_push(key='workitem_output', value=out)


def _validate_spark_output(**context):
    out = context['ti'].xcom_pull(dag_id=job_name, task_ids='create_spark_config', key='workitem_output')
    logging.info(f'Validating output for {out}')
    # S3 connection
    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)

    obj = s3_hook.get_key_object(key=out)
    results = [json.loads(line) for line in obj.get()['Body'].iter_lines()]
    failures = [feed for feed in results if not feed.get('Completed', False) or feed.get('FailedKeys', 0)]

    successes = [feed for feed in results if feed.get('Completed', False) and not feed.get('FailedKeys', 0)]
    logging.info(f'Successfully scrubbed {len(successes)} feed work items')
    get_success_gauge().set(len(successes))
    get_failure_gauge().set(len(failures))
    push_all(job_name)

    # raise exception if there are any failures
    if failures:
        count = len(failures)
        for i, feed in enumerate(failures, start=1):
            print(f'({i}/{count}) Failed to scrub feed_id={feed.get("FeedId", "Unknown")}: {feed.get("Exception", "Unknown")}')
        raise AirflowFailException(f'Failed to scrub {count} feeds')

    logging.info('Validating that concatenated results have user metadata attached')
    missing_concat_files: List[str] = []
    for result in results:
        if result['ProcessedKeys'] == 0 or not result.get('ShouldConcatenate', False):
            continue
        bucket = result['DestinationBucket']
        key = result['ConcatenationPrefix'].replace('<index>', '0')
        if s3_hook.check_for_key(key, bucket):
            obj = s3_hook.get_key_object(key, bucket)
            metadata = obj.get()['Metadata']
            if 'feed-id' not in metadata:
                raise AirflowFailException(f'Missing user metadata for s3://{bucket}/{key}')
            if result['FeedId'] != metadata['feed-id']:
                raise AirflowFailException(f'Expected FeedId {result["FeedId"]}, found {metadata["feed-id"]}')
        else:
            missing_concat_files.append(key)

    if missing_concat_files:
        count = len(missing_concat_files)
        for i, file in enumerate(missing_concat_files, start=1):
            print(f'({i}/{count}) Failed to validate concatenated file {file}')
        raise AirflowFailException(f'Failed to validate {count} concatenated files')


reds_scrub_cluster = EmrClusterTask(
    name=f'{prefix}reds_scrub_cluster',
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': DATASRVC.team.jira_team,
    },
    cluster_auto_terminates=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_1,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=16,
    ),
)

reds_scrub_step = EmrJobTask(
    cluster_specs=reds_scrub_cluster.cluster_specs,
    name=f'{prefix}reds_scrub_step',
    class_name='com.thetradedesk.reds.scrubber.RedsScrubber',
    executable_path=f's3://{scrub_bucket_jar}/reds-scrubbing/uber-REDS_S3_File_Scrubbing-2.0-SNAPSHOT.jar',
    command_line_arguments=[
        '-p', scrub_parent_bucket, '-s', scrub_source_bucket, '-d', scrub_destination_bucket, '--parallelism', scrub_parallelism,
        '--partitions', scrub_partitions, '--timeoutInMinutes', scrub_timeout_in_minutes, '--copyFilesFromParent',
        scrub_copy_files_from_parent, '--streamingMode', 'true', '--testMode', 'false', '--outputUrl',
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_output") }}}}',
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_input") }}}}'
    ],
    timeout_timedelta=timedelta(hours=8)
)

get_scrub_feeds = OpTask(op=PythonOperator(
    task_id='get_scrub_feeds',
    python_callable=_get_scrub_feeds,
    provide_context=True,
    dag=dag,
))

create_spark_config = OpTask(
    op=PythonOperator(
        task_id='create_spark_config',
        python_callable=_create_spark_config,
        provide_context=True,
        dag=dag,
    )
)

validate_spark_output = OpTask(
    op=PythonOperator(
        task_id='validate_spark_output',
        python_callable=_validate_spark_output,
        provide_context=True,
        dag=dag,
    )
)
reds_scrub_cluster.add_sequential_body_task(reds_scrub_step)

pipeline >> get_scrub_feeds >> create_spark_config >> reds_scrub_cluster >> validate_spark_output
