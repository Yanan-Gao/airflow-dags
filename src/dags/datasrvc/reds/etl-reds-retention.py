import json
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
import logging

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dags.datasrvc.utils.common import is_prod
from dags.datasrvc.reds.feed_backfill import add_back_fill_prefix
from dags.datasrvc.reds.redsfeed import RedsFeed
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdprometheus import get_or_register_gauge, push_all
from ttd.ttdslack import dag_post_to_slack_callback

testing = not is_prod()


def choose(prod, test):
    return test if testing else prod


provisioning_conn_id = choose(prod='provdb-readonly', test='provdb-readonly')

prefix = choose(prod='', test='dev-')
aws_conn_id = 'aws_default'
job_name = f'{prefix}etl-reds-retention'
feed_bucket = choose(prod='thetradedesk-useast-partner-datafeed', test='thetradedesk-useast-partner-datafeed-test2')
feed_backup_bucket = choose(
    prod='thetradedesk-useast-partner-datafeed-scrubber-new-source', test='thetradedesk-useast-partner-datafeed-test2'
)
bucket_workitem = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_bucket_jar = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_partitions = '100'


def get_success_gauge():
    return get_or_register_gauge(job_name, "reds_feed_retention_success", "Reds Feeds retention job success", [])


def get_failure_gauge():
    return get_or_register_gauge(job_name, "reds_feed_retention_failure", "Reds Feeds retention job failure", [])


pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 8, 19),
    on_failure_callback=dag_post_to_slack_callback(dag_name=job_name, step_name='', slack_channel='#scrum-data-services-alarms'),
    schedule_interval='30 1 * * *',
    max_active_runs=2,
    retries=3,
    retry_delay=timedelta(minutes=5),
    tags=['DATASRVC', 'SQL']
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag


def _get_retention_feeds(**context):
    dt = context['logical_date']
    dt_slot = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    dt_slot_end = dt_slot + timedelta(days=1)
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    conn = sql_hook.get_conn()

    feeds = list(filter(lambda x: _has_retention_work_item(x, dt_slot, dt_slot_end), RedsFeed.all(conn)))
    context['ti'].xcom_push(key='feeds', value=feeds)


def _has_retention_work_item(feed, dt_slot, dt_slot_end):
    return feed.destination_type == 'S3' and \
           feed.min_enable_date is not None and \
           feed.min_enable_date.replace(tzinfo=timezone.utc) < dt_slot_end - timedelta(days=feed.retention_period_in_days) and \
           (feed.max_disable_date is None or feed.max_disable_date.replace(tzinfo=timezone.utc) > dt_slot - timedelta(days=feed.retention_period_in_days))


def _create_spark_config(**context):
    dt = context['logical_date']
    dt_slot = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    feeds = context['ti'].xcom_pull(task_ids='get_retention_feeds', key='feeds')
    # Rebuild the list of RedsFeed objects
    rebuilt_feeds = [RedsFeed(*feed) for feed in feeds]

    configs = set()
    for feed in rebuilt_feeds:
        expired_date_string = (dt_slot - timedelta(days=feed.retention_period_in_days)).strftime('%Y-%m-%d')
        path_prefix = f'{feed.source_path}/date={expired_date_string}'
        # add the feed bucket
        options = {'Prefix': path_prefix, 'SourceBucket': feed_bucket}
        configs.add(json.dumps(options))
        # also add the scrub backup bucket
        options = {'Prefix': path_prefix, 'SourceBucket': feed_backup_bucket}
        configs.add(json.dumps(options))
        # if concat is configured, also add the concat path
        if feed.has_concat:
            path_prefix = f'{feed.destination_path}/date={expired_date_string}'
            options = {'Prefix': path_prefix, 'SourceBucket': feed_bucket}
            configs.add(json.dumps(options))
    # add back-fill prefix
    add_back_fill_prefix(configs, dt_slot)

    data = '\n'.join(configs)
    now_dt = datetime.now(timezone.utc)
    now = now_dt.strftime('%Y-%m-%dT%H-%M-%S')
    date = str(dt.date())
    hour = str(dt.hour)

    s3_object_path = f'reds-data-retention/{dt.year}/{dt.month}/{dt.day}/{prefix}{date}T{hour}-{now}.json'
    s3_uri = f's3://{bucket_workitem}/{s3_object_path}'
    out = s3_uri.replace('.json', '.result.json')

    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    s3_hook.load_string(data, s3_object_path, bucket_workitem)
    logging.info(f'{len(configs)} work items written to {s3_uri}')
    context['ti'].xcom_push(key='workitem_input', value=s3_uri)
    context['ti'].xcom_push(key='workitem_output', value=out)


def _validate_spark_output(**context):
    out = context['ti'].xcom_pull(dag_id=job_name, task_ids='create_spark_config', key='workitem_output')
    logging.info(f'Validating output for {out}')
    # S3 connection
    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    results = [json.loads(line) for line in s3_hook.read_key(out).splitlines()]
    failures = [feed for feed in results if not feed.get('Completed', False) or feed.get('FailedKeys', 0)]
    successes = [feed for feed in results if feed.get('Completed', False) and not feed.get('FailedKeys', 0)]
    logging.info(f'Successfully run retention task for {len(successes)} feed work items')
    get_success_gauge().set(len(successes))
    get_failure_gauge().set(len(failures))
    push_all(job_name)

    # raise exception if there are any failures
    if failures:
        count = len(failures)
        for i, feed in enumerate(failures, start=1):
            print(
                f'({i}/{count}) Failed to run retention task for prefix={feed.get("Prefix", "Unknown")} sourceBucket={feed.get("SourceBucket", "Unknown")} failedKeys={feed.get("FailedKeys", "Unknown")}: {feed.get("Exception", "Unknown")}'
            )
        raise AirflowFailException(f'Failed to run retention task for {count} feeds')


master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=4,
)

reds_deletion_cluster = EmrClusterTask(
    name=f'{prefix}reds_deletion_cluster',
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        'Team': DATASRVC.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_auto_terminates=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_1
)

reds_deletion_step = EmrJobTask(
    cluster_specs=reds_deletion_cluster.cluster_specs,
    name=f'{prefix}reds_deletion_step',
    class_name='com.thetradedesk.reds.retention.RedsS3Deleter',
    executable_path=f's3://{scrub_bucket_jar}/reds-retention/uber-REDS_S3_Data_Retention-2.0-SNAPSHOT.jar',
    command_line_arguments=[
        '--partitions', scrub_partitions, '--testMode', 'false', '--mode', 'delete', '--outputUrl',
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_output") }}}}',
        f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_input") }}}}'
    ],
    configure_cluster_automatically=True
)

get_retention_feeds = OpTask(
    op=PythonOperator(task_id='get_retention_feeds', python_callable=_get_retention_feeds, provide_context=True, dag=dag)
)

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

reds_deletion_cluster.add_sequential_body_task(reds_deletion_step)

pipeline >> get_retention_feeds >> create_spark_config >> reds_deletion_cluster >> validate_spark_output
