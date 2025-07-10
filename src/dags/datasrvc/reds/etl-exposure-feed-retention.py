"""
Airflow DAG that runs daily to delete expired Exposure Feeds.
"""

from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdprometheus import get_or_register_gauge, push_all
from ttd.ttdslack import dag_post_to_slack_callback
from dags.datasrvc.reds.feed_utils import get_dates_to_delete, format_date
from dags.datasrvc.reds.exposurefeed import get_retention_target_query, ExposureFeed
import re

##################################
# Job Configurations
##################################
job_name = 'etl-exposure-feed-retention'
schedreporting_db_conn_id = 'schedreportingdb-readonly'
aws_conn_id = 'aws_default'

delete_batch_size = 500
dry_run = False

##################################
# DAG Configurations
##################################
default_args = {'owner': 'airflow', 'retries': 3, 'retry_delay': timedelta(seconds=30), 'depends_on_past': False}

dag = TtdDag(
    job_name,
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    on_failure_callback=dag_post_to_slack_callback(dag_name=job_name, step_name='', slack_channel='#scrum-data-services-alarms'),
    run_only_latest=True,
    tags=['DATASRVC', 'Exposure'],
    schedule_interval="0 20 * * *",
    max_active_runs=1
)

adag = dag.airflow_dag


##################################
# Task Configurations
##################################
def get_retention_targets(**context):
    feeds_expired_gauge = get_or_register_gauge(
        'get_retention_targets', name='exposure_feed_retention_dag_feeds_expired', description='', labels=[]
    )

    execution_date = context['logical_date'].date()
    print(f'etl-exposure-feed-retention: Execution date: {execution_date}')

    sql_hook = MsSqlHook(mssql_conn_id=schedreporting_db_conn_id)
    conn = sql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(get_retention_target_query.format(execution_date=execution_date.strftime('%Y-%m-%d')))

    feeds = [ExposureFeed(*row) for row in cursor]
    expired_feeds = []

    # Convert execution_date from date to datetime, to be consistent with start date and enable date forced in ExposureFeed init
    execution_date = datetime(execution_date.year, execution_date.month, execution_date.day)

    for feed in feeds:
        try:
            dates_to_delete = get_dates_to_delete(execution_date, feed.enable_date, feed.start_date, feed.retention_period)

            if dates_to_delete is not None:
                dates_to_delete_str = f"Dates To Delete: [{format_date(dates_to_delete[0])}, {format_date(dates_to_delete[1])}]"
                expired_feeds.append((feed.to_dict(), dates_to_delete))
            else:
                dates_to_delete_str = "Retention period not started"

            print(
                f"etl-exposure-feed-retention: FeedId {feed.feed_id}, Start Date {feed.start_date}, Enable Date {feed.enable_date}, Retention Period: {feed.retention_period}, {dates_to_delete_str}"
            )

        except Exception as e:
            print(f"Error processing FeedId {feed.feed_id}, Error: {e}")
            raise e

    context['ti'].xcom_push(key='feeds', value=expired_feeds)

    feeds_expired_gauge.set(len(expired_feeds))
    push_all(job='get_retention_targets')


def process_targets(**context):
    files_deleted_gauge = get_or_register_gauge(
        'process_targets', name='exposure_feed_retention_dag_files_deleted', description='', labels=[]
    )

    expired_feeds = context['ti'].xcom_pull(task_ids='get_retention_targets', key='feeds')
    files_deleted = 0

    with ThreadPoolExecutor(max_workers=4) as executor:
        for result in executor.map(delete_files, expired_feeds):
            files_deleted += result

        files_deleted_gauge.set(files_deleted)

    push_all(job='process_targets')


def delete_files(expired_feed):
    feed = ExposureFeed.from_dict(expired_feed[0])
    dates_to_delete = expired_feed[1]

    start_date = dates_to_delete[0]
    end_date = dates_to_delete[1]
    delta = timedelta(days=1)

    aws_storage = AwsCloudStorage(conn_id='aws_default')
    nums_files_to_delete = 0

    while start_date <= end_date:
        date_to_delete_string = start_date.strftime('%Y-%m-%d')

        keys = aws_storage.list_keys(
            feed.populate_destination_date_prefix(date=date_to_delete_string, raise_on_unresolved=True), feed.bucket
        )

        if keys is not None:
            files_to_delete = [
                key for key in keys if re.match(feed.populate_destination(date=date_to_delete_string, raise_on_unresolved=True), key)
            ]
            nums_files_to_delete += len(files_to_delete)
            batch_id = 0

            print(
                f'etl-exposure-feed-retention: FeedId {feed.feed_id}, Bucket {feed.bucket}, Path {feed.populate_destination_date_prefix(date=date_to_delete_string)}, Number of files to delete {len(files_to_delete)}'
            )
            while len(files_to_delete) > 0:
                batch_size = min(len(files_to_delete), delete_batch_size)
                batch_to_delete = files_to_delete[:batch_size]
                files_to_delete = files_to_delete[batch_size:]
                print(f'etl-exposure-feed-retention: FeedId {feed.feed_id}, Batch {batch_id}: Deleting {len(batch_to_delete)} files')

                if not dry_run:
                    aws_storage.remove_objects_by_keys(feed.bucket, batch_to_delete)

                batch_id += 1

        start_date += delta

    return nums_files_to_delete


get_retention_targets = OpTask(
    op=PythonOperator(task_id='get_retention_targets', python_callable=get_retention_targets, provide_context=True, dag=adag)
)

process_targets = OpTask(op=PythonOperator(task_id='process_targets', python_callable=process_targets, provide_context=True, dag=adag))

dag >> get_retention_targets >> process_targets
