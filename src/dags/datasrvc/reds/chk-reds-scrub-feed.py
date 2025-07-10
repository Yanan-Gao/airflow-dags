"""
Validate that REDS scrubbing has been performed successfully by checking a sample of feeds.
"""

import csv
import gzip
import random
import re
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dags.datasrvc.reds.redsfeed import RedsFeed
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

testing = False


def choose(prod, test):
    return test if testing else prod


prefix = choose(prod='', test='dev-')
job_name = f'{prefix}chk-reds-scrub-feed'
aws_conn_id = 'aws_default'
provisioning_conn_id = 'provdb-readonly'

pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 1),
    on_failure_callback=dag_post_to_slack_callback(dag_name=job_name, step_name='', slack_channel='#scrum-data-services-alarms'),
    schedule_interval='0 * * * *',
    max_active_runs=4,
    retries=1,
    retry_delay=timedelta(minutes=30),
    tags=['DATASRVC', 'SQL'],
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag


def get_check_dt(execution_dt):
    return execution_dt - timedelta(days=101)  # Add some buffer after our 99-day scrub


def _get_scrub_reds_feeds_sample(**context):
    dt = get_check_dt(context['logical_date'].replace(tzinfo=None))

    def scrubbable(feed):
        return (feed.status != 'Draft') and (not feed.max_disable_date or feed.max_disable_date > dt)

    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    conn = sql_hook.get_conn()
    feeds = [feed for feed in RedsFeed.all(conn) if feed.has_scrub and scrubbable(feed)]
    sample = sorted(random.sample(feeds, 100))
    context['ti'].xcom_push(key='feeds', value=sample)


def _get_scrub_reds_files_sample(**context):
    dt = get_check_dt(context['logical_date'])
    date = str(dt.date())
    hour = str(dt.hour)

    feeds = context['ti'].xcom_pull(task_ids='get_scrub_reds_feeds_sample', key='feeds')
    # Rebuild the list of RedsFeed objects
    rebuilt_feeds = [RedsFeed(*feed) for feed in feeds]

    files = []

    def get_sample(f, r, ks):
        if ks:
            for key in ks:
                if r.match(key):
                    files.append((f.feed_id, key))
                    return

    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    for feed in rebuilt_feeds:
        regex = re.compile(feed.get_date_hour_grouping_regex(dt))
        keys = s3_hook.list_keys(f'{feed.source_path}/date={date}/hour={hour}', feed.bucket)
        get_sample(feed, regex, keys)

        if feed.has_concat:
            regex = re.compile(feed.get_date_hour_concat_prefix(dt).replace('<index>', '0'))
            keys = s3_hook.list_keys(f'{feed.destination_path}/date={date}/hour={hour}', feed.bucket)
            get_sample(feed, regex, keys)

    context['ti'].xcom_push(key='files', value=files)


def _validate_scrub_reds_files(**context):
    feeds = context['ti'].xcom_pull(task_ids='get_scrub_reds_feeds_sample', key='feeds')
    # Rebuild the list of RedsFeed objects
    rebuilt_feeds = [RedsFeed(*feed) for feed in feeds]

    files = context['ti'].xcom_pull(task_ids='get_scrub_reds_files_sample', key='files')
    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)

    def scrubbed_guid(s):
        return not s or '-' not in s or s == '00000000-0000-0000-0000-000000000000'

    def scrubbed_ip(s):
        return not s or ('.' not in s and ':' not in s) or s.endswith('.0') or s.endswith('.x') or s.endswith(':0') or s.endswith(':x')

    def scrubbed_lat_long(s):
        return '.' not in s or len(s.split('.')[-1]) <= 2

    scrubbed_fields = {
        'tdid': scrubbed_guid,
        'ipaddress': scrubbed_ip,
        'latitude': scrubbed_lat_long,
        'longitude': scrubbed_lat_long,
        'deviceadvertisingid': scrubbed_guid,
        'sspauctionid': scrubbed_guid,
        'combinedidentifier': scrubbed_guid,
        'originalid': scrubbed_guid
    }

    metadata = {feed.feed_id: feed for feed in rebuilt_feeds}
    file_count = line_count = 0
    for feed_id, key in files:
        print(f'Checking feed {feed_id} and key {key}')
        file_count += 1
        feed = metadata[feed_id]
        schema = feed.schema.lower().split(',')

        obj = s3_hook.get_key_object(key, bucket_name=feed.bucket)
        with gzip.open(obj.get()['Body'], mode='rt') as f:
            reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
            header = feed.has_header
            for line in reader:
                if header:
                    header = False
                    continue
                line_count += 1
                for k, v in zip(schema, line):
                    if k in scrubbed_fields:
                        check = scrubbed_fields[k]
                        if not check(v):
                            raise AirflowFailException(f'Line for feed {feed.feed_id} in key {key} appears unscrubbed: {line}')
    print(f'Validated {line_count} lines across {file_count} files')


# Define all operators here

get_scrub_reds_feeds_sample = OpTask(
    op=PythonOperator(
        task_id='get_scrub_reds_feeds_sample',
        python_callable=_get_scrub_reds_feeds_sample,
        provide_context=True,
        dag=dag,
    )
)

get_scrub_reds_files_sample = OpTask(
    op=PythonOperator(
        task_id='get_scrub_reds_files_sample',
        python_callable=_get_scrub_reds_files_sample,
        provide_context=True,
        dag=dag,
    )
)

validate_scrub_reds_files = OpTask(
    op=PythonOperator(
        task_id='validate_scrub_reds_files',
        python_callable=_validate_scrub_reds_files,
        provide_context=True,
        dag=dag,
    )
)

# Configure dependencies

pipeline >> get_scrub_reds_feeds_sample >> get_scrub_reds_files_sample >> validate_scrub_reds_files
