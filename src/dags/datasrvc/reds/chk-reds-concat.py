"""
Check that old/new sets of REDS concatenation files match
"""

import os
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from dags.datasrvc.reds.redsfeed import RedsFeed
from datetime import datetime, timedelta

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

job_name = 'chk-reds-concat'
provisioning_conn_id = 'provdb-readonly'
bucket_old = 'thetradedesk-useast-partner-datafeed'
bucket_new = 'thetradedesk-useast-partner-datafeed'

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': False,
}

dag = TtdDag(
    dag_id=job_name,
    default_args=default_args,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
    start_date=datetime(2025, 6, 1),
    tags=['DATASRVC', 'SQL'],
    schedule_interval='15 8 * * *',
)

adag = dag.airflow_dag


def _get_concat_reds_feeds(**context):
    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    conn = sql_hook.get_conn()
    feeds = [feed for feed in RedsFeed.all(conn) if feed.has_concat]
    context['ti'].xcom_push(key='feeds', value=feeds)


def _validate_concat_reds_feeds_files(**context):
    print(f'Checking {bucket_new} (new) against {bucket_old} (old)')
    s3_hook = AwsCloudStorage(conn_id='aws_default')

    prefixes = set()
    feeds = context['ti'].xcom_pull(key=None, task_ids='get_concat_reds_feeds')
    # Rebuild the list of RedsFeed objects
    rebuilt_feeds = [RedsFeed(*feed) for feed in feeds]

    for feed in rebuilt_feeds:
        prefixes.add(feed.destination_path + '/date=' + context['yesterday_ds'])

    count = len(prefixes)
    for i, prefix in enumerate(sorted(prefixes), start=1):
        print(f'({i}/{count}) Checking prefix "{prefix}"')

        etags_old = s3_hook.list_keys_with_filtered_value(bucket_old, prefix, "ETag")
        etags_new = s3_hook.list_keys_with_filtered_value(bucket_new, prefix, "ETag")
        mismatches = etags_old.items() - etags_new.items()
        if mismatches:
            message = os.linesep.join([
                f'  {key} (received ETag {etags_new.get(key, "None")}, expected {expected})' for key, expected in mismatches
            ])
            raise AirflowFailException(f'Mismatches found for prefix "{prefix}" on keys:{os.linesep}{message}')
        print(f'Matched {len(etags_old)} keys for prefix "{prefix}"')


# Define all operators here

get_concat_reds_feeds = OpTask(
    op=PythonOperator(
        task_id='get_concat_reds_feeds',
        python_callable=_get_concat_reds_feeds,
        provide_context=True,
        dag=adag,
    )
)

validate_concat_reds_feeds_files = OpTask(
    op=PythonOperator(
        task_id='validate_concat_reds_feeds_files',
        python_callable=_validate_concat_reds_feeds_files,
        provide_context=True,
        dag=adag,
    )
)

dag >> get_concat_reds_feeds >> validate_concat_reds_feeds_files
