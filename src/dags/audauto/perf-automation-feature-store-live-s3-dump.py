from datetime import timedelta, datetime, timezone
import time

from airflow.operators.python import PythonOperator

from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from ttd.ttdenv import TtdEnvFactory

from dags.audauto.utils import utils

# Determine environment and db
env_path = 'prod' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'test'
db_read = "audauto_feature"
db_write = 'audauto_feature' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'audauto_feature_test'
s3_root = 's3://thetradedesk-mlplatform-us-east-1/features/feature_store'
# Dataset configuration
DATASETS = [
    {
        "name": "clicktracker",
        "json_table": "clicktracker_online_json",
        "parquet_table": "clicktracker_online_parquet",
        "s3_base_read": f"{s3_root}/prod/online/clicktracker",
        "s3_base_write": f"{s3_root}/{env_path}/online/clicktracker",
        "columns": " tdid, timestamp, dc, site, adGroupId, clickStatus, date, hour ",
    },
    {
        "name":
        "conversiontracker",
        "json_table":
        "conversiontracker_online_json",
        "parquet_table":
        "conversiontracker_online_parquet",
        "s3_base_read":
        f"{s3_root}/prod/online/conversiontracker",
        "s3_base_write":
        f"{s3_root}/{env_path}/online/conversiontracker",
        "columns":
        " tdid, tdidtype, advertiserid, trackingtagid, referralurl, timestamp, dc, ipaddress, country, region, city, zip, date, hour ",
    },
    {
        "name":
        "eventtracker",
        "json_table":
        "eventtracker_online_json",
        "parquet_table":
        "eventtracker_online_parquet",
        "s3_base_read":
        f"{s3_root}/prod/online/eventtracker",
        "s3_base_write":
        f"{s3_root}/{env_path}/online/eventtracker",
        "columns":
        " tdid, tdidtype, advertiserid, trackingtagid, referralurl, timestamp, dc, ipaddress, country, region, city, zip, date, hour ",
    },
]


# Helper functions
def get_s3_paths(dataset):
    base_read = dataset["s3_base_read"]
    json_s3 = f'{base_read}/json/date={{{{ ds_nodash }}}}/hour={{{{ logical_date.strftime("%H") }}}}/'
    base_write = dataset["s3_base_write"]
    parquet_s3 = f'{base_write}/parquet/date={{{{ ds_nodash }}}}/hour={{{{ logical_date.strftime("%H") }}}}/'
    return json_s3, parquet_s3


def get_register_query(dataset):
    return f"""alter table {db_read}.{dataset['json_table']} add IF NOT EXISTS partition (date='{{{{ ds_nodash }}}}', hour='{{{{ logical_date.strftime(\"%H\") }}}}'); """


def get_parquet_query(dataset):
    return f""" insert into {db_write}.{dataset['parquet_table']} select {dataset['columns']} from {db_read}.{dataset['json_table']} where date='{{{{ ds_nodash }}}}' and hour='{{{{ logical_date.strftime(\"%H\") }}}}'; """


# Generic S3 wait function
def wait_s3_prefix_until_inactive(**kwargs):
    s3path = kwargs['s3path']
    inactive_threshold_in_minutes = kwargs['inactive_threshold_in_minutes']
    poke_interval_in_second = kwargs['poke_interval_in_second']
    timeout_in_minutes = kwargs['timeout_in_minutes']
    start = datetime.now(timezone.utc)
    end = start + timedelta(minutes=timeout_in_minutes)
    last_modified = None
    print(f"checking if folder is completed or not: {s3path}")
    while (datetime.now(timezone.utc) < end):
        last_modified = utils.latest_file_timestamp_in_s3_prefix(s3path)
        if last_modified is not None:
            age_in_minutes = (datetime.now(timezone.utc) - last_modified).total_seconds() / 60
            if (age_in_minutes > inactive_threshold_in_minutes):
                return
            else:
                print(
                    f"lastest file timestamp {last_modified}, age in minutes {age_in_minutes}, require age:{inactive_threshold_in_minutes}, will retry"
                )
        else:
            print(f"No file found under {s3path}, will retry")
        time.sleep(poke_interval_in_second)
    raise Exception(f'Timed out waiting for {s3path} to be ready, found latest file timestamp is {last_modified}')


def delete_s3_objects(**kwargs):
    s3_path = kwargs['s3_path']
    utils.delete_s3_objects_with_prefix(s3_path=s3_path)


# DAG definition
start_date = datetime(2025, 6, 30, 0, 0)
ttd_dag = TtdDag(
    dag_id="perf-automation-feature-store-live-s3-dump",
    start_date=start_date,
    schedule_interval='50 * * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#dev-perf-auto-alerts-cpa-roas",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO", "FEATURE_STORE"]
)
adag = ttd_dag.airflow_dag

# Dynamically create tasks for each dataset
tasks = {}
for dataset in DATASETS:
    json_s3, parquet_s3 = get_s3_paths(dataset)
    # Wait for JSON partition
    wait_json = OpTask(
        op=PythonOperator(
            task_id=f'wait_{dataset["name"]}_json_partition',
            python_callable=wait_s3_prefix_until_inactive,
            op_kwargs={
                's3path': json_s3,
                'inactive_threshold_in_minutes': 60,
                'poke_interval_in_second': 180,
                'timeout_in_minutes': 180
            },
            provide_context=True,
            dag=adag,
        )
    )
    # Register JSON partition
    register_json = OpTask(
        op=AWSAthenaOperator(
            task_id=f'register_{dataset["name"]}_json_partition',
            output_location='s3://thetradedesk-useast-data-import/temp/athena-output',
            query=get_register_query(dataset),
            retries=3,
            sleep_time=60,
            aws_conn_id='aws_default',
            database=db_read
        )
    )
    # Clean up Parquet
    clean_up_parquet = OpTask(
        op=PythonOperator(
            task_id=f'clean_up_{dataset["name"]}_parquet',
            provide_context=True,
            python_callable=delete_s3_objects,
            op_kwargs={'s3_path': parquet_s3},
            dag=adag,
        )
    )
    # Generate Parquet
    gen_parquet = OpTask(
        op=AWSAthenaOperator(
            task_id=f'gen_{dataset["name"]}_parquet',
            output_location='s3://thetradedesk-useast-data-import/temp/athena-output',
            query=get_parquet_query(dataset),
            retries=3,
            sleep_time=60,
            aws_conn_id='aws_default',
            database=db_write
        )
    )
    tasks[dataset["name"]] = {
        "wait_json": wait_json,
        "register_json": register_json,
        "clean_up_parquet": clean_up_parquet,
        "gen_parquet": gen_parquet,
    }

for task in tasks.values():
    ttd_dag >> task["wait_json"] >> task["register_json"] >> task["clean_up_parquet"] >> task["gen_parquet"]
