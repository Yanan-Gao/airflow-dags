"""
Update list of proxy IPs from BrightData to https://ttd-content.adsrvr.org/ips (S3 Bucket)
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator

from ttd.slack.slack_groups import CTXMP
from ttd.ttdslack import dag_post_to_slack_callback
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

# Configs
pipeline_name = 'brightdata-ips-update'
ttdcontent_bucket = 'ttd-content-adsrvr-org-s3bucketroot-3v5ucx3irdni'
ttdcontent_file_key = 'ips'
brightdata_http_connection_id = 'brightdata'
brightdata_token_variable_id = 'brightdata_token_secret'
job_start_date = datetime(year=2021, month=7, day=22, hour=3, minute=0)
job_schedule_interval = timedelta(hours=8)
retry_delay = timedelta(minutes=20)
run_timeout = timedelta(hours=3)
retries = 5
max_active_runs = 1
encoding = "utf-8"
content_type = f"text/plain; charset={encoding}"
cache_control_seconds = 1800
cache_control = f"max-age={cache_control_seconds}"
team = CTXMP.team

# DAG
dag = DAG(
    pipeline_name,
    default_args={
        'owner': team.jira_team,
        'depends_on_past': False,
        'start_date': job_start_date,
        'retries': retries,
        'retry_delay': retry_delay,
        # Do not use email
        'email': [],
        'email_on_failure': None,
        'email_on_retry': False,
    },
    schedule_interval=job_schedule_interval,
    max_active_runs=max_active_runs,
    catchup=False,
    dagrun_timeout=run_timeout,
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=team.name, step_name=team.name, slack_channel=team.alarm_channel, message="Bright Data ips update has failed"
    ),
    tags=[team.name]
)


def check(response_code):
    if response_code == 200:
        print("Returning True (200)")
        return True
    else:
        print(f"Returning False (not 200 its {response_code}")
        return False


get_current_ips = HttpOperator(
    task_id="get_current_ips",
    dag=dag,
    http_conn_id=brightdata_http_connection_id,
    method="GET",
    endpoint="api/zone/route_ips",
    headers={"Authorization": f"Bearer {{{{ var.value.{brightdata_token_variable_id} }}}}"},
    data={
        "expand": 1,
        "customer": "c_90fbf91a",
        "zone": "production2"
    },
    do_xcom_push=True,
    log_response=True,
    response_check=lambda response: check(response.status_code)
)


# Same as S3Hook#load_string but need to add some specific Metadata to the file.
# Latest versions of Airflow seem to allow this, so can change this to use load_string
# directly in future
def load_string_to_s3(string_data, key, bucket_name):
    bytes_data = string_data.encode(encoding)

    # this is why doing this manually as currently not able to supply our own extra-args metadata in the hook
    # we want to specify the CacheControl ourselves, as we are using CloudFront as a cache for this bucket
    # it is configured with minimumTtl of 0 and default of 1 day. Therefore if we don't provide our own
    # CacheControl value for the file, then the cached version from CloudFrount will be returned for 1 day.
    # That's too long, so instead specifying a much smaller lookup, so can recheck sooner for any updates.
    # We also specify ContentType so can be rendered properly in a browser, as otherwise S3 assigns default of
    # octet-stream type
    extra_args = {"ContentType": content_type, "CacheControl": cache_control}

    s3 = AwsCloudStorage(conn_id='aws_default', extra_args=extra_args)
    s3.load_string(string_data, key, bucket_name, replace=True)


# Note that limit of 20-30kb data can be passed between tasks using XCom (Cross Communication).
# For future tasks that might be bigger, likely need a single operator (or similar) that uses http_hook and s3_hooks
# directly (or for other tasks, store the data externally elsewhere in between tasks)
def copy_ips_string_to_s3(**kwargs):
    ti = kwargs['ti']
    current_ips = ti.xcom_pull(key=None, task_ids='get_current_ips')
    print(f"retrieved ips to copy to s3={current_ips}")
    load_string_to_s3(string_data=current_ips, bucket_name=ttdcontent_bucket, key=ttdcontent_file_key)


copy_ips_to_s3 = PythonOperator(
    task_id='copy_ips_to_s3',
    dag=dag,
    python_callable=copy_ips_string_to_s3,
    provide_context=True,
)

get_current_ips >> copy_ips_to_s3
