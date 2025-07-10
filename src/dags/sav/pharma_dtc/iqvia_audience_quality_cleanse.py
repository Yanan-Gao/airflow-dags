import urllib.parse

from dags.datasrvc.materialized_gating.materialized_gating_sensor import MaterializedGatingSensor
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdslack import dag_post_to_slack_callback
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.slack.slack_groups import sav
from ttd.kubernetes.pod_resources import PodResources

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdslack import get_slack_client

DAG_NAME = "pharma-dtc-data-ingestion-iqvia"
DAG_START_TIME = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
DAG_RETRY_INTERVAL = 0
DAG_SCHEDULE_INTERVAL = timedelta(days=1)
SERVICE_ACCOUNT_NAME = "pharma-dtc"
LOGWORKFLOW_CONN = "lwdb"
LOGWORKFLOW_SANDBOX_CONN = "sandbox-lwdb"
GATING_TYPE_ID = 2000224
GRAIN_ID = 100002
ALARM_SLACK_CHANNEL = "#scrum-sav-alerts"
SAV_BUCKET = "ttd-sav"
SAV_BUCKET_SANDBOX = "ttd-sav-sandbox"

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    K8S_DEPLOYMENT_ENV = "sav-dev"
    K8S_ENV_VARS = {
        "DEBUG": "false",
        "ENVIRONMENT": "iqvia_sandbox",
        "INPUT_BUCKET": SAV_BUCKET_SANDBOX,
        "INPUT_PREFIX": "iqvia/v=1/",
        "OUTPUT_BUCKET": SAV_BUCKET_SANDBOX,
        "PHARMA_DTC_DATA_VENDOR": "iqvia",
        "OUTPUT_KEY": 'iqvia_cleansed/date={{ (logical_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}',
        "ARCHIVE_PREFIX": "iqvia_archive",
        "FAILED_CLEANSE_OUTPUT_KEY": "iqvia_failed_cleanse/date={{ prev_ds_nodash }}",
    }

else:
    K8S_DEPLOYMENT_ENV = "sav"
    K8S_ENV_VARS = {
        "DEBUG": "false",
        "ENVIRONMENT": "iqvia_prod",
        "INPUT_BUCKET": SAV_BUCKET,
        "INPUT_PREFIX": "iqvia/v=1/",
        "OUTPUT_BUCKET": SAV_BUCKET,
        "PHARMA_DTC_DATA_VENDOR": "iqvia",
        "OUTPUT_KEY": 'iqvia_cleansed/date={{ (logical_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}',
        "ARCHIVE_PREFIX": "iqvia_archive",
        "FAILED_CLEANSE_OUTPUT_KEY": "iqvia_failed_cleanse/date={{ prev_ds_nodash }}",
    }

dag = TtdDag(
    dag_id=DAG_NAME,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": DAG_START_TIME,
        "email": ["alex.fung@thetradedesk.com"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 2,
        "retry_delay": DAG_RETRY_INTERVAL,
        "provide_context": True,
    },
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    max_active_runs=1,
    run_only_latest=True,
    tags=[sav.jira_team],
    on_failure_callback=dag_post_to_slack_callback(dag_name=DAG_NAME, step_name="parent dagrun", slack_channel=ALARM_SLACK_CHANNEL),
)

airflow_dag = dag.airflow_dag

container_resource = PodResources(request_cpu="1000m", request_memory="1Gi", limit_memory="3Gi", limit_ephemeral_storage="5Gi")

pharma_dtc_data_ingestion_iqvia = TtdKubernetesPodOperator(
    namespace=K8S_DEPLOYMENT_ENV,
    image="docker.pkgs.adsrvr.org/apps-prod/ttd/pharma-dtc-data-ingestion:latest",
    image_pull_policy="Always",
    name="pharma_dtc_data_ingestion_iqvia",
    task_id="pharma_dtc_data_ingestion_iqvia",
    dnspolicy="ClusterFirst",
    get_logs=True,
    is_delete_operator_pod=True,
    dag=airflow_dag,
    startup_timeout_seconds=500,
    execution_timeout=timedelta(hours=1),
    log_events_on_failure=True,
    service_account_name=SERVICE_ACCOUNT_NAME,
    resources=container_resource,
    env_vars=K8S_ENV_VARS,
)

wait_for_performance_report = MaterializedGatingSensor(task_id="wait_for_performance_report", dag=airflow_dag)
for vertica_cluster in ("VerticaUSEast01", "VerticaUSWest01"):
    wait_for_performance_report.add_dependency(
        TaskId="dbo.fn_Enum_Task_VerticaMergeIntoPerformanceReport()",
        LogTypeId="dbo.fn_Enum_LogType_VerticaHourlyProcessing()",
        GrainId="dbo.fn_enum_TaskBatchGrain_Hourly()",
        TaskVariantId=f"dbo.fn_enum_TaskVariant_{vertica_cluster}()",
    )


def _open_lwdb_gate(**context):
    task_date: datetime = context["logical_date"]
    dt = task_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    log_start_time = dt.strftime("%Y-%m-%d %H:00:00")
    _ = ExternalGateOpen(
        mssql_conn_id=LOGWORKFLOW_CONN if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else LOGWORKFLOW_SANDBOX_CONN,
        sproc_arguments={
            "gatingType": GATING_TYPE_ID,
            "grain": GRAIN_ID,
            "dateTimeToOpen": log_start_time,
        },
    )


open_lwdb_gate = PythonOperator(
    task_id="open_lwdb_gate",
    python_callable=_open_lwdb_gate,
    provide_context=True,
    dag=airflow_dag,
)


def _check_failed_cleanse_file(**context):
    env = TtdEnvFactory.get_from_system()
    data_interval_start = context["data_interval_start"]
    yesterday_ds_nodash = context["yesterday_ds_nodash"]
    prefix = f'iqvia_failed_cleanse/date={yesterday_ds_nodash}'
    bucket_name = SAV_BUCKET if env == TtdEnvFactory.prod else SAV_BUCKET_SANDBOX
    print(f'Checking for failed cleanse file from the bucket:{bucket_name}, prefix:{prefix}')

    aws_cloud_storage = AwsCloudStorage(conn_id='aws_default')
    keys = aws_cloud_storage.list_keys(prefix, bucket_name)

    if keys is None or len(keys) == 0:
        print("No failed cleanse file detected.")
        return

    base_url = context["var"]["value"].get("BASE_URL")
    dag_id = context["dag"].dag_id
    formatted_dag_run_id = urllib.parse.quote_plus(context["run_id"])

    init_text = f""":airflow-tire-fire: Airflow DAG `{dag_id}` *failed cleanse file detected* in `{env.execution_env}`! \n\nDetails in :thread:"""
    init_message = get_slack_client().chat_postMessage(channel=ALARM_SLACK_CHANNEL, text=init_text)

    thread_id = init_message["message"]["ts"]
    thread_text = f"""*Execution date:* {data_interval_start.to_datetime_string()}\n\n*Environment:* `{env.execution_env}`\n\n\n"""

    thread_text += "*Failed cleanse files:*\n"
    thread_text += f"Bucket name: {bucket_name}\n"
    thread_text += f"Prefix: {prefix}\n\n\n"

    thread_text += f"\n<{base_url}/dags/{dag_id}/grid?dag_run_id={formatted_dag_run_id}|*Grid view*>\n\n"

    get_slack_client().chat_postMessage(channel=ALARM_SLACK_CHANNEL, text=thread_text, thread_ts=thread_id)


check_failed_cleanse_file = PythonOperator(
    task_id="check_failed_cleanse_file",
    python_callable=_check_failed_cleanse_file,
    dag=airflow_dag,
    provide_context=True,
)

pharma_dtc_data_ingestion_iqvia >> wait_for_performance_report >> open_lwdb_gate >> check_failed_cleanse_file
