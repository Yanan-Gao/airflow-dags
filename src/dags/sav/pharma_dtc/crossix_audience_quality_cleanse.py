from ttd.kubernetes.pod_resources import PodResources
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdslack import dag_post_to_slack_callback
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.operators.python import PythonOperator
from datetime import date, datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.slack.slack_groups import sav

DAG_NAME = "pharma-dtc-data-ingestion-crossix"
DAG_START_TIME = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
DAG_RETRY_INTERVAL = 0
DAG_SCHEDULE_INTERVAL = timedelta(days=1)
SERVICE_ACCOUNT_NAME = "pharma-dtc"
LOGWORKFLOW_CONN = "lwdb"
LOGWORKFLOW_SANDBOX_CONN = "sandbox-lwdb"
GATING_TYPE_ID = 2000272  # select * from dbo.GatingType where GatingTypeName = 'ImportCrossixAudienceQuality'
GRAIN_ID = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    K8S_DEPLOYMENT_ENV = "sav-dev"
    K8S_ENV_VARS = {
        "DEBUG": "false",
        "ENVIRONMENT": "crossix_sandbox",
        "INPUT_BUCKET": "ttd-sav-sandbox",
        "INPUT_PREFIX": "crossix/v=1/",
        "OUTPUT_BUCKET": "ttd-sav-sandbox",
        "PHARMA_DTC_DATA_VENDOR": "crossix",
        "OUTPUT_KEY": 'crossix_cleansed/date={{ (logical_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}',
        "ARCHIVE_PREFIX": "crossix_archive",
    }

else:
    K8S_DEPLOYMENT_ENV = "sav"
    K8S_ENV_VARS = {
        "DEBUG": "false",
        "ENVIRONMENT": "crossix_prod",
        "INPUT_BUCKET": "ttd-sav",
        "INPUT_PREFIX": "crossix/v=1/",
        "OUTPUT_BUCKET": "ttd-sav",
        "PHARMA_DTC_DATA_VENDOR": "crossix",
        "OUTPUT_KEY": 'crossix_cleansed/date={{ (logical_date - macros.timedelta(days=1)).strftime("%Y%m%d") }}',
        "ARCHIVE_PREFIX": "crossix_archive",
    }

dag = TtdDag(
    dag_id=DAG_NAME,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": DAG_START_TIME,
        "email": ["river.he@thetradedesk.com"],
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
    on_failure_callback=dag_post_to_slack_callback(dag_name=DAG_NAME, step_name="parent dagrun", slack_channel="#scrum-sav-alerts"),
)

airflow_dag = dag.airflow_dag

container_resource = PodResources(request_cpu="1000m", request_memory="1Gi", limit_memory="3Gi", limit_ephemeral_storage="5Gi")

pharma_dtc_data_ingestion_crossix = TtdKubernetesPodOperator(
    namespace=K8S_DEPLOYMENT_ENV,
    image="docker.pkgs.adsrvr.org/apps-prod/ttd/pharma-dtc-data-ingestion:latest",
    image_pull_policy="Always",
    name="pharma_dtc_data_ingestion_crossix",
    task_id="pharma_dtc_data_ingestion_crossix",
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

pharma_dtc_data_ingestion_crossix >> open_lwdb_gate
