from datetime import datetime, timedelta
from pendulum import DateTime

from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.cncf.kubernetes.secret import Secret

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.ttdenv import TtdEnvFactory
from ttd.kubernetes.pod_resources import PodResources
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.eldorado.base import TtdDag

# Job config
DAG_NAME = "viewability-data-ingestion-integral"
DAG_START_TIME = datetime(2024, 10, 18)
DAG_RETRY_INTERVAL = timedelta(minutes=5)
DAG_RETRY_EXPONENTIAL_BACKOFF = True
DAG_SCHEDULE_INTERVAL = "0 * * * *"
DAG_CONCURRENT_EXECUTIONS = 8

SERVICE_ACCOUNT_NAME = "viewability"
K8S_ENV_VARS = {
    "INTEGRAL_DATA_RECEIVED_HOUR_UTC": "{{ get_current_time_string() }}",
    "INTEGRAL_TARGET_DATE": "{{ get_app_date_string(data_interval_end) }}",
    "INTEGRAL_REPORT_HOUR_UTC": "{{ get_utc_date_string(data_interval_end) }}",
}
PROMETHEUS_PUSH_GATEWAY = "prom-push-gateway.adsrvr.org:80"

INTEGRAL_OUTPUT_BUCKET_NAME = "ttd-viewability-data-ingestion"
DATA_LAG_HOURS = 0
IAS_TIMEZONE_STR = "US/Eastern"

# Gate config
GATING_TYPE_ID_TTD = 2000480  # TTD tenant data mover job
GATING_TYPE_ID_WALMART = 2000561  # Walmart tenant data mover job

GRAIN_ID = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()


# Macros
def get_utc_timezone_delayed_date(data_interval_end: DateTime) -> DateTime:
    """Round down to closest hour and subtract by number of data lag hours"""
    return data_interval_end.start_of("hour").subtract(hours=DATA_LAG_HOURS)


def get_ias_timezone_delayed_date(data_interval_end: DateTime) -> DateTime:
    """Set to alternate timezone since IAS uses east coast timezone in the log file and the sftp file name"""
    utc_lag_time = get_utc_timezone_delayed_date(data_interval_end)
    ias_lag_time = utc_lag_time.in_tz(IAS_TIMEZONE_STR)
    return ias_lag_time


def get_utc_date_string(data_interval_end: DateTime) -> str:
    return get_utc_timezone_delayed_date(data_interval_end).strftime("%Y-%m-%d %H:00")


def get_app_date_string(data_interval_end: DateTime) -> str:
    """sftp uses non utc timezone and requires YY-mm-DD HH:00 as input config"""
    ias_lag_time = get_ias_timezone_delayed_date(data_interval_end)
    return ias_lag_time.strftime("%Y-%m-%d %H:00")


def get_app_s3_dir_string(data_interval_end: DateTime) -> str:
    utc_lag_time = get_utc_timezone_delayed_date(data_interval_end)
    s3_date_str = utc_lag_time.format("YYYYMMDD")
    s3_date_hour_str = utc_lag_time.format("HH")
    return f"date={s3_date_str}/hour={s3_date_hour_str}"


def get_lwdb_date_string(data_interval_end: DateTime) -> str:
    utc_lag_time = get_utc_timezone_delayed_date(data_interval_end)
    return utc_lag_time.strftime("%Y-%m-%d %H:00:00")


def get_sftp_file_string(data_interval_end: DateTime) -> str:
    ias_lag_time = get_ias_timezone_delayed_date(data_interval_end)
    time_bit: str = ias_lag_time.strftime("%Y%m%d_%H")
    return f"outbound/2064289/2064289_{time_bit}_fastfeedback.dat.gz"


def get_current_time_string() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:00")


def _open_lwdb_gate_ttd(**context):
    data_interval_end: DateTime = context["data_interval_end"]
    _ = ExternalGateOpen(
        mssql_conn_id=LOGWORKFLOW_CONN,
        sproc_arguments={
            "gatingType": GATING_TYPE_ID_TTD,
            "grain": GRAIN_ID,
            "dateTimeToOpen": get_lwdb_date_string(data_interval_end),
        },
    )


def _open_lwdb_gate_walmart(**context):
    data_interval_end: DateTime = context["data_interval_end"]
    _ = ExternalGateOpen(
        mssql_conn_id=LOGWORKFLOW_CONN,
        sproc_arguments={
            "gatingType": GATING_TYPE_ID_WALMART,
            "grain": GRAIN_ID,
            "dateTimeToOpen": get_lwdb_date_string(data_interval_end),
        },
    )


if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
    GIT_BRANCH = "latest"
    DATA_INGESTION_SPARK_IMAGE = (f"production.docker.adsrvr.org/ttd-measurement/viewability-data-ingestion:{GIT_BRANCH}")
    # DATA_INGESTION_SPARK_IMAGE = "production.docker.adsrvr.org/ttd-measurement/viewability-data-ingestion:latest"
    K8S_DEPLOYMENT_ENV = "viewability-data-ingestion-dev"
    K8S_CONN_ID = "airflow-2-pod-scheduling-rbac-conn-viewability-non-prod"
    LOGWORKFLOW_CONN = "lwdb-sandbox"
    SLACK_CHANNEL = None
    DAG_CATCH_UP = False
    K8S_ENV_VARS = {
        **K8S_ENV_VARS,
        **{
            "RUNTIME_ENVIRONMENT": "prodtest",
            "INTEGRAL_UPLOAD_DIRECTORY_S3": f"s3a://{INTEGRAL_OUTPUT_BUCKET_NAME}/test/cleansed/v=2/" + "tenant={Tenant}/" + "{{ get_app_s3_dir_string(data_interval_end) }}",
            "INTEGRAL_IGNORE_ADDITION_COLUMNS": "true",
            "INTEGRAL_REMOVE_IP_ADDR_COLUMN": "true",
            "INTEGRAL_PARTITION_BY_TENANT": "true",
        },
    }
    CONTAINER_RESOURCES = PodResources.from_dict(
        request_ephemeral_storage="25G",
        limit_ephemeral_storage="100G",
        request_memory="16G",
        limit_memory="32G",
        request_cpu="4000m",
        limit_cpu="4000m",
    )
else:
    DATA_INGESTION_SPARK_IMAGE = ("production.docker.adsrvr.org/ttd-measurement/viewability-data-ingestion:latest")
    K8S_DEPLOYMENT_ENV = "viewability-data-ingestion-prod"
    K8S_CONN_ID = "airflow-2-pod-scheduling-rbac-conn-viewability-prod"
    LOGWORKFLOW_CONN = "lwdb"
    SLACK_CHANNEL = "#scrum-measurement-up-alarms"
    DAG_CATCH_UP = True
    K8S_ENV_VARS = {
        **K8S_ENV_VARS,
        **{
            "RUNTIME_ENVIRONMENT": "prod",
            "INTEGRAL_UPLOAD_DIRECTORY_S3": f"s3a://{INTEGRAL_OUTPUT_BUCKET_NAME}/prod/cleansed/v=2/" + "tenant={Tenant}/" + "{{ get_app_s3_dir_string(data_interval_end) }}",
            "INTEGRAL_IGNORE_ADDITION_COLUMNS": "true",
            "INTEGRAL_REMOVE_IP_ADDR_COLUMN": "true",
            "INTEGRAL_PARTITION_BY_TENANT": "true",
        },
    }
    CONTAINER_RESOURCES = PodResources.from_dict(
        request_ephemeral_storage="25G",
        limit_ephemeral_storage="100G",
        request_memory="16G",
        limit_memory="32G",
        request_cpu="4000m",
        limit_cpu="4000m",
    )

viewability_data_ingestion_dag = TtdDag(
    dag_id=DAG_NAME,
    start_date=DAG_START_TIME,
    schedule_interval=DAG_SCHEDULE_INTERVAL,
    max_active_runs=DAG_CONCURRENT_EXECUTIONS,
    retries=4,
    depends_on_past=False,
    retry_delay=DAG_RETRY_INTERVAL,
    contact_email=True,
    default_args={
        "email": ["alex.fung@thetradedesk.com"],
        "email_on_failure": True,
        "email_on_retry": True,
        "retry_exponential_backoff": DAG_RETRY_EXPONENTIAL_BACKOFF,
    },
    tags=["measurement", "viewabiity-data-ingestion"],
    slack_channel=SLACK_CHANNEL,
    run_only_latest=(not DAG_CATCH_UP),
)

dag = viewability_data_ingestion_dag.airflow_dag

dag.user_defined_macros = {
    "get_app_date_string": get_app_date_string,
    "get_app_s3_dir_string": get_app_s3_dir_string,
    "get_utc_date_string": get_utc_date_string,
    "get_sftp_file_string": get_sftp_file_string,
    "get_current_time_string": get_current_time_string,
}

ingest_data_from_integral_sftp = TtdKubernetesPodOperator(
    namespace=K8S_DEPLOYMENT_ENV,
    kubernetes_conn_id=K8S_CONN_ID,
    image=DATA_INGESTION_SPARK_IMAGE,
    image_pull_policy="Always",
    name="viewability-data-ingestion-integral",
    task_id="viewability_data_ingestion_integral",
    dnspolicy="ClusterFirst",
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
    startup_timeout_seconds=500,
    execution_timeout=timedelta(hours=4),
    log_events_on_failure=True,
    service_account_name=SERVICE_ACCOUNT_NAME,
    annotations={
        "sumologic.com/include": "true",
        "sumologic.com/sourceCategory": "viewability-data-ingestion-integral",
    },
    secrets=[
        Secret(
            deploy_type="env",
            deploy_target="VERTICA_USERNAME",
            secret="viewability-integral",
            key="vertica_username",
        ),
        Secret(
            deploy_type="env",
            deploy_target="VERTICA_PASSWORD",
            secret="viewability-integral",
            key="vertica_password",
        ),
        Secret(
            deploy_type="env",
            deploy_target="INTEGRAL_SFTP_USERNAME",
            secret="viewability-integral",
            key="sftp_username",
        ),
        Secret(
            deploy_type="env",
            deploy_target="INTEGRAL_SFTP_PASSWORD",
            secret="viewability-integral",
            key="sftp_password",
        ),
    ],
    resources=CONTAINER_RESOURCES,
    env_vars=K8S_ENV_VARS,
    arguments=[
        "--conf",
        "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
        "--driver-memory",  # only driver memory matters in local mode as driver serves as executer
        "16G",
        "/viewability-data-ingestion-assembly-1.0.jar",
    ],
)

wait_for_sftp_file = SFTPSensor(
    task_id="wait_for_sftp_file",
    path="{{ get_sftp_file_string(data_interval_end) }}",
    sftp_conn_id="viewability_integral_sftp",
    poke_interval=60 * 15,
    mode="reschedule",
    timeout=60 * 60 * 24,
)

open_lwdb_gate_ttd = PythonOperator(
    task_id="open_lwdb_gate_ttd",
    python_callable=_open_lwdb_gate_ttd,
    provide_context=True,
    dag=dag,
)

wait_for_vertica_up = SqlSensor(
    dag=dag,
    task_id="wait_for_vertica_up",
    conn_id="vertica_prod",
    sql="SELECT COUNT(*) FROM provisioning2.ViewabilitySettings",
    poke_interval=60 * 5,  # 5 minutes
    timeout=60 * 60 * 4,  # 4 hours
    mode="reschedule"
)

open_lwdb_gate_walmart = PythonOperator(
    task_id="open_lwdb_gate_walmart",
    python_callable=_open_lwdb_gate_walmart,
    provide_context=True,
    dag=dag,
)

wait_for_sftp_file >> wait_for_vertica_up >> ingest_data_from_integral_sftp >> open_lwdb_gate_ttd
ingest_data_from_integral_sftp >> open_lwdb_gate_walmart
