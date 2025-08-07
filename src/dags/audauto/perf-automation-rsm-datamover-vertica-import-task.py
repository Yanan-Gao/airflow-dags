from datetime import datetime, timedelta
from pendulum import DateTime

from airflow.operators.python import PythonOperator

from ttd.tasks.op import OpTask
from ttd.eldorado.base import TtdDag
from ttd.cloud_provider import CloudProviders
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.datasets.date_generated_dataset import DateGeneratedDataset

# Job config
DAG_NAME = "perf-automation-rsm-datamover-vertica-import-task"
DAG_START_TIME = datetime(2024, 11, 18)
DAG_RETRY_INTERVAL = timedelta(minutes=5)
DAG_RETRY_EXPONENTIAL_BACKOFF = True
DAG_SCHEDULE_INTERVAL = timedelta(days=1)
DAG_CONCURRENT_EXECUTIONS = 1
DAG_CATCH_UP = False
SLACK_CHANNEL = "#perf-auto-audiences-alerts"

LOGWORKFLOW_CONN = "lwdb"

# Gate config
GATING_TYPE_ID = 2000510  # select GatingTypeId from Task where TaskId = dbo.fn_enum_Task_ImportRSMSeedSegmentScoreDaily()
GRAIN_ID = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()


def get_lwdb_date_string(data_interval_end: DateTime) -> str:
    return data_interval_end.start_of("day").strftime("%Y-%m-%d %H:%M:%S")


rsm_datamover_dag = TtdDag(
    dag_id=DAG_NAME,
    start_date=DAG_START_TIME,
    schedule_interval=timedelta(days=1),
    max_active_runs=DAG_CONCURRENT_EXECUTIONS,
    retries=1,
    depends_on_past=False,
    retry_delay=DAG_RETRY_INTERVAL,
    tags=["AUDAUTO"],
    slack_channel=SLACK_CHANNEL,
    default_args={"owner": "AUDAUTO"},
)
dag = rsm_datamover_dag.airflow_dag

rsm_seed_segment_score_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prodTest",
    data_name="audience/RSM/Seed_Relevance/AggKey=TargetingDataId/v=1/Top10KPerSeed",
    version=None,
    env_aware=False,
    success_file="_SUCCESS"
)

check_segment_score_dataset = OpTask(
    op=DatasetCheckSensor(
        task_id="check_segment_score_dataset",
        datasets=[rsm_seed_segment_score_dataset],
        ds_date="{{data_interval_end.to_datetime_string()}}",
        poke_interval=60 * 10,  # in seconds
        timeout=60 * 60 * 12,  # wait up to 12 hours
        cloud_provider=CloudProviders.aws,
    )
)


def _open_lwdb_gate(**context):
    data_interval_end: DateTime = context["data_interval_end"]
    _ = ExternalGateOpen(
        mssql_conn_id=LOGWORKFLOW_CONN,
        sproc_arguments={
            "gatingType": GATING_TYPE_ID,
            "grain": GRAIN_ID,
            "dateTimeToOpen": get_lwdb_date_string(data_interval_end),
        },
    )


open_lwdb_gate = OpTask(op=PythonOperator(
    task_id="open_lwdb_gate",
    python_callable=_open_lwdb_gate,
    provide_context=True,
    dag=dag,
))

(rsm_datamover_dag >> check_segment_score_dataset >> open_lwdb_gate)
