from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 1/3 * * *"
    retry_delay = timedelta(minutes=30)
    max_look_back_window = timedelta(hours=6)
else:
    schedule_interval = "*/15 * * * *"
    retry_delay = timedelta(minutes=10)
    max_look_back_window = timedelta(hours=1)

start_date = datetime(2024, 5, 1)
collect_task_id = "collect"
collected_dir = Utils.get_job_dir("collect-catchup")
logs_dir = Utils.get_nebula_conversion_logs_dir()


# macros
def get_target_date(data_interval_end) -> str:
    return data_interval_end.strftime("%Y-%m-01")


def get_logs_begin_timestamp(dag_run, data_interval_end) -> str:
    return Utils.get_collect_files_begin_timestamp(dag_run, collect_task_id,
                                                   data_interval_end - max_look_back_window).strftime(Utils.TIMESTAMP_FORMAT)


def get_logs_end_timestamp(data_interval_end) -> str:
    return (data_interval_end - timedelta(minutes=2)).strftime(Utils.TIMESTAMP_FORMAT)


catchup_dag = Utils.create_dag(
    dag_id="disney-id-integration-catchup",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = catchup_dag.airflow_dag

dag.user_defined_macros.update({
    "get_target_date": get_target_date,
    "get_logs_begin_timestamp": get_logs_begin_timestamp,
    "get_logs_end_timestamp": get_logs_end_timestamp,
})

collect_task = Utils.create_pod_operator(
    dag=dag,
    name="collect",
    task_id=collect_task_id,
    sumologic_source_category="disneyidintegration-collect-catchup",
    prometheus_job="disneyidintegration-collect-catchup",
    execution_timeout=timedelta(minutes=60),
    arguments=[
        "collect",
        "catchup",
        "--logs",
        logs_dir,
        "--after",
        "{{ get_logs_begin_timestamp(dag_run, data_interval_end) }}",
        "--before",
        "{{ get_logs_end_timestamp(data_interval_end) }}",
        "--target",
        "{{ get_target_date(data_interval_end) }}",
        "--output",
        collected_dir,
    ],
)

import_task = Utils.create_pod_operator(
    dag=dag,
    name="import",
    task_id="import",
    sumologic_source_category="disneyidintegration-import-catchup",
    prometheus_job="disneyidintegration-import-catchup",
    execution_timeout=timedelta(minutes=60),
    arguments=[
        "import", "--target-date", "{{ get_target_date(data_interval_end) }}", "--input", collected_dir, "--hash", "true",
        "--update-cumulative-imported-count-metrics", "true"
    ],
)

catchup_dag >> collect_task >> import_task
