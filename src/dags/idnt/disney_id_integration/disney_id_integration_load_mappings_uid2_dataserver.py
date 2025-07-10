from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 */3 * * *"  # catchup schedule is "0 1/3 * * *", do this before it
    retry_delay = timedelta(minutes=10)
    max_look_back_window = timedelta(hours=6)
else:
    schedule_interval = "*/15 * * * *"
    retry_delay = timedelta(minutes=5)
    max_look_back_window = timedelta(hours=1)

start_date = datetime(2024, 5, 1)
load_mappings_uid2_dataserver_task_id = "load-mappings-uid2-dataserver"
logs_dir = Utils.get_uid2_audience_logs_dir()
collected_dir = Utils.get_job_dir("collect-load-mappings-uid2-dataserver")


# macros
def get_target_date(data_interval_end) -> str:
    return datetime(data_interval_end.year, data_interval_end.month, 1).strftime(Utils.DATE_FORMAT)


def get_logs_begin_timestamp(dag_run, data_interval_end) -> str:
    return Utils.get_load_mappings_files_begin_timestamp(
        dag_run, load_mappings_uid2_dataserver_task_id, data_interval_end - max_look_back_window
    ).strftime(Utils.TIMESTAMP_FORMAT)


def get_logs_end_timestamp(data_interval_end) -> str:
    return (data_interval_end - timedelta(minutes=2)).strftime(Utils.TIMESTAMP_FORMAT)


load_mappings_uid2_dataserver_dag = Utils.create_dag(
    dag_id="disney-id-integration-load-mappings-uid2-dataserver",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = load_mappings_uid2_dataserver_dag.airflow_dag

dag.user_defined_macros.update({
    "get_target_date": get_target_date,
    "get_logs_begin_timestamp": get_logs_begin_timestamp,
    "get_logs_end_timestamp": get_logs_end_timestamp,
})

load_mappings_uid2_dataserver_task = Utils.create_pod_operator(
    dag=dag,
    name="load-mappings-uid2-dataserver",
    task_id=load_mappings_uid2_dataserver_task_id,
    sumologic_source_category="disneyidintegration-load-mappings-uid2-dataserver",
    prometheus_job="disneyidintegration-load-mappings-uid2-dataserver",
    execution_timeout=timedelta(hours=4),
    arguments=[
        "load-mappings-uid2-dataserver",
        "--target-date",
        "{{ get_target_date(data_interval_end) }}",
        "--logs",
        logs_dir,
        "--after",
        "{{ get_logs_begin_timestamp(dag_run, data_interval_end) }}",
        "--before",
        "{{ get_logs_end_timestamp(data_interval_end) }}",
        "--collect-output",
        collected_dir,
    ],
)

import_task = Utils.create_pod_operator(
    dag=dag,
    name="import",
    task_id="import",
    sumologic_source_category="disneyidintegration-import-load-mappings-uid2-dataserver",
    prometheus_job="disneyidintegration-import-load-mappings-uid2-dataserver",
    execution_timeout=timedelta(minutes=60),
    arguments=[
        "import", "--target-date", "{{ get_target_date(data_interval_end) }}", "--input", collected_dir, "--hash", "true",
        "--update-cumulative-imported-count-metrics", "true"
    ],
)

load_mappings_uid2_dataserver_dag >> load_mappings_uid2_dataserver_task >> import_task
