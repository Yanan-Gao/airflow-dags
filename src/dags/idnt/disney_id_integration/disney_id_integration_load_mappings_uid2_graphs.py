from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 22 * * *"
    retry_delay = timedelta(minutes=30)
    graphs_max_look_back_window = timedelta(days=2)
else:
    schedule_interval = "*/15 * * * *"
    retry_delay = timedelta(minutes=10)
    graphs_max_look_back_window = timedelta(days=1)

start_date = datetime(2024, 5, 1)
graphs_dir = Utils.get_uid2_graphs_dir()

load_mappings_uid2_graphs_task_id = "load-mappings-uid2-graphs"

collected_dir_graphs = Utils.get_job_dir("collect-load-mappings-uid2-graphs")


# macros
def get_target_date(data_interval_end) -> str:
    return datetime(data_interval_end.year, data_interval_end.month, 1).strftime(Utils.DATE_FORMAT)


def get_graphs_begin_timestamp(dag_run, data_interval_end) -> str:
    return Utils.get_load_mappings_files_begin_timestamp(
        dag_run, load_mappings_uid2_graphs_task_id, data_interval_end - graphs_max_look_back_window
    ).strftime(Utils.TIMESTAMP_FORMAT)


def get_source_file_end_timestamp(data_interval_end) -> str:
    return (data_interval_end - timedelta(minutes=2)).strftime(Utils.TIMESTAMP_FORMAT)


load_mappings_uid2_graphs_dag = Utils.create_dag(
    dag_id="disney-id-integration-load-mappings-uid2-graphs",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = load_mappings_uid2_graphs_dag.airflow_dag

dag.user_defined_macros.update({
    "get_target_date": get_target_date,
    "get_graphs_begin_timestamp": get_graphs_begin_timestamp,
    "get_source_file_end_timestamp": get_source_file_end_timestamp,
})

load_mappings_uid2_graphs_task = Utils.create_pod_operator(
    dag=dag,
    name="load-mappings-uid2-graphs",
    task_id=load_mappings_uid2_graphs_task_id,
    sumologic_source_category="disneyidintegration-load-mappings-uid2-graphs",
    prometheus_job="disneyidintegration-load-mappings-uid2-graphs",
    execution_timeout=timedelta(hours=4),
    arguments=[
        "load-mappings-uid2-graphs",
        "--target-date",
        "{{ get_target_date(data_interval_end) }}",
        "--graphs",
        graphs_dir,
        "--after",
        "{{ get_graphs_begin_timestamp(dag_run, data_interval_end) }}",
        "--before",
        "{{ get_source_file_end_timestamp(data_interval_end) }}",
        "--collect-output",
        collected_dir_graphs,
    ],
)

import_task_arguments = [
    "import", "--target-date", "{{ get_target_date(data_interval_end) }}", "--hash", "true", "--update-cumulative-imported-count-metrics",
    "true"
]

import_graphs_task = Utils.create_pod_operator(
    dag=dag,
    name="import-graphs",
    task_id="import-graphs",
    sumologic_source_category="disneyidintegration-import-load-mappings-uid2-graphs",
    prometheus_job="disneyidintegration-import-load-mappings-uid2-graphs",
    execution_timeout=timedelta(minutes=60),
    arguments=import_task_arguments + ["--input", collected_dir_graphs],
)

load_mappings_uid2_graphs_dag >> load_mappings_uid2_graphs_task >> import_graphs_task
