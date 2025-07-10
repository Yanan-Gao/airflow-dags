from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 12 * * *"
    retry_delay = timedelta(minutes=30)
else:
    schedule_interval = "*/15 * * * *"
    retry_delay = timedelta(minutes=10)

start_date = datetime(2025, 2, 17)
bidrequest_dir = Utils.get_bidrequest_dir()
exec_date = "{{ logical_date.subtract(days=1).strftime('dayDate=%Y-%m-%d') }}"
full_bidrequest_read_path = bidrequest_dir + '/' + exec_date

load_mappings_bidrequest_task_id = "load-mappings-bidrequest"

collected_dir_bidrequest = Utils.get_job_dir("collect-load-mappings-bidrequest")


def get_target_date(data_interval_end) -> str:
    return datetime(data_interval_end.year, data_interval_end.month, 1).strftime(Utils.DATE_FORMAT)


load_mappings_uid2_bidrequest_dag = Utils.create_dag(
    dag_id="disney-id-integration-load-mappings-bidrequest",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = load_mappings_uid2_bidrequest_dag.airflow_dag

dag.user_defined_macros.update({"get_target_date": get_target_date})

load_mappings_bidrequest_task = Utils.create_pod_operator(
    dag=dag,
    name="load-mappings-bidrequest",
    task_id=load_mappings_bidrequest_task_id,
    sumologic_source_category="disneyidintegration-load-mappings-bidrequest",
    prometheus_job="disneyidintegration-load-mappings-bidrequest",
    execution_timeout=timedelta(hours=4),
    arguments=[
        "load-mappings-bidrequest",
        "--target-date",
        "{{ get_target_date(data_interval_end) }}",
        "--location",
        full_bidrequest_read_path,
        "--collect-output",
        collected_dir_bidrequest,
    ],
)

import_task_arguments = [
    "import", "--target-date", "{{ get_target_date(data_interval_end) }}", "--hash", "true", "--update-cumulative-imported-count-metrics",
    "true"
]

import_bidrequest_task = Utils.create_pod_operator(
    dag=dag,
    name="import-bidrequest",
    task_id="import-bidrequest",
    sumologic_source_category="disneyidintegration-import-load-mappings-bidrequest",
    prometheus_job="disneyidintegration-import-load-mappings-bidrequest",
    execution_timeout=timedelta(minutes=60),
    arguments=import_task_arguments + ["--input", collected_dir_bidrequest],
)

load_mappings_uid2_bidrequest_dag >> load_mappings_bidrequest_task >> import_bidrequest_task
