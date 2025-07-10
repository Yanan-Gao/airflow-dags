from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 0 5 * *"
    retry_delay = timedelta(minutes=30)
else:
    schedule_interval = "*/15 * * * *"
    retry_delay = timedelta(minutes=10)

start_date = datetime(2024, 5, 1)


# macros
def get_purge_date(data_interval_end) -> str:
    return (data_interval_end - relativedelta(months=3)).strftime(Utils.DATE_FORMAT)


# DAG
purge_mappings_dag = Utils.create_dag(
    dag_id="disney-id-integration-purge-mappings",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = purge_mappings_dag.airflow_dag

dag.user_defined_macros.update({
    "get_purge_date": get_purge_date,
})

purge_mappings = Utils.create_pod_operator(
    dag=dag,
    name="purge-mappings",
    task_id="purge-mappings",
    sumologic_source_category="disneyidintegration-purge-mappings",
    prometheus_job="disneyidintegration-purge-mappings",
    execution_timeout=timedelta(hours=1),
    arguments=[
        "purge-mappings",
        "--before",
        "{{ get_purge_date(data_interval_end) }}",
    ],
)

purge_mappings_dag >> purge_mappings
