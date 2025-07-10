from datetime import timedelta, datetime

from dags.forecast.columnstore.columnstore_common_setup import ColumnStoreDAGSetup
from dags.forecast.columnstore.columnstore_sql import ColumnStoreFunctions
from ttd.eldorado.base import TtdDag
from ttd.tasks.chain import ChainOfTasks
from dags.forecast.columnstore.enums.xd_level import XdLevel, get_tables

columnstore_sql_functions = ColumnStoreFunctions()
columnstore_dag_common_parameters = ColumnStoreDAGSetup()

tasks = []

date = "{{ data_interval_start.subtract(days=30).format('YYYY-MM-DD') }}"

for xd_level in XdLevel:
    tables = get_tables(xd_level, 60)
    task_id = f"drop_partitions_{xd_level.name}"
    tasks.append(columnstore_sql_functions.drop_partitions_query(tables=tables, latest_date_to_drop=date, task_id=task_id))

tasks.append(
    columnstore_sql_functions.drop_partitions_query(
        tables=["VectorValueMappings", "SuccessfulLoads"], latest_date_to_drop=date, task_id="drop_partitions_VectorValueMappings"
    )
)

task_group_name = "drop_vertica_partitions"

chain_of_tasks = ChainOfTasks(task_id=task_group_name, tasks=tasks).as_taskgroup(task_group_name)

job_start_date = datetime(year=2024, month=4, day=10, hour=0)
# Every day at midnight
job_schedule_interval = "0 0 * * *"
active_running_jobs = 1

dag = TtdDag(
    dag_id="uf-columnstore-drop-partitions",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=columnstore_dag_common_parameters.slack_channel,
    tags=columnstore_dag_common_parameters.tags,
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30),
    dag_tsg=columnstore_dag_common_parameters.dag_tsg,
    default_args=columnstore_dag_common_parameters.default_args
)

dag >> chain_of_tasks

adag = dag.airflow_dag
