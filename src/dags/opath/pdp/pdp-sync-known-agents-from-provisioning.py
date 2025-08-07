from airflow import DAG
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.slack.slack_groups import OPATH
from ttd.ttdslack import dag_post_to_slack_callback
from dags.dist.metrics import dag_metric_success_callback

dag_name = 'pdp-sync-known-agents-from-provisioning'
job_slack_channel = OPATH.team.alarm_channel

job_schedule_interval = timedelta(minutes=120)
job_start_date = datetime(2024, 1, 20, 0, 0)

sql_connection_id = "ttd_airflow_openpath_pdp"

# Setup DAG
pdp_sync_known_agents_from_provisioning_dag = DAG(
    dag_name,
    schedule_interval=job_schedule_interval,
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "start_date": job_start_date,
    },
    max_active_runs=1,  # we only want 1 run at a time. Sproc shouldn't run multiple times concurrently
    catchup=False,  # Just execute the latest run. It's fine to skip old runs
    dagrun_timeout=timedelta(hours=2),
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=dag_name, step_name="DAG", slack_channel=job_slack_channel, message="DAG has failed"),
    on_success_callback=dag_metric_success_callback(dag_name=dag_name, slack_channel=job_slack_channel)
)


def execute_pdp_sproc(sproc_name: str, **kwargs):
    connection = BaseHook.get_connection(sql_connection_id)
    sql_hook = MsSqlHook(mssql_conn_id=connection, schema='OpenPath')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sql = f'exec {sproc_name}'
    cursor.execute(sql)


pdp_sync_known_agents_from_provisioning_sproc_task = PythonOperator(
    task_id='pdp_sync_known_agents_from_provisioning_sproc',
    python_callable=execute_pdp_sproc,
    dag=pdp_sync_known_agents_from_provisioning_dag,
    op_kwargs={'sproc_name': 'pdp.prc_SyncKnownAgentsFromProvisioning'}
)

pdp_construct_known_agents_task = PythonOperator(
    task_id='pdp_construct_known_agents_sproc',
    python_callable=execute_pdp_sproc,
    dag=pdp_sync_known_agents_from_provisioning_dag,
    op_kwargs={'sproc_name': 'pdp.prc_ModifyKnownAgents'}
)

pdp_sync_known_agents_from_provisioning_sproc_task >> pdp_construct_known_agents_task
