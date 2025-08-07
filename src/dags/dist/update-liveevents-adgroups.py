from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.ttdslack import dag_post_to_slack_callback
from dags.dist.metrics import dag_metric_success_callback

dag_name = 'update-liveevents-adgroups'
job_slack_channel = '#taskforce-budget-alarms-high-pri'

job_schedule_interval = timedelta(minutes=10)
job_start_date = datetime(2023, 7, 12, 0, 0)

# Setup DAG
update_liveevents_adgroups_dag = DAG(
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
    dagrun_timeout=timedelta(hours=1),
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=dag_name, step_name="DAG", slack_channel=job_slack_channel, message="DAG has failed"),
    on_success_callback=dag_metric_success_callback(dag_name=dag_name, slack_channel=job_slack_channel)
)


def update_liveevents_adgroups_sproc(**kwargs):
    sql_hook = MsSqlHook(mssql_conn_id='ttd_airflow_provisioning', schema='Provisioning')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sql = 'exec dbo.prc_UpdateLiveEventsAdGroups'
    cursor.execute(sql)


update_liveevents_adgroups_sproc_task = PythonOperator(
    task_id='update_liveevents_adgroups_sproc', python_callable=update_liveevents_adgroups_sproc, dag=update_liveevents_adgroups_dag
)
