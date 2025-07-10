from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from ttd.ttdslack import dag_post_to_slack_callback

dag_name = "perf-automation-default-frequency-counter-sproc"
job_slack_channel = "#scrum-perf-auto-audience-alerts"

job_schedule_interval = timedelta(minutes=30)
job_start_date = datetime(2024, 7, 8, 0, 0)

create_default_reach_counters_dag = DAG(
    dag_name,
    schedule_interval=job_schedule_interval,
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "start_date": job_start_date,
    },
    max_active_runs=1,  # we only want 1 run at a time. Sproc shouldn't run multiple times concurrently
    catchup=False,  # Just execute the latest run. It's fine to skip old runs
    dagrun_timeout=timedelta(hours=1),
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_name,
        step_name="DAG",
        slack_channel=job_slack_channel,
        message="Default Frequency Counter DAG has failed",
    ),
    tags=["AUDAUTO"]
)


def create_default_frequency_counters_sproc(**kwargs):
    sql_hook = MsSqlHook(mssql_conn_id="ttd_audauto_provdb", schema="Provisioning")
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sql = "exec dbo.prc_CreateDefaultFrequencyCounterForReachKPIs @commitChanges=1"
    cursor.execute(sql)


create_default_reach_counters_sproc_task = PythonOperator(
    task_id="run_default_frequency_counter_sproc",
    python_callable=create_default_frequency_counters_sproc,
    dag=create_default_reach_counters_dag,
    provide_context=True,
)
