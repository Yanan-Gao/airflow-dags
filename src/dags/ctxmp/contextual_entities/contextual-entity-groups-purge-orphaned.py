"""
Runs sprocs in provisioning to purge orphaned contextual entity groups.

Job Details:
    - Runs daily
    - Can retry once after 5 minutes
    - Terminate in 1 hour if not finished
    - Expected to not take much time
    - Can only run one job at a time
"""
from airflow import DAG
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all
from ttd.slack.slack_groups import CTXMP
from ttd.ttdslack import dag_post_to_slack_callback

###########################################
#   Job Configs
###########################################

dag_name = 'contextual-entity-groups-purge-orphaned'

job_schedule_interval = timedelta(hours=24)
job_start_date = datetime(2025, 7, 22, 3)

# Setup DAG
purge_contextual_entity_group_dag = DAG(
    dag_name,
    schedule_interval=job_schedule_interval,
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": CTXMP.team.jira_team,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": job_start_date,
    },
    max_active_runs=1,  # we only want 1 run at a time. Sproc shouldn't run multiple times concurrently
    catchup=False,  # Just execute the latest run. It's fine to skip old runs
    dagrun_timeout=timedelta(hours=1),  # (optional) Don't allow it to run longer
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_name,
        step_name="parent dagrun",
        slack_channel=CTXMP.team.alarm_channel,
        message="Contextual Entity Groups purge job has failed"
    ),
    tags=[CTXMP.team.name, CTXMP.team.jira_team]
)

# Setup sproc call
commitChanges = 1


def get_cursor(connection_id, schema):
    sql_hook = MsSqlHook(mssql_conn_id=connection_id, schema=schema)
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    return conn.cursor()


# this sproc purges Contextual Entity Groups where there is no CEG mapping to a Contextual Entity and the CEG is non-sharable
def run_ceg_purge_sproc(**kwargs):
    logger = logging.getLogger(__name__)
    cursor = get_cursor('ttd_airflow_provisioning', 'Provisioning')
    sprocCall = f"exec dbo.prc_PurgeOrphanedContextualEntityGroups @debug=0, @commitChanges={commitChanges}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_purged_ceg_count = get_or_register_gauge(
        job=dag_name, name='ctxmp_purged_ceg_count', description='CTX Marketplace - count of orphaned CEGs purged'
    )

    # sproc returns one row - count of purged ceg
    totals = cursor.fetchone()
    purged_count = totals[0]
    logger.info("Found %i Contextual Entity Groups to purge.", purged_count)
    prom_purged_ceg_count.set(purged_count)

    push_all(dag_name)


run_ceg_purge_sproc_task = PythonOperator(
    task_id='run_ceg_purge_sproc', python_callable=run_ceg_purge_sproc, dag=purge_contextual_entity_group_dag, provide_context=True
)
