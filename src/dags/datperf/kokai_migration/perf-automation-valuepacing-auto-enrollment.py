"""
Runs a sproc in provisioning to update new kokai and/or managed service campaigns to value pacing

Job Details:
    - Runs every 30 minutes
    - Can retry once after 15 minutes
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

###########################################
#   Job Configs
###########################################

dag_name = 'perf-automation-valuepacing-auto-enrollment'

job_schedule_interval = timedelta(minutes=15)
job_start_date = datetime(2024, 11, 6, 0, 0)


def get_sproc_datetime_arg():
    # get the current datetime and put it in the right format for the sproc
    # leaving a big 2-month gap, because we now have a processing limit, and we might fall behind.
    # believe it or not, this only increases the number of candidate campaigns by about 20%
    # and it will be even fewer once we start closing the eligibility gap.
    lookback = datetime.now() - timedelta(days=60)
    formatted_lookback = lookback.strftime('%Y-%m-%dT%H:00:00')
    print(formatted_lookback)
    return formatted_lookback


value_pacing_update_dag = DAG(
    dag_name,
    schedule_interval=job_schedule_interval,
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": job_start_date,
    },
    max_active_runs=1,  # we only want 1 run at a time. Sproc shouldn't run multiple times concurrently
    catchup=False,  # Just execute the latest run. It's fine to skip old runs
    dagrun_timeout=timedelta(minutes=30)  # (optional) Don't allow it to run longer
)

# Setup sproc call
updateManagedService = 1
updateKokai = 1
updateOnlyCreatedCloned = 0
testOnly = 0
maxCampaigns = 150  # max campaigns to be migrated (about ~5k per day)
applyRestrictedModels = 1  # change to 1 when ready to start enabling sensitive models


def run_update_sproc(lastHour, **kwargs):
    logger = logging.getLogger(__name__)
    sql_hook = MsSqlHook(mssql_conn_id='ttd_perfauto_provdb', schema='Provisioning')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sprocCall = f"declare @updateFrom datetime; set @updateFrom = cast('{lastHour}' as datetime); exec dbo.prc_UpdateCampaignsToValuePacing @updateFrom = @updateFrom, @updateManagedService = {updateManagedService}, @updateKokai = {updateKokai}, @updateOnlyCreatedCloned = {updateOnlyCreatedCloned}, @maxCampaigns = {maxCampaigns},  @testOnly = {testOnly}, @applyRestrictedModels = {applyRestrictedModels}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_migrated_campaign_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_changed_campaign_count',
        description='Distributed Algos - count of campaigns automatically migrated'
    )

    # resultset 1 and 2 contains model override outputs and must be skipped
    cursor.nextset()
    cursor.nextset()
    # resultset 3 contains pharma results and also must be skipped for this DAG
    cursor.nextset()

    # resultset 4 is migrated campaigns
    changed_campaigns = cursor.fetchall()
    logger.info("Fetched %i rows.", len(changed_campaigns))
    if len(changed_campaigns) > 0:
        # get count of campaigns migrated... but campaigns might be duplicated
        all_campaigns = [r[0] for r in changed_campaigns if r[4] == 1]
        migrated_count = len(set(all_campaigns))
        logger.info("Found %i migrated campaigns.", migrated_count)
        prom_migrated_campaign_count.labels({'change_type': 'migrated'}).set(migrated_count)

    # resultset 4 is count of campaigns in queue
    cursor.nextset()
    queue_length = cursor.fetchone()[0]
    prom_migrated_campaign_count.labels({'change_type': 'in_queue'}).set(queue_length)

    push_all(dag_name)


run_update_sproc_task = PythonOperator(
    task_id='run_update_sproc',
    python_callable=run_update_sproc,
    dag=value_pacing_update_dag,
    provide_context=True,
    op_kwargs={'lastHour': get_sproc_datetime_arg()}
)
