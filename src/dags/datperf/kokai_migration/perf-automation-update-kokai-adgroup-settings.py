"""
Runs sprocs in provisioning to update adgroups settings for kokai compatibilty including:
    - settings new or upgraded Kokai adgroups to koa SD (3.5)
    - setting new or upgraded Kokai adgroups with QA rails to QASuiteEnabled = 0

Job Details:
    - Runs every 15 minutes
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

###########################################
#   Job Configs
###########################################

dag_name = 'perf-automation-update-kokai-adgroup-settings'

job_schedule_interval = timedelta(minutes=15)
job_start_date = datetime(2024, 11, 6, 0, 0)

# Setup DAG
update_kokai_adgroup_settings_dag = DAG(
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
    dagrun_timeout=timedelta(hours=1)  # (optional) Don't allow it to run longer
)

# Setup sproc call
commitChanges = 1


def get_cursor(connection_id, schema):
    sql_hook = MsSqlHook(mssql_conn_id=connection_id, schema=schema)
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    return conn.cursor()


# this sproc enables KoaSd for adgroups migrated to Kokai, it also deletes Koa V3 bidlists
def run_koasd_update_sproc(**kwargs):
    logger = logging.getLogger(__name__)
    cursor = get_cursor('ttd_perfauto_provdb', 'Provisioning')
    sprocCall = f"exec dbo.prc_UpdateKokaiAdGroupsToKoaSD @debug=0, @commitChanges={commitChanges}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_updated_adgroup_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_updated_koasd_adgroup_count',
        description='Distributed Algos - count of adgroups automatically migrated'
    )

    # sproc returns one row - count of upgraded adgroups
    totals = cursor.fetchone()
    upgraded_count = totals[0]
    logger.info("Found %i migrated adgroups.", upgraded_count)
    prom_updated_adgroup_count.set(upgraded_count)

    push_all(dag_name)


def run_update_adgroup_vp_settings_sproc(**kwargs):
    logger = logging.getLogger(__name__)
    cursor = get_cursor('ttd_perfauto_provdb', 'Provisioning')
    sprocCall = f"exec dbo.prc_UpdateAdGroupSettingsForValuePacing @debug=0, @commitChanges={commitChanges}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_bbao_adgroup_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_deprecated_bbao_adgroup_count',
        description='Distributed Algos - count of adgroups BBAO was disabled for'
    )

    prom_hmrmfp_adgroup_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_deprecated_hmrmfp_adgroup_count',
        description='Distributed Algos - count of adgroups HMRMFP was disabled for'
    )

    # sproc returns two rows - first is count of bbao disabled adgroups and second is count of hmrmfp disabled adgroups
    bbao_count = cursor.fetchone()[0]
    logger.info(f"Found {bbao_count} adgroups which BBAO was disabled for.")

    cursor.nextset()
    hmrmfp_count = cursor.fetchone()[0]
    logger.info(f"Found {hmrmfp_count} adgroups which HMRMFP was disabled for.")

    prom_bbao_adgroup_count.set(bbao_count)
    prom_hmrmfp_adgroup_count.set(hmrmfp_count)

    push_all(dag_name)


# this sproc enables QA in KoKai UI for adgroups (with QA railes) migrated to Kokai
def run_qa_update_sproc(**kwargs):
    logger = logging.getLogger(__name__)
    cursor = get_cursor('ttd_perfauto_provdb', 'Provisioning')
    sprocCall = f"exec dbo.prc_UpdateKokaiAdGroupsWithQARails @debug=0, @commitChanges={commitChanges}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_updated_adgroup_count = get_or_register_gauge(
        job=dag_name, name='distributed_algos_updated_qa_adgroup_count', description='Distributed Algos - count of qa adgroups updated'
    )

    # sproc returns one row - count of updated adgroups
    totals = cursor.fetchone()
    upgraded_count = totals[0]
    logger.info("Found %i updated qa adgroups.", upgraded_count)
    prom_updated_adgroup_count.set(upgraded_count)

    push_all(dag_name)


# this sproc enables sensitive advertisers to use the default DA neo settings for RSM on campaigns with active prism adgroups
def run_update_sensitive_advertiser_campaign_neo_status_sproc(**kwargs):
    logger = logging.getLogger(__name__)
    cursor = get_cursor('ttd_perfauto_provdb', 'Provisioning')
    sprocCall = f"exec dbo.prc_UpdateSensitiveAdvertiserNeoStatus @removeOverrideForPrismOnly=1, @debug=0, @commitChanges={commitChanges}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_updated_adgroup_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_updated_sensitive_advertiser_campaign_count',
        description='Distributed Algos - count of sensitive advertiser campaigns with RSM defaults enabled'
    )

    # sproc returns one row - count of updated campaigns
    totals = cursor.fetchone()
    upgraded_count = totals[0]
    logger.info("Found %i updated sensitive advertiser campaigns.", upgraded_count)
    prom_updated_adgroup_count.set(upgraded_count)

    push_all(dag_name)


def run_update_pharma_adgroups_to_offline_kpis(**kwargs):
    logger = logging.getLogger(__name__)
    cursor = get_cursor('ttd_perfauto_provdb', 'Provisioning')
    sprocCall = f"exec dbo.prc_UpdatePharmaVPAdGroupsToOfflineKpis @commitChanges={commitChanges}"
    logger.info(f"Calling {sprocCall}")
    sql = sprocCall
    cursor.execute(sql)

    prom_offline_kpi_adgroup_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_new_pharma_offline_kpis_count',
        description='Distributed Algos - count of new or recently enabled pharma goal adgroups opted into offline kpis'
    )

    prom_campaign_stack_rank_adgroup_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_deprecated_campaign_stack_rank_pharma_adgroup_count',
        description='Distributed Algos - count of pharma goal adgroups where campaign stack rank for APAA needed to be reset'
    )

    prom_apaa_campaign_count = get_or_register_gauge(
        job=dag_name,
        name='distributed_algos_deprecated_apaa_pharma_campaign_count',
        description='Distributed Algos - count of pharma goal campaigns where APAA needed to be disabled'
    )

    # sproc returns three rows - first is count of new offline kpi adgroups, second is count of adgroups where campaign stack rank was reset, third is count of campaigns where APAA was disabled
    offline_kpi_count = cursor.fetchone()[0]
    logger.info(f"Found {offline_kpi_count} pharma goal adgroups opted in to offline kpis.")

    cursor.nextset()
    campaign_stack_rank_reset_count = cursor.fetchone()[0]
    logger.info(f"Found {campaign_stack_rank_reset_count} pharma adgroups which campaign stack rank was reset.")

    cursor.nextset()
    apaa_disabled_count = cursor.fetchone()[0]
    logger.info(f"Found {apaa_disabled_count} pharma campaigns which APAA was disabled.")

    prom_offline_kpi_adgroup_count.set(offline_kpi_count)
    prom_campaign_stack_rank_adgroup_count.set(campaign_stack_rank_reset_count)
    prom_apaa_campaign_count.set(apaa_disabled_count)

    push_all(dag_name)


run_koasd_update_sproc_task = PythonOperator(
    task_id='run_koasd_update_sproc', python_callable=run_koasd_update_sproc, dag=update_kokai_adgroup_settings_dag, provide_context=True
)

run_adgroup_vp_settings_update_sproc_task = PythonOperator(
    task_id='run_adgroup_vp_settings_update_sproc',
    python_callable=run_update_adgroup_vp_settings_sproc,
    dag=update_kokai_adgroup_settings_dag,
    provide_context=True
)

run_qa_update_sproc_task = PythonOperator(
    task_id='run_qa_update_sproc', python_callable=run_qa_update_sproc, dag=update_kokai_adgroup_settings_dag, provide_context=True
)

run_sensitive_advertiser_update_sproc_task = PythonOperator(
    task_id='run_sensitive_advertiser_update_sproc_task',
    python_callable=run_update_sensitive_advertiser_campaign_neo_status_sproc,
    dag=update_kokai_adgroup_settings_dag,
    provide_context=True
)

run_pharma_adgroup_offline_kpi_upgrade_sproc_task = PythonOperator(
    task_id='run_pharma_adgroup_offline_kpi_upgrade_sproc_task',
    python_callable=run_update_pharma_adgroups_to_offline_kpis,
    dag=update_kokai_adgroup_settings_dag,
    provide_context=True
)

run_koasd_update_sproc_task >> run_adgroup_vp_settings_update_sproc_task >> run_qa_update_sproc_task >> run_sensitive_advertiser_update_sproc_task >> run_pharma_adgroup_offline_kpi_upgrade_sproc_task
