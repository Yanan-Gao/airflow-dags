from datetime import timedelta, datetime

from datasources.sources.invmkt_datasources import InvMktDatasources
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.interop.vertica_import_operators import VerticaImportFromCloud, LogTypeFrequency
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

# Prod variables
job_name = 'MIST-avails-vertica-weekly-import'
invmkt = INVENTORY_MARKETPLACE().team
job_schedule_interval = timedelta(weeks=1)
job_start_date = datetime(2025, 4, 13, 23)
job_environment = TtdEnvFactory.get_from_system()
max_active_runs = 1
lw_import_gating_type_id = 2000556

task_name = 'Mist-Vertica-Weekly-Import'

# Date and Partition configs
run_date = '{{ dag_run.logical_date.strftime(\"%Y-%m-%d\") }}'
run_datetime = '{{ (dag_run.logical_date + macros.timedelta(weeks=1)).strftime(\"%Y-%m-%d %H:%M:%S\") }}'

###########################################
# DAG
###########################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=max_active_runs,
    slack_channel=invmkt.alarm_channel,
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=[invmkt.jira_team, task_name],
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": invmkt.jira_team,
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        "start_date": job_start_date,
    }
)

adag = dag.airflow_dag

###########################################
# Dataset Check Sensor step
###########################################
check_mist_data_sensor_task = OpTask(
    op=DatasetCheckSensor(
        ds_date=run_datetime,
        task_id='check_mist_data_availability',
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        datasets=[InvMktDatasources.mist_avails_agg_daily],
        timeout=60 * 60 * 12  # wait up to 12 hours
    )
)

###########################################
#   VerticaImportFromCloud
###########################################
open_lw_import_gate_task = VerticaImportFromCloud(
    dag=adag,
    subdag_name="mist_unsampled_avails",
    gating_type_id=lw_import_gating_type_id,
    log_type_id=LogTypeFrequency.WEEKLY.value,
    log_start_time=run_date,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))
dag >> check_mist_data_sensor_task >> open_lw_import_gate_task >> final_dag_check
