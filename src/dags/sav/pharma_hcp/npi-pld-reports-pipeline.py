"""
NPI PLD Reports Pipeline

This pipeline generates PLD reports.
"""
from datetime import datetime

from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import sav

from dags.sav.pharma_hcp.npi_common_config import NpiCommonConfig
from dags.sav.pharma_hcp.npi_operator_factory import NpiOperatorFactory

dag_id = "npi-pld-reports-pipeline"
job_schedule_interval = "0 1 * * *"
job_start_date = datetime(2025, 7, 1)

ttd_dag = TtdDag(
    dag_id=dag_id,
    slack_channel=NpiCommonConfig.NOTIFICATION_SLACK_CHANNEL,
    enable_slack_alert=True,
    start_date=job_start_date,
    run_only_latest=True,
    schedule_interval=job_schedule_interval,
    retries=NpiCommonConfig.DEFAULT_RETRIES,
    max_active_runs=1,
    depends_on_past=True,
    retry_delay=NpiCommonConfig.DEFAULT_RETRY_DELAY,
    tags=[sav.jira_team],
)
dag = ttd_dag.airflow_dag

additional_pld_reports_env_vars = {
    "TTD_NPI_REPORTING_REPORTDATE":
    "{{ (dag_run.conf.get('REPORTDATE') if dag_run and dag_run.conf else None) or ds }}",
    "TTD_NPI_REPORTING_OverrideAdvertiserList":
    "{{ dag_run.conf.get('OverrideAdvertiserList') if dag_run and dag_run.conf and dag_run.conf.get('OverrideAdvertiserList') is not none else '' }}"
}

pld_reports_task = NpiOperatorFactory.create_standard_npi_task(
    task_id="npi_pld_reports_generator",
    task_name="npi_pld_reports_generator",
    generator_task_type="NpiReportsGenerator",
    dag=dag,
    workload_size="standard",
    additional_env_vars=additional_pld_reports_env_vars
)

ttd_dag >> pld_reports_task
