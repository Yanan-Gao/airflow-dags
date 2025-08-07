"""
NPI Pulling Pipeline

This pipeline pulls NPI (National Provider Identifier) data from external SFTP/S3.
"""
from datetime import datetime

from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import sav

from dags.sav.pharma_hcp.npi_common_config import NpiCommonConfig
from dags.sav.pharma_hcp.npi_operator_factory import NpiOperatorFactory

dag_id = "npi-pulling-pipeline"
job_schedule_interval = "0 * * * *"
job_start_date = datetime(2025, 5, 20)

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

pulling_task = NpiOperatorFactory.create_standard_npi_task(
    task_id="pharma_hcp_npi_pulling",
    task_name="pharma_hcp_npi_pulling",
    generator_task_type="NPIPullingJob",
    dag=dag,
    workload_size="light"
)

ttd_dag >> pulling_task
