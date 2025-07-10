from datetime import datetime

from dags.pdg.data_subject_request.dsar_tasks import DataSubjectAccessRequestTasks
from ttd.eldorado.base import TtdDag

###########################################
#   Job Configs
###########################################
job_name = 'data-governance-dsr-access-automation'
slack_channel = '#scrum-pdg-alerts'
job_start_date = datetime(2024, 7, 1)
job_schedule_interval = '0 0 * * 2'
job_type = "UserDSAR"

dag_id = 'dsar-pipeline'

tasks = DataSubjectAccessRequestTasks(dag_id)

###########################################
#   DAG Setup
###########################################
dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_channel,
    retries=0,
    tags=["Data Governance", "PDG", job_type],
)

adag = dag.airflow_dag

retrieve_cold_storage_task = tasks.create_retrieve_cold_storage_task(adag)
retrieve_targeting_data_task = tasks.create_targeting_data_task(adag)
retrieve_vertica_data_task = tasks.create_vertica_data_task(adag)
finalize_output_task = tasks.create_finalize_output_task(adag)

dag >> retrieve_cold_storage_task >> retrieve_targeting_data_task >> retrieve_vertica_data_task >> finalize_output_task
