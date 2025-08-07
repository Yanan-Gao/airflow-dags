"""
MicroTargeting DAG runs a Spark/EMR pipeline to detect any microtargeting ad groups.

This involves identifying US Political AdGroups, collecting their Audience, applying supported rails
to those Audiences and, finally, inserts those into Provisioning DB for the MicroTargeting State Machine.
"""

from datetime import datetime, timedelta

from dags.pdg.microtargeting_tasks.aws import MicrotargetingTasksAws
from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from dags.pdg.microtargeting_tasks.export_audience import ExportAudienceTasks
from dags.pdg.task_utils import choose

dag_id = 'microtargeting-pipeline'
job_name = "UsPoliticalMicroTargeting"
policy_id = 5  # US Political policy from dataPolicies.DataPolicy

shared_tasks = MicrotargetingTasksShared(dag_id, job_name)
tasks = MicrotargetingTasksAws(dag_id, job_name)
export_audience_tasks = ExportAudienceTasks(dag_id)

dag = shared_tasks.create_dag(
    additional_tags=['Microtargeting'],
    start_date=datetime(2024, 9, 16, 0, 0),
    schedule_interval=choose(
        prod="0 11 * * *",
        # Adjusted cron schedule for testing to minimize cost.
        # Runs once weekly to ensure test integration remains functional.
        test="0 12 * * 1"
    ),
    retries=0,
    dagrun_timeout=timedelta(hours=5),
    policy_label='us-political',
    policy_id=policy_id,
)

adag = dag.airflow_dag

set_context_shared = shared_tasks.create_shared_set_context_task(adag)
set_context_aws = tasks.create_aws_set_context_task(adag)

# TODO(PDG-1417): Organise information dump
# We split producing retrieval of adgroup data, audiences and applying the geo rail pipeline
# to utilise different cluster setups for each and creating a checkpoint
# in case of failure allowing us to minimise retryable pieces.

retrieve_data = tasks.create_aws_retrieve_adgroup_data_task("RetrieveUsPoliticalAdGroupData")
export_audience = export_audience_tasks.create_aws_export_audiences_service_task()
write_output = tasks.create_aws_write_output_task()

dag >> set_context_shared >> set_context_aws >> retrieve_data >> export_audience

export_audience >> tasks.create_aws_rails_application_task() >> write_output
