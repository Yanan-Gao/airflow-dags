"""
MicroTargeting DAG runs a Spark/EMR pipeline to detect any microtargeting ad groups.

This involves identifying HEC AdGroups, collecting their Audience, applying supported rails
to those Audiences and, finally, inserts those into Provisioning DB for the MicroTargeting State Machine.
"""
from datetime import datetime, timedelta

from dags.pdg.microtargeting_tasks.aws import MicrotargetingTasksAws
from dags.pdg.microtargeting_tasks.azure import MicrotargetingTasksAzure
from dags.pdg.microtargeting_tasks.copy_dataset import CopyDatasetTasks
from dags.pdg.microtargeting_tasks.export_audience import ExportAudienceTasks
from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from dags.pdg.task_utils import choose

dag_id = "hec-pipeline"
job_name = "HecMicroTargeting"
policy_id = 4  # HEC policy from dataPolicies.DataPolicy

shared_tasks = MicrotargetingTasksShared(dag_id, job_name)
aws_tasks = MicrotargetingTasksAws(dag_id, job_name)
azure_tasks = MicrotargetingTasksAzure(dag_id, job_name)
export_audience_tasks = ExportAudienceTasks(dag_id)
copy_dataset_tasks = CopyDatasetTasks(dag_id)

dag = shared_tasks.create_dag(
    additional_tags=['HEC'],
    start_date=datetime(2024, 12, 1, 0, 0),
    schedule_interval=choose(
        prod="0 10 * * *",
        # Adjusted cron schedule for testing to minimize cost.
        # Runs once weekly to ensure test integration remains functional.
        test="0 12 * * 1"
    ),
    retries=0,
    dagrun_timeout=timedelta(hours=5),
    policy_label='hec',
    policy_id=policy_id,
)

adag = dag.airflow_dag

set_context_shared = shared_tasks.create_shared_set_context_task(adag)
set_context_aws = aws_tasks.create_aws_set_context_task(adag)
set_context_azure = azure_tasks.create_azure_set_context_task(adag)

retrieve_data_emr = aws_tasks.create_aws_retrieve_adgroup_data_task("RetrieveHecAdGroupData")
export_audience_emr = export_audience_tasks.create_aws_export_audiences_service_task()
apply_rails_emr = aws_tasks.create_aws_rails_application_task()
write_output_emr = aws_tasks.create_aws_write_output_task()

check_adjacent_zip_codes_emr = aws_tasks.create_aws_check_adjacent_zip_codes_task()
write_zip_code_check_history_emr = aws_tasks.create_aws_write_zip_code_check_history_task()

copy_geo_store_data = copy_dataset_tasks.create_copy_geo_store_data_task()
copy_us_zip_adjacency_data = copy_dataset_tasks.create_copy_us_zip_adjacency_data_task()

retrieve_data_hdi = azure_tasks.create_azure_retrieve_adgroup_data_task("RetrieveHecAdGroupData")
export_audience_hdi = export_audience_tasks.create_azure_export_audiences_service_task()
apply_rails_and_write_output_hdi = azure_tasks.create_azure_rails_and_write_output_application_task()

check_adjacent_zip_codes_hdi = azure_tasks.create_azure_check_adjacent_zip_codes_task()

dag >> set_context_shared >> [set_context_aws, set_context_azure]

set_context_aws >> retrieve_data_emr >> export_audience_emr
export_audience_emr >> apply_rails_emr >> write_output_emr

retrieve_data_emr >> check_adjacent_zip_codes_emr >> write_zip_code_check_history_emr

set_context_azure >> retrieve_data_hdi
set_context_azure >> copy_geo_store_data
set_context_azure >> copy_us_zip_adjacency_data

retrieve_data_hdi >> export_audience_hdi >> apply_rails_and_write_output_hdi
copy_geo_store_data >> apply_rails_and_write_output_hdi

retrieve_data_hdi >> check_adjacent_zip_codes_hdi
copy_us_zip_adjacency_data >> check_adjacent_zip_codes_hdi
