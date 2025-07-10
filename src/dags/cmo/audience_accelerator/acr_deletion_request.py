import logging
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from datetime import timedelta, datetime

from dags.pdg.data_subject_request.operators.parquet_delete_operators.aws import AWSParquetDeleteOperation
from dags.pdg.data_subject_request.operators.parquet_delete_operators.shared import XCOM_PULL_STR, DSR
from datasources.sources.tivo_datasources import TivoExternalDatasource
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

pipeline_name = 'acr-deletion-request'
job_start_date = datetime(2024, 8, 19)
env = TtdEnvFactory.get_from_system()
job_schedule_interval = '@daily'

dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["acr"],
    retries=1,
    retry_delay=timedelta(hours=1)
)
adag: DAG = dag.airflow_dag


def check_deletion_request_for_date(**context):
    """ Check if there's a deletion file for the date """
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    logical_date = context['logical_date']
    formatted_date_str = (logical_date + timedelta(days=-3)).strftime("%Y_%m_%d")

    if (aws_storage.check_for_wildcard_key(bucket_name=TivoExternalDatasource.tivo_request_to_delete.bucket,
                                           wildcard_key='tivo/tv_viewership/papaya_1/request_to_delete/file_name=' + formatted_date_str +
                                           "_*.csv")):
        logging.info("Found request to delete file for: " + formatted_date_str)
        return True
    logging.info("There is no request to delete file for: " + formatted_date_str)
    return False


check_deletion_request = OpTask(
    op=ShortCircuitOperator(
        task_id='check_deletion_request',
        dag=adag,
        retry_exponential_backoff=True,
        provide_context=True,
        python_callable=check_deletion_request_for_date
    )
)


def get_deletion_request_file_content(**context):
    """ Read the file content for deletion request """
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    logical_date = context['logical_date']
    formatted_date_str = (logical_date + timedelta(days=-3)).strftime("%Y_%m_%d")

    key = aws_storage.get_wildcard_key(
        bucket_name=TivoExternalDatasource.tivo_request_to_delete.bucket,
        wildcard_key='tivo/tv_viewership/papaya_1/request_to_delete/file_name=' + formatted_date_str + "_*.csv"
    )

    if key:
        logging.info(f" -------------- Reading deletion file {key} -------------- ")
        content = aws_storage.read_key(key=key, bucket_name=TivoExternalDatasource.tivo_request_to_delete.bucket)
        ids = [x.strip() for x in content.strip().split("\n")[1::]]
        content_string = ','.join(ids)
    else:
        logging.error(" -------------- Did not find deletion file to read, something is wrong! -------------- ")
        content_string = ""

    if content_string == "":
        raise Exception("Error reading content from request to delete file - result is empty!")

    return content_string


get_deletion_request_file_content_job = OpTask(
    op=PythonOperator(
        dag=adag, task_id="get_deletion_request_file_content", python_callable=get_deletion_request_file_content, provide_context=True
    )
)

masterFleetInstances = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

coreFleetInstances = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=2
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> check_deletion_request >> get_deletion_request_file_content_job

dataset_configs = {
    "tivoipaddress": {
        "master_fleet_instance_type_configs": masterFleetInstances,
        "core_fleet_instance_type_configs": coreFleetInstances
    },
    "tivomaid": {
        "master_fleet_instance_type_configs": masterFleetInstances,
        "core_fleet_instance_type_configs": coreFleetInstances
    },
}

# Pull the content_string read from get_deletion_request_file_content task
dsr_request_id = XCOM_PULL_STR.format(dag_id=pipeline_name, task_id='poll_jira_epic_for_emails_to_process', key='request_id')
processing_mode = DSR(
    uiids=XCOM_PULL_STR.format(dag_id=pipeline_name, task_id=get_deletion_request_file_content_job.task_id, key="return_value")
)

delete_tasks = AWSParquetDeleteOperation(dsr_request_id=dsr_request_id,
                                         job_class_name="com.thetradedesk.dsr.delete.ParquetUID2DeleteJob",
                                         mode=processing_mode,
                                         dataset_configs=dataset_configs) \
    .create_parquet_delete_job_tasks()

for delete_task in delete_tasks:
    get_deletion_request_file_content_job >> delete_task >> final_dag_status_step
