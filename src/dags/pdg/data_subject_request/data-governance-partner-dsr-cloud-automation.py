from datetime import datetime, timedelta

from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

from dags.pdg.data_subject_request.config.vertica_config import cleanse_table_name, \
    PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_ADVERTISER, PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_MERCHANT
from dags.pdg.data_subject_request.handler import partner_dsr_handler, monitor_logex_task
from dags.pdg.data_subject_request.handler.vertica_scrub_generator_creator import VerticaScrubGeneratorTaskPusher, \
    create_generator_tasks_partner_dsr
from dags.pdg.data_subject_request.operators.parquet_delete_operators import PARTNER_DSR_DATASET_CONFIG
from dags.pdg.data_subject_request.operators.parquet_delete_operators.aws import AWSParquetDeleteOperation
from dags.pdg.data_subject_request.util import dsr_dynamodb_util
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask

###########################################
#   Job Configs
###########################################

job_name = 'data-governance-partner-dsr-cloud-automation'
slack_channel = '#scrum-pdg-alerts'
job_start_date = datetime(2024, 7, 1)
# TODO this should change back to weekly after https://thetradedesk.atlassian.net/browse/PDG-2909
job_schedule_interval = '7 11 * * *'  # Daily at 11:07 am UTC
job_class_name = "com.thetradedesk.dsr.deletev2.ParquetDeleteV2JobMain"


###########################################
#   DAG success and failure callbacks
###########################################
def persist_dag_failed(context):
    task_instance = context['task_instance']
    request_id = task_instance.xcom_pull(key='batch_request_id')
    dsr_dynamodb_util.persist_dag_ended('failed', request_id, context['run_id'])
    items_processes = task_instance.xcom_pull(key='partner_dsr_requests')
    dsr_dynamodb_util.update_items_processing_state(items_processes, 'failed')


def persist_dag_success(context):
    task_instance = context['task_instance']
    request_id = task_instance.xcom_pull(key='batch_request_id')
    dsr_dynamodb_util.persist_dag_ended('completed', request_id, context['run_id'])
    items_processes = task_instance.xcom_pull(key='partner_dsr_requests')
    dsr_dynamodb_util.update_items_processing_state(items_processes, 'completed')


###########################################
#   DAG Setup
###########################################

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_channel,
    retries=0,
    retry_delay=timedelta(minutes=1),
    tags=["Data Governance", "PDG"],
    on_failure_callback=persist_dag_failed,
    on_success_callback=persist_dag_success,
)

adag = dag.airflow_dag

process_partner_dsr_requests_ready_for_cloud_processing = OpTask(
    op=ShortCircuitOperator(
        task_id='process_partner_dsr_requests_ready_for_cloud_processing',
        dag=adag,
        provide_context=True,
        python_callable=partner_dsr_handler.process_partner_dsr_requests_ready_for_cloud_processing,
    )
)

save_tenant_id_and_partner_id = OpTask(
    op=PythonOperator(
        task_id='save_tenant_id_and_partner_id',
        dag=adag,
        provide_context=True,
        python_callable=partner_dsr_handler.save_tenant_id_and_partner_id,
        retries=0
    )
)

write_to_s3_enriched = OpTask(
    op=PythonOperator(
        task_id='write_to_s3_enriched', dag=adag, provide_context=True, python_callable=partner_dsr_handler.write_to_s3_enriched, retries=0
    )
)

push_vertica_scrub_generator_logex_task = OpTask(
    op=PythonOperator(
        task_id='push_vertica_scrub_generator_logex_task',
        dag=adag,
        provide_context=True,
        python_callable=VerticaScrubGeneratorTaskPusher(create_generator_tasks_partner_dsr).push_vertica_scrub_generator_logex_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
)

logex_vertica_scrub_monitoring_tasks = [
    (
        OpTask(
            op=PythonSensor(
                task_id=f"monitor_logex_vertica_scrub_generator_for_{cleanse_table_name(table_config)}",
                python_callable=monitor_logex_task.monitor_logex_task,
                poke_interval=10 * 60,  # 10 minutes
                timeout=48 * 60 * 60,  # 48 hours
                templates_dict={
                    'table_name': table_name,
                    'logex_task': monitor_logex_task.LogExTask.VerticaScrubGenerator
                },
                mode='reschedule',
                dag=adag,
            )
        ),
        OpTask(
            op=PythonSensor(
                task_id=f"monitor_all_logex_vertica_scrubs_for_{cleanse_table_name(table_config)}",
                python_callable=monitor_logex_task.monitor_logex_task,
                poke_interval=10 * 60,  # 10 minutes
                timeout=48 * 60 * 60,  # 48 hours
                templates_dict={
                    'table_name': table_name,
                    'logex_task': monitor_logex_task.LogExTask.VerticaScrub
                },
                mode='reschedule',
                dag=adag,
            )
        )
    ) for table_name, table_config in
    (PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_ADVERTISER | PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_MERCHANT).items()
]

scrub_aws_parquet_datasets_tasks = \
    AWSParquetDeleteOperation(
        cluster_name="datagov-partner-dsr-delete",
        job_class_name=job_class_name,
        dataset_configs=PARTNER_DSR_DATASET_CONFIG['AWS'],
        additional_args={
            'isPartnerDsr': 'true',
        }
    ).create_parquet_delete_job_tasks()

# scrub_azure_parquet_datasets_tasks = \
#     AzureParquetDeleteOperation(
#         cluster_name="datagov-partner-dsr-delete",
#         job_class_name=job_class_name,
#         dataset_configs=PARTNER_DSR_DATASET_CONFIG['AZURE'],
#         additional_args={
#             'azure.key': 'eastusttdlogs,ttddatasubjectrequests',
#             'openlineage.enable': 'false',
#         }
#     ).create_parquet_delete_job_tasks() \
#         if is_prod() else [
#         OpTask(op=DummyOperator(
#             task_id='scrub_azure_parquet_dataset_dummyop',
#             dag=adag
#         ))
#     ]

dag >> process_partner_dsr_requests_ready_for_cloud_processing

process_partner_dsr_requests_ready_for_cloud_processing \
    >> save_tenant_id_and_partner_id \
    >> write_to_s3_enriched

process_partner_dsr_requests_ready_for_cloud_processing \
    >> push_vertica_scrub_generator_logex_task

for monitor_scrub_gen_task, monitor_scrub_task in logex_vertica_scrub_monitoring_tasks:
    push_vertica_scrub_generator_logex_task \
        >> monitor_scrub_gen_task \
        >> monitor_scrub_task

# add back ,[scrub_azure_parquet_datasets_tasks]:
for scrub_parquet_tasks in [scrub_aws_parquet_datasets_tasks]:
    for scrub_parquet_task in scrub_parquet_tasks:
        process_partner_dsr_requests_ready_for_cloud_processing \
            >> scrub_parquet_task
