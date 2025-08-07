import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

from dags.pdg.data_subject_request.config.vertica_config import cleanse_table_name, DSR_VERTICA_TABLE_CONFIGURATIONS
from dags.pdg.data_subject_request.handler import monitor_logex_task
from dags.pdg.data_subject_request.handler.vertica_scrub_generator_creator import VerticaScrubGeneratorTaskPusher, \
    create_generator_tasks_dsr
from dags.pdg.data_subject_request.operators.parquet_delete_operators import DSR_DELETE_DATASET_CONFIG
from dags.pdg.data_subject_request.operators.parquet_delete_operators.aws import AWSParquetDeleteOperation
from dags.pdg.data_subject_request.operators.parquet_delete_operators.shared import XCOM_PULL_STR, DSR
from dags.pdg.data_subject_request.util import dsr_dynamodb_util, jira_util, uid2_util, euid_util, s3_utils, sql_util, \
    opt_out_util, openpass_util, fast_check_util, ses_util, aerospike_util, phone_number_util, serialization_util, \
    provdb_util
from dags.pdg.data_subject_request.util.dsr_dynamodb_util import PROCESSING_STATE_ATTRIBUTE_NAME
from dags.pdg.data_subject_request.util.forecasting_data import create_forecasting_data_task_group
from dags.pdg.data_subject_request.util.uid2_client_util import uid2_id_mapping
from dags.pdg.data_subject_request.util.prodtest_util import get_parquet_operations, is_prodtest, \
    is_dag_operation_enabled, get_prodtest_uiids
from dags.pdg.data_subject_request.util.forecasting_data import forecasting_task_name
from dags.pdg.data_subject_request.dsr_item import DsrItem
from dags.pdg.data_subject_request.identity import DsrIdentityType
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge
"""
This DAG is responsible for receiving emails as inputs and deleting the appropriate UID2 and TDID
from the relevant data sources that are storing information corresponding to that email.  We will
also be opting the email's UID2 and TDID out of our systems (hot cache and cold storage)
"""

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

###########################################
#   Job Configs
###########################################

job_name = 'data-governance-dsr-delete-automation'
slack_channel = '#scrum-pdg-alerts'
job_start_date = datetime(2024, 7, 3)
job_schedule_interval = '0 0 * * 2'
pdg_jira_team = "PDG"
job_type = "UserDsr"
scala_job_class_name = "com.thetradedesk.dsr.delete.ParquetUID2DeleteJob"


def persist_dag_failed(context):
    if is_prodtest and not is_dag_operation_enabled():
        return

    request_id = context['task_instance'].xcom_pull(key='request_id')
    dsr_dynamodb_util.persist_dag_ended('failed', request_id, context['run_id'])


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
    tags=["Data Governance", pdg_jira_team, job_type],
    on_failure_callback=persist_dag_failed,
    dagrun_timeout=timedelta(hours=72),
)

adag = dag.airflow_dag


def update_task_success(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    request_id = task_instance.xcom_pull(key='request_id')
    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)

    for dsr_item in dsr_items:
        if is_prod:
            dsr_dynamodb_util.update_items_attribute(request_id, dsr_item.email_sort_key, task_id, True)
            if dsr_item.phone_sort_key:
                dsr_dynamodb_util.update_items_attribute(request_id, dsr_item.phone_sort_key, task_id, True)
        else:
            logging.info(
                f'Non prod environment. '
                f'In prod we would persist pk:{request_id}, email sk:{dsr_item.email_sort_key}, phone sk:{dsr_item.phone_sort_key} task_id:{task_id}'
            )


def _fast_check(task_instance: TaskInstance, **kwargs):
    if is_prodtest and not is_dag_operation_enabled():
        return True

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    request_id = task_instance.xcom_pull(key='request_id')

    dsr_items = fast_check_util.fast_check(dsr_items, jira_util.jira_in_progress_status)

    if not dsr_items:
        return False

    jira_ticket_numbers = [dsr_item.jira_ticket_number for dsr_item in dsr_items]
    item = dsr_dynamodb_util.build_single_attribute_data(
        request_id, kwargs['dag_run'].run_id, PROCESSING_STATE_ATTRIBUTE_NAME, 'processing'
    )
    item['jira_tickets'] = jira_ticket_numbers
    if is_prod:
        dsr_dynamodb_util.put_item(item)
    else:
        logging.info(f'Item to have been persisted in prod - Item:{item}')

    task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
    return True


def _verify_optouts(task_instance: TaskInstance, **kwargs) -> bool:
    if is_prodtest and not is_dag_operation_enabled():
        return True

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)

    aerospike_client = aerospike_util.get_aerospike_client()

    try:
        for dsr_item in dsr_items:
            for guid in dsr_item.guids:
                user_metadata = aerospike_util.get_user_metadata(aerospike_client, guid)
                if not user_metadata:
                    logging.info(f'Unable to look up metadata for item: {dsr_item.jira_ticket_number}')
                    continue
                # If any IDs aren't opted out, return false and reschedule the task.
                if not user_metadata['UserOptOut']:
                    return False
    finally:
        aerospike_client.close()

    return True


def _cross_device_vendor_check():
    if not is_prod:
        return True
    """
    Query provisioning for the number of cross device vendor changes and emit as a metric.
    This will be used to alert if cross device vendors have changed so we can evalute their place in DSDR scrubbing.
    """
    cross_device_vendors_changed_count: TtdGauge = get_or_register_gauge(
        job=job_name,
        name="cross_device_vendors_changed",
        description="DSDR - Cross Device Vendors changed that require scrubbing from graphs",
    )

    count = provdb_util.get_cross_device_vendors_changed_count()
    logging.info(f"cross device vendors changed count: {count}")

    cross_device_vendors_changed_count.set(count)
    push_all(job=job_name)

    return True


def _poll_jira_for_delete_requests(task_instance: TaskInstance, **kwargs):
    if is_prodtest and not is_dag_operation_enabled():
        return True

    dsr_items = jira_util.read_jira_dsr_queue(jira_util.REQUEST_TYPE_DELETE)
    if not dsr_items:
        logging.info('No items to process.')
        return False

    dsr_items = phone_number_util.validate_dsr_request_phones(dsr_items)
    if not dsr_items:
        logging.info('No items to process after phone validation')
        return False

    task_instance.xcom_push(key='request_id', value=str(uuid.uuid4()))
    task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
    return True


def _emails_to_uid2s_to_tdids(task_instance: TaskInstance):
    # For prodtest, only use the IDs provided in the config variable.
    if is_prodtest:
        task_instance.xcom_push(key='uiids_string', value=get_prodtest_uiids())
        return

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    request_id = task_instance.xcom_pull(key='request_id')
    task_id = task_instance.task_id

    identity_client = uid2_util.create_client()
    uiids_string = uid2_id_mapping(dsr_items, identity_client, DsrIdentityType.UID2, request_id)

    db_items: List[Dict[str, str]] = []
    for dsr_item in dsr_items:
        uid2s = dsr_item.uid2_identities
        if not uid2s:
            continue

        for uid2 in uid2s:
            db_item = dsr_dynamodb_util.build_single_attribute_data(request_id, uid2.sort_key, task_id, True)
            db_item['uid2_salt_bucket'] = uid2.salt_bucket
            db_items.append(db_item)

    dsr_dynamodb_util.write_db_records(db_items)

    task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
    task_instance.xcom_push(key='uiids_string', value=uiids_string)


def _emails_to_euids_to_tdids(task_instance: TaskInstance):
    # Exit here for prodtest because we already pulled all of the IDs in the UID2s step above
    if is_prodtest:
        uiids_string = task_instance.xcom_pull(key='uiids_string') or ''
        task_instance.xcom_push(key='uiids_string', value=uiids_string)
        return True

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    request_id = task_instance.xcom_pull(key='request_id')
    task_id = task_instance.task_id

    prev_uiids_string = task_instance.xcom_pull(key='uiids_string') or ''

    identity_client = euid_util.create_client()
    uiids_string = uid2_id_mapping(dsr_items, identity_client, DsrIdentityType.EUID, request_id)

    if not prev_uiids_string and not uiids_string:
        return False

    # The dynamo items were created in the previous step, so we'll update them here
    for dsr_item in dsr_items:
        euids = dsr_item.euid_identities
        if not euids:
            continue

        for euid in euids:
            dsr_dynamodb_util.do_update_item_attributes(
                request_id,
                euid.sort_key,
                attributes={
                    f'{task_id}': True,
                    'euid_tdid': f"{euid.identifier}#{euid.tdid}",
                    'euid_salt_bucket': euid.salt_bucket
                }
            )

    uiids_string = f'{prev_uiids_string},{uiids_string}'

    task_instance.xcom_push(key='dsr_items', value=serialization_util.serialize_list(dsr_items))
    task_instance.xcom_push(key='uiids_string', value=uiids_string)

    return True


cross_device_vendor_check = OpTask(
    op=ShortCircuitOperator(
        task_id='cross_device_vendor_check',
        dag=adag,
        retry_exponential_backoff=False,
        provide_context=True,
        python_callable=_cross_device_vendor_check
    )
)

poll_jira_epic_for_emails_to_process = OpTask(
    op=ShortCircuitOperator(
        task_id='poll_jira_epic_for_emails_to_process',
        dag=adag,
        retry_exponential_backoff=True,
        provide_context=True,
        python_callable=_poll_jira_for_delete_requests
    )
)

emails_to_uid2s_to_tdids = OpTask(
    op=PythonOperator(
        task_id='emails_to_uid2s_to_tdids',
        dag=adag,
        provide_context=True,
        python_callable=_emails_to_uid2s_to_tdids,
        retry_exponential_backoff=True
    )
)

emails_to_euids_to_tdids = OpTask(
    op=ShortCircuitOperator(
        task_id='emails_to_euids_to_tdids',
        dag=adag,
        provide_context=True,
        python_callable=_emails_to_euids_to_tdids,
        retry_exponential_backoff=True
    )
)

fast_check = OpTask(
    op=ShortCircuitOperator(
        task_id='fast_check', dag=adag, provide_context=True, python_callable=_fast_check, retry_exponential_backoff=False, retries=3
    )
)

post_dsr_notification_to_s3 = OpTask(
    op=PythonOperator(
        task_id='post_dsr_notification_to_s3', dag=adag, provide_context=True, python_callable=s3_utils.post_dsr_notification_to_s3
    )
)

verify_optouts = OpTask(
    op=PythonSensor(
        task_id='verify_optouts',
        dag=adag,
        mode='reschedule',
        python_callable=_verify_optouts,
        timeout=60 * 60 * 24,  # Timeout in seconds (24 hours)
        poke_interval=60 * 60,  # 1 hour
    )
)

persist_to_ttdglobal_deleterequest = OpTask(
    op=PythonOperator(
        task_id='persist_to_ttdglobal_deleterequest',
        dag=adag,
        provide_context=True,
        retries=0,
        python_callable=sql_util.persist_to_ttdglobal_deleterequest,
        on_success_callback=update_task_success
    )
)

opt_out_hotcache_and_coldstorage = OpTask(
    op=PythonOperator(
        task_id='opt_out_hotcache_and_coldstorage',
        dag=adag,
        provide_context=True,
        python_callable=opt_out_util.opt_out,
        retry_exponential_backoff=True,
        retries=3
    )
)

init_openpass_delete = OpTask(
    op=PythonOperator(task_id='init_openpass_delete', dag=adag, provide_context=True, python_callable=openpass_util.init_openpass_delete)
)

monitor_openpass_delete = OpTask(
    op=PythonSensor(
        task_id='monitor_openpass_delete_task',
        python_callable=openpass_util.monitor_openpass_delete,
        timeout=60 * 60 * 2,  # Timeout in seconds (2 hours)
        poke_interval=60 * 10,  # 10 minutes
        mode='reschedule',
        on_success_callback=update_task_success,
        on_failure_callback=openpass_util.process_openpass_failure,
        dag=adag,
    )
)


def persist_dag_success(task_instance: TaskInstance):
    if is_prodtest and not is_dag_operation_enabled():
        return

    request_id = task_instance.xcom_pull(key='request_id')
    dsr_dynamodb_util.persist_dag_ended('completed', request_id, task_instance.run_id)

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    for dsr_item in dsr_items:
        jira_util.transition_jira_status([dsr_item.jira_ticket_number], jira_util.jira_done_status)
        ses_util.send_delete_confirmation(dsr_item.email)


persist_dag_completion_success = OpTask(
    op=PythonOperator(
        task_id='persist_dag_completion_success',
        dag=adag,
        provide_context=True,
        python_callable=persist_dag_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
)

dag >> cross_device_vendor_check >> poll_jira_epic_for_emails_to_process >> emails_to_uid2s_to_tdids >> emails_to_euids_to_tdids >> fast_check \
    >> post_dsr_notification_to_s3 >> opt_out_hotcache_and_coldstorage >> persist_to_ttdglobal_deleterequest >> verify_optouts \
    >> init_openpass_delete >> monitor_openpass_delete >> persist_dag_completion_success

if is_prod:
    forecasting_data_task_group = create_forecasting_data_task_group(
        XCOM_PULL_STR.format(dag_id=job_name, task_id=emails_to_euids_to_tdids.task_id, key='uiids_string')
    )

    push_vertica_scrub_generator_logex_task = OpTask(
        op=PythonOperator(
            task_id='push_vertica_scrub_generator_logex_task',
            dag=adag,
            provide_context=True,
            python_callable=VerticaScrubGeneratorTaskPusher(create_generator_tasks_dsr).push_vertica_scrub_generator_logex_task,
            trigger_rule=TriggerRule.ALL_SUCCESS
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
        ) for table_name, table_config in DSR_VERTICA_TABLE_CONFIGURATIONS.items()
    ]

    for monitor_scrub_generator_logex_task, monitor_scrub_logex_task in logex_vertica_scrub_monitoring_tasks:
        verify_optouts >> push_vertica_scrub_generator_logex_task \
            >> monitor_scrub_generator_logex_task >> monitor_scrub_logex_task \
            >> persist_dag_completion_success

    dsr_request_id = XCOM_PULL_STR.format(dag_id=job_name, task_id=poll_jira_epic_for_emails_to_process.task_id, key='request_id')
    mode = DSR(uiids=XCOM_PULL_STR.format(dag_id=job_name, task_id=emails_to_euids_to_tdids.task_id, key='uiids_string'))
    delete = AWSParquetDeleteOperation(
        dsr_request_id=dsr_request_id, job_class_name=scala_job_class_name, mode=mode, dataset_configs=DSR_DELETE_DATASET_CONFIG['AWS']
    )
    for task in delete.create_parquet_delete_job_tasks():
        verify_optouts >> task >> persist_dag_completion_success

    verify_optouts >> forecasting_data_task_group >> persist_dag_completion_success
elif is_prodtest:
    parquet_delete_operations = get_parquet_operations()
    filtered_configs = {k: DSR_DELETE_DATASET_CONFIG['AWS'][k] for k in parquet_delete_operations if k in DSR_DELETE_DATASET_CONFIG['AWS']}

    if forecasting_task_name in parquet_delete_operations:
        forecasting_data_task_group = create_forecasting_data_task_group(
            XCOM_PULL_STR.format(dag_id=job_name, task_id=emails_to_euids_to_tdids.task_id, key='uiids_string')
        )

    # Currently prodtest is only set up to run parquet scrubbing. The scrubbing is kicked off with dryRun set to true.
    dsr_request_id = XCOM_PULL_STR.format(dag_id=job_name, task_id=poll_jira_epic_for_emails_to_process.task_id, key='request_id')
    mode = DSR(uiids=XCOM_PULL_STR.format(dag_id=job_name, task_id=emails_to_euids_to_tdids.task_id, key='uiids_string'))
    delete = AWSParquetDeleteOperation(
        dsr_request_id=dsr_request_id, job_class_name=scala_job_class_name, mode=mode, dataset_configs=filtered_configs
    )
    for task in delete.create_parquet_delete_job_tasks():
        verify_optouts >> task >> persist_dag_completion_success

    if forecasting_task_name in parquet_delete_operations:
        verify_optouts >> forecasting_data_task_group >> persist_dag_completion_success
    else:
        verify_optouts >> persist_dag_completion_success
else:
    emr_tasks_placeholder = OpTask(op=EmptyOperator(task_id='emr_tasks_placeholder', dag=adag))
    verify_optouts >> emr_tasks_placeholder >> persist_dag_completion_success
