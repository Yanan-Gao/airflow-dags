import logging
import time
from datetime import timedelta, datetime, timezone

from airflow.operators.python import ShortCircuitOperator

from dags.forecast.avails_coldstorage_lookup.constants import DAG_NAME
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from airflow.utils.trigger_rule import TriggerRule

from ttd.tasks.op import OpTask


def _check_data_existence(**kwargs):
    """
    Checks for the existence of a specific Sampled Avails (hour) partition in AWS.

    Returns:
        True if Sampled Avails Dataset has been generated for the hour requested
        False otherwise
    """
    dag_name = kwargs['dag_name']
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    iteration = kwargs['templates_dict']['iteration_number']
    check_hour = datetime.strptime(
        kwargs['task_instance'].xcom_pull(dag_id=dag_name, task_ids='initialize_run_hour', key=f'data_date_{iteration}'),
        '%Y-%m-%dT%H:00:00'
    )
    logging.info(f'Check data of {check_hour}')
    # avails should already exist, but check anyway
    now_time = datetime.now(timezone.utc) - timedelta(hours=1)
    logging.info(f'Now time is  {now_time}')

    if now_time.timestamp() > check_hour.timestamp():
        logging.info(f'Yes, the current time is later than {check_hour}')
    else:
        time_diff = check_hour.timestamp() - now_time.timestamp()
        logging.info(f"It's {time_diff} seconds earlier than {check_hour}")
        logging.info(f'Sleep {time_diff} seconds')
        time.sleep(time_diff)
    next_hour = check_hour + timedelta(hours=1)
    logging.info(f"Check if next hour's avail is processing: {next_hour}")
    max_check_iteration = 15
    check_iteration = 0
    while check_iteration < max_check_iteration:
        logging.info(f'Check iteration {check_iteration} :')
        if not Datasources.coldstorage.avails_sampled.with_check_type('hour').check_data_exist(cloud_storage, next_hour):
            logging.info(f'Data of next hour {next_hour} is not being generated')
            # Usually, there'll be 5min delay for last hour's avail data to be all done
            logging.info('Wait for another minute')
            time.sleep(60)
            check_iteration += 1
        else:
            logging.info(f'Data of next hour {next_hour} is being generated')
            logging.info(f'Data of check hour {check_hour} should have all been generated')
            return True
    logging.info('Exceed max check iteration')
    return False


class DataExistenceCheckTask(OpTask):
    """
    Class to encapsulate an OpTask creation for Avails ColdStorage sync.

    More info: https://atlassian.thetradedesk.com/confluence/display/EN/Universal+Forecasting+Walmart+Data+Sovereignty
    """

    def __init__(self, iteration_number):
        super().__init__(
            op=ShortCircuitOperator(
                provide_context=True,
                python_callable=_check_data_existence,
                task_id=f'hourly_data_check_{iteration_number}',
                templates_dict={'iteration_number': iteration_number},
                trigger_rule=TriggerRule.ALL_DONE,
                op_kwargs={'dag_name': DAG_NAME}
            )
        )
