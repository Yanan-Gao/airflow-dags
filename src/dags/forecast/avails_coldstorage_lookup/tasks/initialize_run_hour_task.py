from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.tasks.op import OpTask

_TASK_ID = 'initialize_run_hour'
_TEMPLATES_DICT = {'ts_nodash': '{{ ts_nodash }}'}


def _store_hours(**kwargs):
    """
    Stores a number of timestamps (hours) into Airflow via XCom.
    """
    job_execution_ts = kwargs['templates_dict']['ts_nodash']
    ts = datetime.strptime(job_execution_ts, '%Y%m%dT%H%M%S')  # Converting pendulum ts to datetime
    num_of_hours_to_process = kwargs['num_of_hours_to_process']
    for h in range(0, num_of_hours_to_process):
        tmp_hour = ts + timedelta(hours=h)
        kwargs['task_instance'].xcom_push(key=f'data_date_{h}', value=tmp_hour.strftime('%Y-%m-%dT%H:00:00'))


class InitializeRunHourTask(OpTask):
    """
    Class to encapsulate an OpTask creation for Avails ColdStorage sync.

    This OpTask will store in Airflow the timestamps that will be consumed by downstream tasks

    More info: https://atlassian.thetradedesk.com/confluence/display/EN/Universal+Forecasting+Walmart+Data+Sovereignty
    """

    def __init__(self, num_of_hours_to_process):
        super().__init__(
            op=PythonOperator(
                provide_context=True,
                python_callable=_store_hours,
                task_id=_TASK_ID,
                templates_dict=_TEMPLATES_DICT,
                op_kwargs={'num_of_hours_to_process': num_of_hours_to_process}
            )
        )
