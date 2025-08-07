from datetime import datetime

from airflow.operators.python import PythonOperator

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY, \
    RAM_GENERATION_ISO_WEEKDAY_KEY
from ttd.tasks.op import OpTask


def set_timestamps_and_iso_weekday(**context):
    ram_generation_timestamp = context['templates_dict']['ds_nodash']
    print(f'Setting {RAM_GENERATION_TIMESTAMP_KEY} to {ram_generation_timestamp}')
    context["task_instance"].xcom_push(key=RAM_GENERATION_TIMESTAMP_KEY, value=ram_generation_timestamp)

    ram_generation_iso_weekday = str(datetime.strptime(context['templates_dict']['ds'], '%Y-%m-%d').isoweekday())
    print(f'Setting {RAM_GENERATION_ISO_WEEKDAY_KEY} to {ram_generation_iso_weekday}')
    context["task_instance"].xcom_push(key=RAM_GENERATION_ISO_WEEKDAY_KEY, value=ram_generation_iso_weekday)
    return True


class SetTimestampsAndIsoWeekday(OpTask):

    def __init__(self, dag):
        super().__init__(
            op=PythonOperator(
                dag=dag,
                task_id="set_timestamps_and_iso_weekday",
                python_callable=set_timestamps_and_iso_weekday,
                provide_context=True,
                templates_dict={
                    'ds_nodash': '{{ ds_nodash }}',
                    'ds': '{{ ds }}'
                }
            )
        )
