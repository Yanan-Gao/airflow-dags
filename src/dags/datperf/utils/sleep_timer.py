from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
from airflow.utils.decorators import apply_defaults


class SleepTimer(BaseSensorOperator):

    @apply_defaults
    def __init__(self, duration_mins, *args, **kwargs):
        super(SleepTimer, self).__init__(*args, **kwargs)
        self.duration_mins = duration_mins

    def poke(self, context):
        ti = context['ti']
        time_to_wake = datetime.utcnow() + timedelta(minutes=self.duration_mins)
        print(f"Setting time_to_wake to {time_to_wake}")
        ti.xcom_push(key='time_to_wake', value=time_to_wake)
        return True
