from datetime import datetime


def xcom_pull_from_template(dag_name, task_id, iteration_number):
    return f'{{{{ task_instance.xcom_pull(' \
           f'dag_id="{dag_name}", ' \
           f'task_ids="{task_id}", ' \
           f'key="data_date_{iteration_number}") }}}}'


def get_date_and_hour(datetime_str):
    return get_date_from_datetime_string(datetime_str), get_hour_from_datetime_string(datetime_str)


def get_hour_from_datetime_string(datetime_str):
    timestamp = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    return f'{timestamp.hour:02d}'


def get_date_from_datetime_string(datetime_str):
    timestamp = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")
    return f'{timestamp.year:04d}{timestamp.month:02d}{timestamp.day:02d}'
