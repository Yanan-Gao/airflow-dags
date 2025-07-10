from airflow.operators.python import PythonOperator

from ttd.tasks.op import OpTask


class PythonTask(OpTask):

    def __init__(self, task_id, python_callable):
        super().__init__(op=PythonOperator(task_id=task_id, python_callable=python_callable, dag=None))


class MessageTask(PythonTask):

    def __init__(self, task_id):
        super().__init__(task_id=task_id, python_callable=lambda **kwargs: print(f"Task: {task_id}"))
