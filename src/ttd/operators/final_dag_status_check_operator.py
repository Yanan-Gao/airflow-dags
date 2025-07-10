from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State


class FinalDagStatusCheckOperator(PythonOperator):
    """
    Final status check to ensure that all tasks have completed successfully (not failed)
    Iterates through all dag tasks from the current dag run and fails the dag if any steps were unsuccessful
    """

    def __init__(self, dag: DAG, name: str = "final_dag_status", trigger_rule: str = TriggerRule.ALL_DONE, **kwargs):
        super(FinalDagStatusCheckOperator, self).__init__(
            task_id=name, python_callable=self.check_final_status, trigger_rule=trigger_rule, dag=dag, **kwargs
        )

    @staticmethod
    def check_final_status(**kwargs):
        for task_instance in kwargs["dag_run"].get_task_instances():
            if (task_instance.current_state() == State.FAILED and task_instance.task_id != kwargs["task_instance"].task_id):
                raise Exception(
                    "Task {task_id} has failed. Entire DAG run will be reported as a failure".format(task_id=task_instance.task_id)
                )
