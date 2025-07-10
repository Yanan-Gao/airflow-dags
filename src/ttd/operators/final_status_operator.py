from airflow import DAG
from airflow.models import BaseOperator, DagRun
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from ttd.operators.operator_mixins import OperatorMixins


class FinalDagStatusOperator(OperatorMixins, BaseOperator):

    def __init__(self, task_id, dag: DAG, *args, **kwargs):
        super().__init__(  # type: ignore
            task_id=task_id,
            trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
            dag=dag,
            *args,
            **kwargs)
        self.op_kwargs = {}  # type: ignore

    def execute(self, context):
        dag_run = context["dag_run"]
        has_any_failed = self.check_for_step_errors(dag_run)

        self.log.info(has_any_failed)
        return has_any_failed

    def check_for_step_errors(self, dag_run: DagRun):
        from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator

        for op in self.upstream_tasks_until_type([EmrCreateJobFlowOperator.__name__]):
            task_instance = dag_run.get_task_instance(task_id=op.task_id)
            if task_instance.current_state() != State.SUCCESS:  # type: ignore
                self.log.info("Found Failed Task!")
                raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))  # type: ignore

        return True
