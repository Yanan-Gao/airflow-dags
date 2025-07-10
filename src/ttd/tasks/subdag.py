import copy
from typing import Optional, List

from airflow.models import DagBag
from airflow.operators.subdag import SubDagOperator

from ttd.eldorado.base import TtdDag, ElDoradoError
from ttd.tasks.base import BaseTask
from ttd.tasks.op import OpTask
from ttd.eldorado.visitor import AbstractVisitor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator


class SubDagTaskDeprecated(OpTask):
    """
    THIS IS A DEPRECATED FEATURE OF AIRFLOW
    The logic of this operator has changed in Airflow 2, deferring behaviour that most of DAGs rely on.
    This version of SubDagTask mitigates some of the problems such as automatic restarts in case of failures,
    but manual restarts in the UI might come as tricky.
    Please upgrade your DAGs to use TaskGroups instead as soon as you can.

    A SubDAG Task

    :param: task_id
    :type: str

    :param: subtask
    :type: Optional[BaseTask]

    :param: keep_state_on_retry
    :type: bool

    Additional keyword params see TtdDag
    """

    separator = "."

    def __init__(self, task_id: str, subtask: Optional[BaseTask] = None, keep_state_on_retry: bool = False, **subdag_params):
        super().__init__(task_id=task_id, op=None)
        self._sub_downstream: List[BaseTask] = []
        self._ttd_subdag: Optional[TtdDag] = None
        self._keep_state_on_retry = keep_state_on_retry
        self._subdag_params = subdag_params
        if subtask:
            self.add_subtask(subtask)

    def add_subtask(self, subtask: BaseTask) -> "SubDagTaskDeprecated":
        if not subtask:
            raise ElDoradoError("subtask must not be None")

        if subtask in self._sub_downstream:
            return self

        self._sub_downstream.append(subtask)

        if self._ttd_subdag:
            subtask._set_ttd_dag(self._ttd_subdag)

        return self

    def _adopt_ttd_dag(self, ttd_dag: TtdDag) -> None:
        subdag_spec = copy.deepcopy(ttd_dag._dag_spec)
        subdag_spec["dag_id"] += self.separator + self._task_id
        self._ttd_subdag = TtdDag(**subdag_spec)
        globals()[self._ttd_subdag.airflow_dag.dag_id] = self._ttd_subdag.airflow_dag

        airflow_subdag = self._ttd_subdag.airflow_dag

        def on_retry_callback(
            context,
            keep_state_on_retry=self._keep_state_on_retry,
            airflow_subdag=airflow_subdag,
        ):
            if keep_state_on_retry:
                # Don't clear the state on retry. Useful when creating sub-dags inside sub-dags, to
                # avoid the top-level sub-dag from clearing the state of already successfully run
                # SubDagOperator tasks.
                return

            target_dag = DagBag().get_dag(airflow_subdag.dag_id)
            target_dag.clear(
                start_date=context["execution_date"],
                end_date=context["execution_date"],
                # https://issues.apache.org/jira/browse/AIRFLOW-351 Failed to clear downstream tasks
                # The below avoids the deep_copy() in sub_dag() that causes "TypeError: can't pickle _thread.RLock objects"
                include_parentdag=False,
            )

        self.set_op(
            SubDagOperator(
                subdag=airflow_subdag,
                task_id=self._task_id,
                dag=self._ttd_dag.airflow_dag,
                # Retries of this task will be blocked with "some task instances failed" unless the
                # state of the sub-dag's tasks is cleared. This is executed at the end of the failed
                # task instance execution, i.e. when the task's state is changed to retry.
                on_retry_callback=on_retry_callback,
                **self._subdag_params
            )
        )
        final_subdag_check = OpTask(op=FinalDagStatusCheckOperator(dag=airflow_subdag, name="final_subdag_status"))

        for s in self._sub_downstream:
            s._set_ttd_dag(self._ttd_subdag)
            s >> final_subdag_check

    def accept(self, visitor: AbstractVisitor) -> None:
        visitor.visit_subdag(self)
