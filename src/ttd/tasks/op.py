from typing import Optional, List, TYPE_CHECKING

from ttd.eldorado.base import ElDoradoError
from ttd.tasks.base import BaseTask

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from ttd.eldorado.visitor import AbstractVisitor
    from ttd.eldorado.base import TtdDag


class OpTask(BaseTask):

    def __init__(self, task_id: str = None, op: Optional["BaseOperator"] = None):
        super().__init__(task_id)
        self._op = op
        if op and not self._task_id:
            self._task_id = op.task_id
        self.parent_groups = []

    def first_airflow_op(self) -> Optional["BaseOperator"]:
        return self._op

    def last_airflow_op(self) -> Optional["BaseOperator"]:
        return self._op

    @property
    def _child_tasks(self) -> List["BaseTask"]:
        return []

    def set_op(self, op: "BaseOperator") -> None:
        if not op:
            raise ElDoradoError("op must not be None")

        if self._op and self._op != op:
            raise ElDoradoError("op is set")

        if self._op == op:
            return

        self._op = op
        if not self._task_id:
            self._task_id = op.task_id

        if self._ttd_dag:
            self._op.dag = self._ttd_dag.airflow_dag

            for u in self._upstream:
                if u.last_airflow_op():
                    u.last_airflow_op() >> self.first_airflow_op()

            for d in self._downstream:
                if d.first_airflow_op():
                    self.last_airflow_op() >> d.first_airflow_op()

    def _adopt_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        if self._op:
            if self._is_setup:
                self._op.as_setup()
            if self._is_teardown:
                self._op.as_teardown()
            if self._parent_group is not None:
                self._op.task_id = self._parent_group.child_id(self._op.task_id)
                self._op.task_group = self._parent_group
                self._parent_group.add(self._op)

            adag = self._ttd_dag.airflow_dag
            self._op.dag = adag
            if "owner" in adag.default_args:
                self._op.owner = adag.default_args["owner"]

    def accept(self, visitor: "AbstractVisitor") -> None:
        visitor.visit_optask(self)
