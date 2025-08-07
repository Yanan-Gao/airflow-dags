from typing import TYPE_CHECKING, Optional, Sequence, List

from ttd.eldorado.base import ElDoradoError
from ttd.tasks.base import BaseTask
from ttd.tasks.op import OpTask
from ttd.tasks.task_group_retry_mixin import TaskGroupRetryMixin

if TYPE_CHECKING:
    from airflow.models import BaseOperator

    from ttd.eldorado.base import TtdDag
    from ttd.eldorado.visitor import AbstractVisitor


class ChainOfTasks(BaseTask, TaskGroupRetryMixin):

    def __init__(self, task_id: str, tasks: List[BaseTask]):
        super().__init__(task_id)
        if not tasks:
            raise ElDoradoError("You must provide some tasks")
        self._tasks = tasks
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]
        TaskGroupRetryMixin.__init__(self)

    def _adopt_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        self.first._set_ttd_dag(ttd_dag)

    def first_airflow_op(self) -> Optional["BaseOperator"]:
        return self.first.first_airflow_op()

    def last_airflow_op(self) -> Optional["BaseOperator"]:
        return self.last.last_airflow_op()

    @property
    def _child_tasks(self) -> Sequence["BaseTask"]:
        return self._tasks

    @property
    def first(self) -> "BaseTask":
        return self._tasks[0]

    @property
    def last(self) -> "BaseTask":
        return self._tasks[-1]

    def _attach_retry_task(self, retry_task: OpTask):
        self.last >> retry_task
        self._tasks.append(retry_task)

    def accept(self, visitor: "AbstractVisitor") -> None:
        visitor.visit_chain_of_tasks(self)
