from __future__ import annotations
import abc
from typing import Optional, List, TYPE_CHECKING, Union, Self

from airflow.utils.task_group import TaskGroup

from ttd.eldorado.base import ElDoradoError
from ttd.eldorado.visitor import AbstractVisitor

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from ttd.eldorado.base import TtdDag


class BaseTask(abc.ABC):

    def __init__(self, task_id: str = None):
        self._task_id = task_id
        self._ttd_dag: Optional[TtdDag] = None
        self._upstream: List[BaseTask] = []
        self._downstream: List[BaseTask] = []
        self._group_id: Optional[str] = None

        self._is_setup: bool = False
        self._is_teardown: bool = False
        self._parent_group: Optional[TaskGroup] = None

    @abc.abstractmethod
    def first_airflow_op(self) -> Union[Optional["BaseOperator"], List["BaseOperator"]]:
        pass

    @abc.abstractmethod
    def last_airflow_op(self) -> Union[Optional["BaseOperator"], List["BaseOperator"]]:
        pass

    @property
    def task_id(self) -> Optional[str]:
        return self._task_id

    @property
    def upstream(self) -> List["BaseTask"]:
        return [x for x in self._upstream]

    @property
    def downstream(self) -> List["BaseTask"]:
        return [x for x in self._downstream]

    @property
    def group_id(self) -> Optional[str]:
        return self._group_id

    @property
    @abc.abstractmethod
    def _child_tasks(self) -> List["BaseTask"]:
        pass

    def as_setup(self) -> Self:
        self._is_setup = True
        for task in self._child_tasks:
            task.as_setup()
        return self

    def as_teardown(self) -> Self:
        self._is_teardown = True
        for task in self._child_tasks:
            task.as_teardown()
        return self

    def as_taskgroup(self, group_id) -> Self:
        self._group_id = group_id
        return self

    def _set_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        self._validate_ttd_dag(ttd_dag)

        if self._ttd_dag == ttd_dag:
            return

        self._ttd_dag = ttd_dag

        self._adopt_task_group()

        self._adopt_ttd_dag(ttd_dag)

        self._populate_ttd_dag_downstream(ttd_dag)

    @abc.abstractmethod
    def _adopt_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        pass

    def _populate_ttd_dag_downstream(self, ttd_dag: "TtdDag") -> None:
        for d in self._downstream:
            d._set_ttd_dag(ttd_dag)

            if self.last_airflow_op() and d.first_airflow_op():
                self.last_airflow_op() >> d.first_airflow_op()

    def _validate_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        if not ttd_dag:
            raise ElDoradoError("ttd_dag must not be None")
        if self._ttd_dag and self._ttd_dag != ttd_dag:
            raise ElDoradoError("ttd_dag is set")

    def set_downstream(self, others: BaseTask | list[BaseTask]):
        if not isinstance(others, list):
            others = [others]

        for other in others:
            if other not in self._downstream:
                self._downstream.append(other)

            if self not in other._upstream:
                other._upstream.append(self)

            if self._ttd_dag:
                other._set_ttd_dag(self._ttd_dag)

                if self.last_airflow_op() and other.first_airflow_op():
                    self.last_airflow_op() >> other.first_airflow_op()

    def __rshift__(self, others: BaseTask | list[BaseTask]) -> BaseTask | list[BaseTask]:
        self.set_downstream(others)
        return others

    def __rrshift__(self, other):
        if isinstance(other, list):
            for task in other:
                task >> self
        else:
            raise NotImplementedError
        return self

    def accept(self, visitor: "AbstractVisitor") -> None:
        visitor.visit_base_task(self)

    def _adopt_task_group(self) -> None:
        if self.group_id is not None:
            group = TaskGroup(
                group_id=self.group_id,
                dag=self._ttd_dag.airflow_dag,
                parent_group=self._parent_group,
                prefix_group_id=False,
            )
            self._register_children_with_parent_group(group)

    def _register_children_with_parent_group(self, group: TaskGroup) -> None:
        for task in self._child_tasks:
            task._parent_group = group
            task._register_children_with_parent_group(group)
