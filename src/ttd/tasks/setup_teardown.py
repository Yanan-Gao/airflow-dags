from datetime import timedelta
from typing import Optional, List, TYPE_CHECKING, Type, Any, Dict

from ttd.eldorado.base import ElDoradoError
from ttd.eldorado.visitor import CollectOpTasksVisitor
from ttd.operators.task_group_retry_operator import TaskGroupRetryOperator
from ttd.tasks.base import BaseTask
from ttd.tasks.op import OpTask
from ttd.tasks.task_group_retry_mixin import TaskGroupRetryMixin

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from ttd.eldorado.base import TtdDag
    from ttd.eldorado.visitor import AbstractVisitor


class SetupTeardownTask(BaseTask, TaskGroupRetryMixin):

    def __init__(
        self,
        task_id,
        setup_task: BaseTask,
        teardown_task: BaseTask,
        task_group_id: str = None,
        body_task: Optional[BaseTask] = None,
        retries: int = 2,
        retry_delay: timedelta = timedelta(minutes=1),
        retry_exponential_backoff: bool = False,
        custom_retry_op: Type[TaskGroupRetryOperator] = None,
        retry_op_kwargs_override: Dict[str, Any] = None,
    ):
        """
        Creates a TTD task object that can handle setup and teardown of resources, in addition to intelligent handling
        of retries. Automatically creates a TaskGroup to encapsulate the children of this task.

        :param task_id: ID of this task. Also used to name the TaskGroup object created.
        :param setup_task: Task that initialise the resources.
        :param teardown_task: Task that removes the resources. In the event of failure, these will always be run.
        :param task_group_id: ID of the group that will appear in the UI. If not provided, we use the task_id.
        :param body_task: Task that does the work on the resources setup previously. Can be added after initialisation
        of this object.
        :param retries: How many times to retry.
        :param retry_delay: How long to wait between retries.
        :param retry_exponential_backoff: If true, double the retry delay between each successive retry.
        :param custom_retry_op: By default, we use TaskGroupRetryOperator with standard settings. However, this can be
        overriden here to allow custom retry behaviour by passing a class that inherits from TaskGroupRetryOperator
        :param retry_op_kwrags_override: If provided, this parameter will provide additional keyword arguments to the
        retry operator. We combine this with our defaults, preferring values found in the override when we have collisions
        """
        super().__init__(task_id)
        self._setup_tasks = setup_task.as_setup()
        self._teardown_tasks = teardown_task.as_teardown()
        self._body_tasks = []
        self._first_body_tasks = []
        TaskGroupRetryMixin.__init__(self)

        self.tasks = [teardown_task] + [setup_task]

        task_group_id = task_group_id if task_group_id is not None else task_id
        self.as_taskgroup(group_id=task_group_id).with_retry_op(
            retries=retries,
            retry_delay=retry_delay,
            retry_exponential_backoff=retry_exponential_backoff,
            custom_retry_op=custom_retry_op,
            retry_op_kwargs_override=retry_op_kwargs_override,
            always_attach=True,
        )

        if body_task:
            self.add_parallel_body_task(body_task)

    def add_parallel_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        return self._add_body_task(body_task)

    def add_sequential_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        return self._add_body_task(body_task, is_parallel=False)

    def add_leaf_task(self, leaf_task: BaseTask) -> "SetupTeardownTask":
        self._validate_body_task(leaf_task)
        self._setup_tasks >> leaf_task
        self.tasks.append(leaf_task)

        return self

    def _adopt_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        setup_descendants = CollectOpTasksVisitor.gather_op_tasks_from_node(self._setup_tasks)
        teardown_descendants = CollectOpTasksVisitor.gather_op_tasks_from_node(self._teardown_tasks)

        for setup_task in setup_descendants:
            for teardown_task in teardown_descendants:
                setup_task >> teardown_task

        self._setup_tasks._set_ttd_dag(ttd_dag)

    def first_airflow_op(self) -> Optional["BaseOperator"]:
        return self._setup_tasks.first_airflow_op()

    def last_airflow_op(self) -> Optional["BaseOperator"]:
        if self.task_group_retry_task is not None:
            return self.task_group_retry_task.last_airflow_op()
        else:
            return self._teardown_tasks.last_airflow_op()

    def _attach_retry_task(self, retry_task: OpTask):
        self._teardown_tasks >> retry_task
        self.tasks.append(retry_task)

    @property
    def _child_tasks(self) -> List["BaseTask"]:
        return self.tasks

    @property
    def first_body_tasks(self) -> List["BaseTask"]:
        return self._first_body_tasks

    def accept(self, visitor: "AbstractVisitor") -> None:
        visitor.visit_setup_teardown_task(self)

    def _add_body_task(self, body_task: BaseTask, is_parallel: bool = True) -> "SetupTeardownTask":
        self._validate_body_task(body_task)

        if body_task in self._body_tasks:
            return self

        if is_parallel or len(self._body_tasks) == 0:
            self._setup_tasks >> body_task
            self.first_body_tasks.append(body_task)
        else:
            self._body_tasks[-1] >> body_task

        body_task >> self._teardown_tasks

        self._body_tasks.append(body_task)
        self.tasks.append(body_task)

        return self

    def _validate_body_task(self, body_task: BaseTask) -> None:
        if not body_task:
            raise ElDoradoError("body_task must not be None")

        if not isinstance(body_task, BaseTask):
            raise ElDoradoError("expected BaseTask")
