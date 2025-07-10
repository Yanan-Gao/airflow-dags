from abc import abstractmethod
from datetime import timedelta
from typing import TypeVar, Type, Dict, Any, Optional

from ttd.operators.task_group_retry_operator import TaskGroupRetryOperator
from ttd.tasks.op import OpTask
from ttd.tasks.base import BaseTask

T = TypeVar("T", bound=BaseTask)


class TaskGroupRetryMixin:
    """
    Allows tasks that have been marked as taskgroups to have retry operators attached to them.
    """

    def __init__(self):
        self.task_group_retry_task: Optional["OpTask"] = None

    @abstractmethod
    def _attach_retry_task(self, retry_task: OpTask):
        pass

    def with_retry_op(
        self: T,
        retries: int = 0,
        retry_delay: timedelta = timedelta(minutes=1),
        retry_exponential_backoff: bool = False,
        custom_retry_op: Type[TaskGroupRetryOperator] = None,
        retry_op_kwargs_override: Dict[str, Any] = None,
        always_attach: bool = False,
    ) -> T:
        """
        Sets the current task to be wrapped in a TaskGroup.

        :param group_id: ID of the TaskGroup
        :param retries: Number of retries to attempt
        :param retry_delay: Interval between retries
        :param retry_exponential_backoff: Whether to use exponential backoff
        :param custom_retry_op: Custom operators can be provided that extend the base functionality of TaskGroupRetryOperator.
        :param retry_op_kwargs_override: Optional keyword arguments to pass to the retry operator.
        :param always_attach: Whether to attach a retry operator to the end. By default, if retries is set to 0,
        an operator will not be attached.
        """
        if retries == 0 and always_attach is False:
            return self

        if self.group_id is None:
            raise NotATaskGroupException("Task must have been defined to be a TaskGroup")

        retry_op_class = custom_retry_op or TaskGroupRetryOperator
        retry_op_kwargs = {
            "task_id": f"retry_{self.group_id}",
            "task_group_id": self.group_id,
            "retries": retries,
            "retry_delay": retry_delay,
            "retry_exponential_backoff": retry_exponential_backoff,
        }
        if retry_op_kwargs_override is not None:
            retry_op_kwargs = {**retry_op_kwargs, **retry_op_kwargs_override}

        op = retry_op_class(**retry_op_kwargs)
        task = OpTask(op=op)

        self._attach_retry_task(task)
        self.task_group_retry_task = task

        return self


class NotATaskGroupException(Exception):
    pass
