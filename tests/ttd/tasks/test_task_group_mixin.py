import unittest
from datetime import timedelta
from unittest.mock import patch
from ttd.operators.task_group_retry_operator import TaskGroupRetryOperator
from ttd.tasks.op import OpTask
from ttd.tasks.base import BaseTask
from ttd.tasks.task_group_retry_mixin import TaskGroupRetryMixin


class MockTask(TaskGroupRetryMixin, BaseTask):

    def __init__(self):
        super().__init__()
        self._group_id = None

    def _attach_retry_task(self, retry_task: OpTask):
        self.task_group_retry_task = retry_task

    # Implement abstract methods from BaseTask
    def _adopt_ttd_dag(self, ttd_task):
        pass

    def _child_tasks(self):
        return []

    def first_airflow_op(self):
        return None

    def last_airflow_op(self):
        return None


class TestTaskGroupMixin(unittest.TestCase):

    def setUp(self):
        self.task = MockTask()

    @patch("ttd.tasks.task_group_retry_mixin.TaskGroupRetryOperator")
    def test_as_taskgroup_with_retries(self, mock_retry_operator):
        group_id = "test_group"
        retries = 3
        retry_delay = timedelta(minutes=2)

        mock_op = mock_retry_operator.return_value
        self.task.as_taskgroup(group_id=group_id)
        self.task.with_retry_op(
            retries=retries,
            retry_delay=retry_delay,
            retry_exponential_backoff=True,
        )

        mock_retry_operator.assert_called_once_with(
            task_id=f"retry_{group_id}",
            task_group_id=group_id,
            retries=retries,
            retry_delay=retry_delay,
            retry_exponential_backoff=True,
        )

        self.assertIsInstance(self.task.task_group_retry_task, OpTask)
        self.assertEqual(self.task.task_group_retry_task._op, mock_op)

    @patch("ttd.tasks.task_group_retry_mixin.TaskGroupRetryOperator")
    def test_as_taskgroup_with_always_attach(self, mock_retry_operator):
        group_id = "test_group"

        mock_op = mock_retry_operator.return_value
        self.task.as_taskgroup(group_id=group_id)
        self.task.with_retry_op(retries=0, always_attach=True)

        mock_retry_operator.assert_called_once_with(
            task_id=f"retry_{group_id}",
            task_group_id=group_id,
            retries=0,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=False,
        )

        self.assertIsInstance(self.task.task_group_retry_task, OpTask)
        self.assertEqual(self.task.task_group_retry_task._op, mock_op)

    @patch("ttd.tasks.task_group_retry_mixin.TaskGroupRetryOperator")
    def test_as_taskgroup_with_custom_retry_op(self, mock_retry_operator):
        group_id = "test_group"

        class CustomRetryOperator(TaskGroupRetryOperator):
            pass

        self.task.as_taskgroup(group_id=group_id)
        self.task.with_retry_op(custom_retry_op=CustomRetryOperator, retries=1)

        mock_retry_operator.assert_not_called()
        self.assertIsInstance(self.task.task_group_retry_task._op, CustomRetryOperator)

    @patch("ttd.tasks.task_group_retry_mixin.TaskGroupRetryOperator")
    def test_as_taskgroup_with_retry_op_kwargs_override(self, mock_retry_operator):
        group_id = "test_group"
        retries = 2
        custom_kwargs = {"custom_key": "custom_value"}

        self.task.as_taskgroup(group_id=group_id)
        self.task.with_retry_op(
            retries=retries,
            retry_op_kwargs_override=custom_kwargs,
        )

        mock_retry_operator.assert_called_once_with(
            task_id=f"retry_{group_id}",
            task_group_id=group_id,
            retries=retries,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=False,
            custom_key="custom_value",
        )


if __name__ == "__main__":
    unittest.main()
