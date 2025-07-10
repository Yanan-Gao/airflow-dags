import logging
from datetime import timedelta
from typing import TYPE_CHECKING, List, Union

from airflow.exceptions import AirflowFailException, AirflowException, AirflowSkipException
from airflow.models import BaseOperator, MappedOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from airflow.models import DagRun


class TaskGroupRetryOperator(BaseOperator):
    ui_color: str = "#773dbd"
    ui_fgcolor: str = "#fff"

    def __init__(
        self,
        *args,
        task_id: str,
        task_group_id: str,
        retries: int = 1,
        retry_delay: timedelta = timedelta(minutes=1),
        retry_exponential_backoff: bool = False,
        excluded_task_ids: List[str] = None,
        **kwargs,
    ):
        self._task_group_id = task_group_id
        self._excluded_task_ids = excluded_task_ids
        super().__init__(  # type: ignore
            *args,
            task_id=task_id,
            retries=retries,
            retry_delay=retry_delay,
            retry_exponential_backoff=retry_exponential_backoff,
            # This task should always execute whether upstream succeeded or failed because we need to trigger a retry
            # for those failures
            trigger_rule=TriggerRule.ALL_DONE,
            **kwargs,
        )

    def execute(self, context: "Context"):
        group = self.dag.task_group_dict[self._task_group_id]
        task_ids = self._find_task_ids_in_group(group)

        dag_run: DagRun = context["dag_run"]  # type: ignore
        failed_tasks = self._ops_with_ti_state_in_dag_run(TaskInstanceState.FAILED, dag_run, task_ids)

        if not any(failed_tasks):
            if any(self._ops_with_ti_state_in_dag_run(TaskInstanceState.UPSTREAM_FAILED, dag_run, task_ids)):
                raise AirflowFailException(
                    "Detected no tasks in FAILED state in this TaskGroup, but did find tasks in the UPSTREAM_FAILED state. Failed tasks further upstream of this TaskGroup must be fixed before this TaskGroup can be retried"
                )
            if any(self._ops_with_ti_state_in_dag_run(TaskInstanceState.SKIPPED, dag_run, task_ids)):
                raise AirflowSkipException(
                    "Detected tasks in the SKIPPED state. Will skip this execution to propagate skip further downstream"
                )
            return

        ti = context["task_instance"]
        if ti.try_number > ti.max_tries:
            raise AirflowFailException("Exceeded the number of retries specified. FAILING the run.")

        logging.info(f"Detected the following FAILED tasks: {'; '.join([task.task_id for task in failed_tasks])}")

        if all(task.is_teardown for task in failed_tasks):
            logging.info("The only tasks are teardown tasks, there's no need to re-do the actual work of other upstream tasks")
            task_ids_to_clear = [task.task_id for task in failed_tasks] + [self.task_id]
        else:
            # a task failed that was attempting to do actual work - we need to retry everything
            task_ids_to_clear = task_ids

        should_retry_result = self.should_retry(context, failed_tasks)

        if isinstance(should_retry_result, List):
            task_ids_to_clear = should_retry_result + [self.task_id]
        elif not should_retry_result:
            raise AirflowFailException("The RetryOperator does not wish to retry these tasks. FAILING the run.")

        logging.info(f"Clearing the following tasks: {'; '.join(task_ids_to_clear)}")
        execution_date = context["execution_date"]
        self.dag.clear(
            task_ids=task_ids_to_clear,
            start_date=execution_date,
            end_date=execution_date,
        )

        raise AirflowException

    def should_retry(self, context: "Context", failed_tasks: List[BaseOperator | MappedOperator]) -> Union[bool, List[str]]:
        """
        Can be overwritten by inheriting class to do a more "smart" retry.
        By default, we will always retry no matter what.

        For even more fine-grained control, custom implementation can also return a list of specific task ids to retry.
        (The retry operator itself will implicitly get retried, so no need to return that)
        """
        return True

    def _find_task_ids_in_group(self, group: "TaskGroup") -> List[str]:
        tasks: List[str] = []
        for child in group.children.values():
            if isinstance(child, TaskGroup):
                tasks = tasks + self._find_task_ids_in_group(child)
            else:
                assert isinstance(child, BaseOperator)
                tasks.append(child.task_id)
        return tasks

    def _ops_with_ti_state_in_dag_run(
        self,
        state: TaskInstanceState,
        dag_run: "DagRun",
        task_ids: List[str],
    ) -> List[BaseOperator | MappedOperator]:
        tis = dag_run.get_task_instances(state=[state])

        if self._excluded_task_ids is not None:
            tis = [ti for ti in tis if ti.task_id not in self._excluded_task_ids]

        return [self.dag.task_dict[ti.task_id] for ti in tis if ti.task_id in task_ids]


class SetupTaskRetryOperator(TaskGroupRetryOperator):
    """
    Implementation of TaskGroupRetryOperator that only retries if the failure occurred in setup tasks
    """

    def should_retry(self, context: "Context", failed_tasks: List[BaseOperator | MappedOperator]) -> bool:
        for task in failed_tasks:
            if not task.is_setup:
                return False
        return True
