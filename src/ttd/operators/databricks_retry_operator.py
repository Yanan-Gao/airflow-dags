from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunState

from ttd.operators.task_group_retry_operator import TaskGroupRetryOperator
from typing import TYPE_CHECKING, List, Union

from airflow.providers.databricks.operators.databricks import XCOM_RUN_ID_KEY
from airflow.models import TaskInstance

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from airflow.models import DagRun


class DatabricksRetryRunOperator(TaskGroupRetryOperator):
    DB_HOOK_RETRY_LIMIT = 30
    DB_HOOK_RETRY_DELAY_SEC = 1

    def __init__(self, databricks_conn_id: str, submit_run_task_id: str, watch_run_task_id: str, task_names: List[str], *args, **kwargs):
        self._hook = DatabricksHook(
            databricks_conn_id,
            retry_limit=self.DB_HOOK_RETRY_LIMIT,
            retry_delay=self.DB_HOOK_RETRY_DELAY_SEC,
            caller="DatabricksRetryRunOperator",
        )
        self._submit_run_task_id = submit_run_task_id
        self._watch_run_task_id = watch_run_task_id
        self.task_names = task_names
        super().__init__(*args, **kwargs)

    def should_retry(self, context: "Context", failed_tasks) -> Union[bool, List[str]]:
        dag_run: "DagRun" = context["dag_run"]  # type: ignore
        submit_run_task_instance: TaskInstance = dag_run.get_task_instance(self._submit_run_task_id)  # type: ignore

        if submit_run_task_instance is None:
            raise AirflowFailException(f"No task instance named {self._submit_run_task_id}")

        databricks_run_id = submit_run_task_instance.xcom_pull(key=XCOM_RUN_ID_KEY, task_ids=[self._submit_run_task_id])

        run = self._hook.get_run(databricks_run_id)
        run_state = RunState(**run["state"])

        if not run_state.is_terminal:
            # this can happen in times when the databricks API is flaky. So just retry the watch task, and we should be ok
            self.log.error(f"Databricks job is still in {run_state.life_cycle_state} state. Retrying the task {self._watch_run_task_id}")

            return [
                self._watch_run_task_id,
            ]

        if not run_state.is_successful:
            self.log.error(f"Run failed with state message: {run_state.state_message}")

            watch_run_task_instance = dag_run.get_task_instance(self._watch_run_task_id)

            if watch_run_task_instance is None:
                raise AirflowFailException(f"No task instance named {self._watch_run_task_id}")

            any_task_has_internal_error = bool(
                watch_run_task_instance.xcom_pull(key="has_internal_error", task_ids=[self._watch_run_task_id])
            )

            # retry on INTERNAL_ERROR
            return any_task_has_internal_error or run_state.life_cycle_state == "INTERNAL_ERROR"

        # If the run was successful, we shouldn't have gotten here from TaskGroupRetryOperator
        self.log.warning(
            "Databricks job was not found to be in a failure state. This likely means that the airflow task failed while the databricks run succeeded."
        )
        return False
