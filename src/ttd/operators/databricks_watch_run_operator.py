from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunState

from airflow.sensors.base import BaseSensorOperator

from typing import Any, Dict, List, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksWatchRunOperator(BaseSensorOperator):
    """
    Operator for watching a databricks job run and waiting for it to complete. There is nothing
    within airflow.providers.databricks that provides just this functionality in isolation, but
    it's helpful for us to have this as its own operator to better handle for Databricks API
    errors that we may hit when polling for job run status. In those events, this operator
    allows us to have airflow retry more often without having to kick off a new run
    """
    DB_HOOK_RETRY_LIMIT = 30
    DB_HOOK_RETRY_DELAY_SEC = 10

    template_fields: Sequence[str] = ("run_id", )

    def __init__(self, databricks_conn_id: str, run_id: str, task_names: List[str], *args, **kwargs):
        self._hook: DatabricksHook = DatabricksHook(
            databricks_conn_id,
            retry_limit=self.DB_HOOK_RETRY_LIMIT,
            retry_delay=self.DB_HOOK_RETRY_DELAY_SEC,
            caller="DatabricksWatchRunOperator",
        )
        self.run_id = run_id
        self.task_names = task_names
        super().__init__(*args, **kwargs)

    def poke(self, context: "Context") -> bool:
        self.log.info(f"Checking state of databricks run {self.run_id}")

        run = self._hook.get_run(int(self.run_id))
        run_state = RunState(**run["state"])

        if run_state.is_terminal:
            if run_state.is_successful:
                self.log.info("Found success state")
                return True
            else:
                self.log.error(f"Run {self.run_id} failed with state: '{run_state}' and state message: {run_state.state_message}")

                # TODO grab the error message from databricks
                try:
                    has_internal_error = self._log_databricks_error_message(run)

                    ti = context["ti"]
                    ti.xcom_push(key="has_internal_error", value=has_internal_error)

                except Exception as e:
                    self.log.error("Failed to get databricks error", exc_info=e)

                raise AirflowFailException("Databricks run failed")

        self.log.info(f"Databricks run still executing in state {run_state}")

        return False

    def on_kill(self) -> None:
        self._hook.cancel_run(int(self.run_id))
        self.log.info(f"Task: {self.task_id} with run_id {self.run_id} was requested to be cancelled")

    def _log_databricks_error_message(self, run: Dict[str, Any]) -> bool:
        # Because the run is multitask, we have to look at the tasks of the run
        tasks = run["tasks"]
        spark_tasks = [t for t in tasks if t["task_key"] in self.task_names]

        has_internal_error = False

        if len(spark_tasks) == 0:
            self.log.error("No spark tasks found to analyze / restart")
        else:
            for spark_task in spark_tasks:
                task_run_id = spark_task["run_id"]
                task_key = spark_task["task_key"]
                # try and get error message from another call
                try:
                    task_run_output = self._hook.get_run_output(task_run_id)
                    task_run_output_error = task_run_output.get("error", None)
                    task_run_output_error_trace = task_run_output.get("error_trace", None)
                    self.log.error(f"Exception for {task_key}: {task_run_output_error}\n{task_run_output_error_trace}")

                    task_run = self._hook.get_run(task_run_id)
                    task_run_state = RunState(**task_run["state"])

                    if task_run_state.life_cycle_state == "INTERNAL_ERROR":
                        self.log.error(f"Found internal error state for {task_key} - will retry automatically")
                        has_internal_error = True

                except Exception as e:
                    self.log.error("Failed to get Databricks error", exc_info=e)

        return has_internal_error
