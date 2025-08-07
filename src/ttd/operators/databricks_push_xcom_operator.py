import json

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import XCOM_RUN_ID_KEY
from airflow.utils.context import Context

from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask


class DatabricksPushXcomOperator(BaseOperator):
    """
    Takes the result of dbutils.notebook.exit(JSON_STRING_GOES_HERE)
    Deserializes the json string, then pushes the result to xcom
    We use multiple_outputs to unpack the dict
    """

    DB_HOOK_RETRY_LIMIT = 1
    DB_HOOK_RETRY_DELAY_SEC = 60

    def __init__(
        self,
        databricks_conn_id: str,
        submit_run_task_id: str,
        task: DatabricksJobTask,
        **kwargs,
    ):
        self._hook = DatabricksHook(
            databricks_conn_id,
            retry_limit=self.DB_HOOK_RETRY_LIMIT,
            retry_delay=self.DB_HOOK_RETRY_DELAY_SEC,
            caller=type(self).__name__,
        )
        self._submit_run_task_id = submit_run_task_id
        self.task_name = task.task_name
        super().__init__(
            task_id=task.get_push_xcom_task_id(),
            do_xcom_push=True,
            multiple_outputs=True,
            **kwargs,
        )

    def execute(self, context: Context):
        run_id = context["task_instance"].xcom_pull(key=XCOM_RUN_ID_KEY, task_ids=self._submit_run_task_id)
        run = self._hook.get_run(run_id)
        tasks = [t for t in run["tasks"] if t["task_key"] == self.task_name]
        try:
            (task, ) = tasks
        except ValueError as e:
            raise AirflowFailException(f"Should only have exactly one task matching the task key: {self.task_name}") from e
        task_run_output = self._hook.get_run_output(task["run_id"])
        notebook_output = task_run_output.get("notebook_output")
        if notebook_output is not None:
            if notebook_output["truncated"]:
                self.log.warning("Notebook output was truncated, xcom might not function correctly")
            return json.loads(notebook_output["result"])
        if task_run_output.get("logs_truncated"):
            self.log.warning("Logs were truncated, xcom might not function correctly")
        try:
            logs = task_run_output["logs"]
        except KeyError as e:
            raise AirflowFailException("Could not find xcom logged in databricks run output") from e
        return json.loads(logs)
