import logging
from datetime import datetime, timezone
from typing import Tuple

from airflow import AirflowException
from airflow.models import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dags.measure.attribution.databricks_config import get_databricks_env_param
from ttd.eldorado.databricks.region import DatabricksRegion
from ttd.tasks.op import OpTask


def task_assembly_param(default_location: str) -> dict[str, Param]:
    return {
        "task_assembly_location":
        Param(default=default_location, type="string", description="The JAR assembly cloud storage path used in Databricks task")
    }


def log_data_pipe_param() -> dict[str, Param]:
    return {
        "start_time_override":
        Param(
            default="",
            type=["null", "string"],
            description="Optional: A default utc hour timestamp to create new load history when no existing checkpoint is found."
        )
    }


def databricks_load_task_params(log_task_high_watermark='{{ logical_date.strftime("%Y-%m-%dT%H:00:00") }}') -> list[str]:
    return [
        "env",
        get_databricks_env_param(), "datasource_high_watermark", log_task_high_watermark, 'start_time_override',
        '{{ params.start_time_override }}'
    ]


def get_log_task_high_watermark_timestamp() -> str:
    return "{{ti.xcom_pull(task_ids='fetch_hourly_cleanse_watermark', key='return_value').strftime('%Y-%m-%dT%H:00:00')}}"


def fetch_cleanse_watermark_task(hourly_log_task_fn_expr: str) -> OpTask:

    def _get_latest_hourly_completed_task(mssql_conn_id: str) -> datetime | None:
        try:
            sql_query = f"""
               select max(LogStartTime) as LatestCompleteTime
                    from dbo.LogFileTask lft
                    where lft.TaskId = {hourly_log_task_fn_expr}
                    and lft.LogFileTaskStatusId = dbo.fn_Enum_LogFileTaskStatus_Completed()"""
            hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')

            logging.info(f"executing query: {sql_query}")
            record = hook.get_first(sql=sql_query)

            if not record:
                logging.warning(f"No completion timestamp found for task {hourly_log_task_fn_expr}.")
                return None

            return record[0].replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        except Exception as e:
            logging.error(f"An error occurred while querying MSSQL for task {hourly_log_task_fn_expr}: {e}")
            # Fail safely by returning None if any database error occurs.
            return None

    task_id = "fetch_hourly_cleanse_watermark"
    return OpTask(
        op=PythonOperator(
            task_id=task_id, python_callable=_get_latest_hourly_completed_task, op_kwargs={'mssql_conn_id': 'lwdb'}, provide_context=True
        )
    )


def _run_spark_sql_query(**kwargs):
    workspace_region = kwargs["params"]["workspace_region"]
    databricks_conn_id = f"databricks-aws-{workspace_region}"
    sql_query = kwargs["params"]["sql_query"]

    sql_hook = DatabricksSqlHook(
        databricks_conn_id=databricks_conn_id,
        catalog=get_databricks_env_param(),
        schema="attribution",
        http_path="/sql/1.0/warehouses/3b6aaef215774b52"
    )

    def handle_query_result(cursor) -> dict:
        result = cursor.fetchone()
        if result:
            return result.asDict()
        else:
            return {}

    return sql_hook.run(sql=sql_query, handler=handle_query_result)


def fetch_delta_load_watermark_task(log_type: str) -> Tuple[str, OpTask]:
    region = DatabricksRegion.use()
    task_id = "fetch_delta_load_high_watermark"
    sql_query = f"select IntervalStart from log_ingestion_watermark where logType = '{log_type}' and Status = 1 order by IntervalStart desc limit 1"
    op_task = OpTask(
        op=PythonOperator(
            task_id=task_id,
            python_callable=_run_spark_sql_query,
            params={
                "sql_query": sql_query,
                "workspace_region": region.workspace_region
            },
            provide_context=True
        )
    )
    return task_id, op_task


def select_task_handler(run_now_taskid: str, wait_sensor_taskid: str, **kwargs) -> str:
    ti = kwargs["ti"]
    dag_params = kwargs['params']

    delta_load_high_watermark = ti.xcom_pull(task_ids="fetch_delta_load_high_watermark", key="return_value")
    log_task_high_watermark = ti.xcom_pull(task_ids="fetch_hourly_cleanse_watermark", key="return_value")

    if not delta_load_high_watermark:
        if not dag_params['start_time_override']:
            raise AirflowException(
                "No existing delta load checkpoint found, DAG param start_time_override must be specified to begin new load."
            )
        elif not log_task_high_watermark:
            logging.warning("cleanse task high watermark does not exist.")
            return wait_sensor_taskid
        else:
            logging.info("run_now branch is selected, launching delta load with start time override.")
            return run_now_taskid

    # upstream data source high watermark is required by delta load task, switch to the sensor branch to retry
    if not log_task_high_watermark or delta_load_high_watermark["IntervalStart"] >= log_task_high_watermark:
        return wait_sensor_taskid
    else:
        logging.info("run_now branch is selected, cleanse task high watermark >> delta load task high watermark.")
        return run_now_taskid


def delta_load_branching_task(run_now_taskid: str, wait_sensor_taskid: str) -> OpTask:
    task_id = "select_load_action_task"

    return OpTask(
        op=BranchPythonOperator(
            task_id=task_id,
            python_callable=select_task_handler,
            provide_context=True,
            op_kwargs={
                "run_now_taskid": run_now_taskid,
                "wait_sensor_taskid": wait_sensor_taskid
            }
        )
    )
