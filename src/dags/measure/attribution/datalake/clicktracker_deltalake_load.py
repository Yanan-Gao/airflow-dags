from datetime import datetime

from dags.dataproc.datalake.datalake_parquet_utils import LogNames
from dags.measure.attribution.databricks_config import DatabricksConfig, create_attribution_dag, \
    create_databricks_workflow, log_gate_sensor_task
from dags.measure.attribution.datalake.log_datapipe_config import task_assembly_param, databricks_load_task_params, \
    fetch_delta_load_watermark_task, fetch_cleanse_watermark_task, delta_load_branching_task, \
    get_log_task_high_watermark_timestamp, log_data_pipe_param
from ttd.cloud_provider import CloudProviders
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask


def create_dag(cloud_provider):
    dataset_id = "clicktracker-deltalake"
    ttd_dag = create_attribution_dag(
        dag_name=dataset_id,
        cloud_provider=cloud_provider,
        start_date=datetime(2025, 5, 14, 0, 0),
        schedule_interval="@hourly",
        params={
            **task_assembly_param(DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH),
            **log_data_pipe_param()
        }
    )

    log_task_expr = 'dbo.fn_Enum_Task_HourlyCleanseClickTrackerCompleted()'
    fetch_log_watermark_task = fetch_cleanse_watermark_task(log_task_expr)
    delta_high_watermark_task_id, delta_high_watermark_task = fetch_delta_load_watermark_task(log_type="clicktracker")

    delta_load_run_now_params = databricks_load_task_params(log_task_high_watermark=get_log_task_high_watermark_timestamp())
    delta_load_run_now_task = create_databricks_workflow(
        job_name=f"{dataset_id}_now",
        cloud_provider=cloud_provider,
        use_photon=False,
        driver_node_type="r6gd.4xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=2,
        spark_config=DatabricksConfig.SPARK_CONF_LOG_INGESTION,
        cluster_tags=DatabricksConfig.DATA_INGESTION_TAG,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.click.ClickTrackerIncrementalLoader",
                executable_path="{{params.task_assembly_location}}",
                job_name="incremental_load",
                additional_command_line_parameters=delta_load_run_now_params,
            )
        ],
        install_ttd_certificate=True,
    )

    logs_gate_sensor_ti = log_gate_sensor_task(LogNames.clicktrackerverticaload, CloudProviders.aws)
    load_branching_task = delta_load_branching_task(delta_load_run_now_task.task_id, logs_gate_sensor_ti.task_id)

    delta_load_after_sensor_task = create_databricks_workflow(
        job_name=f"{dataset_id}_post_sensor",
        cloud_provider=cloud_provider,
        use_photon=False,
        driver_node_type="r6gd.4xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=2,
        spark_config=DatabricksConfig.SPARK_CONF_LOG_INGESTION,
        cluster_tags=DatabricksConfig.DATA_INGESTION_TAG,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.click.ClickTrackerIncrementalLoader",
                executable_path="{{params.task_assembly_location}}",
                job_name="incremental_load",
                additional_command_line_parameters=databricks_load_task_params(),
            )
        ],
        install_ttd_certificate=True,
    )

    ttd_dag >> [fetch_log_watermark_task, delta_high_watermark_task
                ] >> load_branching_task >> [logs_gate_sensor_ti, delta_load_run_now_task]
    logs_gate_sensor_ti >> delta_load_after_sensor_task
    return ttd_dag.airflow_dag


clicktracker_delta_load_dag = create_dag(CloudProviders.aws)
