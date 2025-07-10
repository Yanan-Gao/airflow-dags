from datetime import datetime

from dags.dataproc.datalake.datalake_parquet_utils import LogNames
from dags.measure.attribution.databricks_config import DatabricksConfig, get_databricks_env_param, \
    create_databricks_workflow, log_gate_sensor_task, create_attribution_dag
from ttd.cloud_provider import CloudProviders
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask


def create_dag(cloud_provider):
    dataset_id = "conversiontracker-deltalake"
    ttd_dag = create_attribution_dag(
        dag_name=dataset_id,
        cloud_provider=cloud_provider,
        start_date=datetime(2025, 5, 14, 0, 0),
        schedule_interval="@hourly",
    )

    logs_gate_sensor = log_gate_sensor_task(LogNames.conversiontrackerverticaload, CloudProviders.aws)

    databricks_task = create_databricks_workflow(
        job_name=dataset_id,
        cloud_provider=cloud_provider,
        use_photon=False,
        driver_node_type="r6gd.8xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=32,
        spark_config=DatabricksConfig.SPARK_CONF_LOG_INGESTION,
        cluster_tags=DatabricksConfig.DATA_INGESTION_TAG,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.conversion.ConversionTrackerIncrementalLoader",
                executable_path=DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH,
                job_name="incremental_load",
                additional_command_line_parameters=[get_databricks_env_param(), "{{ execution_date.strftime(\"%Y-%m-%dT%H:%M:%S\") }}"],
            )
        ]
    )

    ttd_dag >> logs_gate_sensor >> databricks_task
    return ttd_dag.airflow_dag


conversiontracker_delta_load_dag = create_dag(CloudProviders.aws)
