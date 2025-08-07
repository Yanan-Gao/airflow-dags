from datetime import datetime

from dags.measure.attribution.databricks_config import create_databricks_workflow, \
    DatabricksConfig, get_databricks_env_param, create_attribution_dag
from dags.measure.attribution.datalake.log_datapipe_config import task_assembly_param
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask


def create_dag(cloud_provider: CloudProvider):
    job_name = "delta_lake_optimize"
    ttd_dag: TtdDag = create_attribution_dag(
        dag_name=job_name,
        cloud_provider=cloud_provider,
        start_date=datetime(2025, 5, 14, 0, 0),
        schedule_interval="0 6 * * *",
        params=task_assembly_param(DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH)
    )

    databricks_optimize_task = create_databricks_workflow(
        job_name=job_name,
        cloud_provider=cloud_provider,
        use_photon=True,
        driver_node_type="r6gd.8xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=32,
        spark_config=DatabricksConfig.SPARK_CONF_DELTA_OPTIMIZATION,
        cluster_tags=DatabricksConfig.DATA_MAINTENANCE_TAG,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.bidfeedback.BidFeedbackDeltaOptimizer",
                executable_path="{{params.task_assembly_location}}",
                job_name="bidfeedback-deltalake-optimize",
                additional_command_line_parameters=[get_databricks_env_param()],
            ),
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.click.ClickTrackerDeltaOptimizer",
                executable_path="{{params.task_assembly_location}}",
                job_name="clicktracker-deltalake-optimize",
                additional_command_line_parameters=[get_databricks_env_param()],
            ),
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.conversion.ConversionTrackerDeltaOptimizer",
                executable_path="{{params.task_assembly_location}}",
                job_name="conversiontracker-deltalake-optimize",
                additional_command_line_parameters=[get_databricks_env_param()],
            )
        ]
    )

    ttd_dag >> databricks_optimize_task
    return ttd_dag.airflow_dag


optimize_dag = create_dag(cloud_provider=CloudProviders.aws)
