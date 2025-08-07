from datetime import datetime

from dags.measure.attribution.databricks_config import DatabricksConfig, get_databricks_env_param, \
    create_attribution_dag, create_databricks_workflow
from dags.measure.attribution.datalake.log_datapipe_config import task_assembly_param
from ttd.cloud_provider import CloudProviders
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask


def create_dag(cloud_provider):
    dataset_id = "bidfeedback-bucketed"
    ttd_dag = create_attribution_dag(
        dag_name=dataset_id,
        cloud_provider=cloud_provider,
        start_date=datetime(2025, 5, 14, 0, 0),
        schedule_interval="0 0 * * *",
        params=task_assembly_param(DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH)
    )

    # Disable Databricks disk cache to avoid writing non-reusable remote parquet files
    spark_config = {
        **DatabricksConfig.SPARK_CONF_LOG_INGESTION, 'spark.databricks.io.cache.enabled': 'false',
        'spark.sql.sources.bucketing.enabled': 'false'
    }
    databricks_task = create_databricks_workflow(
        job_name=dataset_id,
        cloud_provider=cloud_provider,
        use_photon=False,
        driver_node_type="r6gd.8xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=32,
        spark_config=spark_config,
        cluster_tags=DatabricksConfig.DATA_INGESTION_TAG,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.datapipe.bidfeedback.BidFeedbackBucketedTableLoader",
                executable_path="{{params.task_assembly_location}}",
                job_name="incremental-load",
                additional_command_line_parameters=[get_databricks_env_param()],
            )
        ]
    )

    ttd_dag >> databricks_task
    return ttd_dag.airflow_dag


bidfeedback_bucket_load_dag = create_dag(CloudProviders.aws)
