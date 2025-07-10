from datetime import datetime

from dags.measure.attribution.databricks_config import DatabricksConfig, \
    get_databricks_env_param, create_databricks_workflow, create_attribution_dag
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask


def create_dag(cloud_provider: CloudProvider):
    dataset_id = "crossdevicegraph-bucketed"
    ttd_dag: TtdDag = create_attribution_dag(
        dag_name=dataset_id, cloud_provider=cloud_provider, start_date=datetime(2025, 5, 14, 0, 0), schedule_interval="0 6 * * 1,4"
    )

    # Disable Databricks disk cache to avoid writing non-reusable remote parquet files
    spark_config = {**DatabricksConfig.SPARK_CONF_LOG_INGESTION, 'spark.databricks.io.cache.enabled': 'false'}
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
                class_name="com.thetradedesk.attribution.datapipe.crossdevicegraph.CrossDeviceGraphLoader",
                executable_path=DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH,
                job_name="incremental-load",
                additional_command_line_parameters=[get_databricks_env_param()],
            )
        ]
    )

    ttd_dag >> databricks_task
    return ttd_dag.airflow_dag


crossdevicegraph_bucketed_load_dag = create_dag(cloud_provider=CloudProviders.aws)
