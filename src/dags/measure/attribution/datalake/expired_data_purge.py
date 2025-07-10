from datetime import datetime

from dags.measure.attribution.databricks_config import create_databricks_workflow, \
    DatabricksConfig, get_databricks_env_param, create_attribution_dag
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask


def create_dag(cloud_provider: CloudProvider):
    job_name = "expired_data_purge"
    ttd_dag: TtdDag = create_attribution_dag(
        dag_name=job_name, cloud_provider=cloud_provider, start_date=datetime(2025, 5, 14, 0, 0), schedule_interval="0 5 * * 1,3,5,7"
    )

    jobTimestamp = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:%M:%S\") }}"
    databricks_purge_expired_task = create_databricks_workflow(
        job_name=job_name,
        cloud_provider=cloud_provider,
        use_photon=False,
        driver_node_type="r6gd.8xlarge",
        worker_node_type="r6gd.4xlarge",
        worker_node_count=16,
        spark_config=DatabricksConfig.SPARK_CONF_DELTA_OPTIMIZATION,
        cluster_tags=DatabricksConfig.DATA_MAINTENANCE_TAG,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.maintenance.ManagedDeltaPurge",
                executable_path=DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH,
                job_name="deltalake-purge",
                additional_command_line_parameters=[get_databricks_env_param(), jobTimestamp],
            ),
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.maintenance.ExternalTablePurge",
                executable_path=DatabricksConfig.LOG_DATAPIPE_SPARK_EXECUTABLE_PATH,
                job_name="external-purge",
                additional_command_line_parameters=[get_databricks_env_param(), jobTimestamp],
            )
        ]
    )

    ttd_dag >> databricks_purge_expired_task
    return ttd_dag.airflow_dag


purge_dag = create_dag(CloudProviders.aws)
