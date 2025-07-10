from datetime import datetime, timedelta
from typing import List

from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.slack.slack_groups import dataproc, MEASUREMENT

hourly_exports = [
    "ParquetSync_AdGroup_Hourly",
    "ParquetSync_AdGroupSecondarySettings_Hourly",
    "ParquetSync_Advertiser_Hourly",
    "ParquetSync_AdvertiserConversionTypeBlacklist",
    "ParquetSync_Campaign_Hourly",
    "ParquetSync_CampaignConversionReportingColumnHourly_Hourly",
    "ParquetSync_CrossDeviceAttributionModelVendor_Hourly",
    "ParquetSync_DATMeasurementSetting",
    "ParquetSync_OfflineDataProvider_Hourly",
    "ParquetSync_TrackingTag_Hourly",
    "ParquetSync_UniversalPixel_Hourly",
    "ParquetSync_UniversalPixelTrackingTag_Hourly",
]

daily_exports = [
    "ParquetSync_OfflineDataProviderMeasurementConfig",
    "ParquetSync_ProductCatalogMerchant",
    "ParquetSync_ProductCatalogEcommerceTrackingTag",
]


def get_task_data(names: List[str]) -> str:
    return ','.join(names)


def create_dag(task_type: str, schedule_interval: str, dataset: List[str]):
    task_config_name = "ProvisioningToParquetChangeTrackingTaskConfig"
    task_name = "ProvisioningToParquetChangeTrackingTask"
    task_name_suffix = f"{task_type}_ATTRIBUTION_DATASET"
    task_service_task_id = TaskServiceOperator.format_task_name(task_name, task_name_suffix)

    dag = TtdDag(
        dag_id="ts-" + task_service_task_id,
        schedule_interval=schedule_interval,
        enable_slack_alert=False,
        max_active_runs=1,
        start_date=datetime(2025, 4, 10),
        run_only_latest=True,
        params={"task_service_task_id": task_service_task_id},
        teams_allowed_to_access=[MEASUREMENT.team.jira_team]
    )

    tso = TaskServiceOperator(
        task_name=task_name,
        task_config_name=task_config_name,
        task_name_suffix=task_name_suffix,
        task_data=get_task_data(dataset),
        scrum_team=dataproc,
        resources=TaskServicePodResources.custom(
            request_cpu="0.5",
            request_memory="1Gi",
            request_ephemeral_storage="2Gi",
            limit_memory="1.5Gi",
            limit_ephemeral_storage="4Gi",
        ),
        task_execution_timeout=timedelta(minutes=30),
    )

    databricks_workflow = DatabricksWorkflow(
        job_name=f"measure_log_provisioning_sync_record_job_{task_type}",
        cluster_name=f"measure_log_provisioning_sync_record_cluster_{task_type}",
        cluster_tags={"Team": MEASUREMENT.team.jira_team},
        databricks_spark_version="15.4.x-scala2.12",
        driver_node_type="r6gd.2xlarge",
        worker_node_type="m6gd.2xlarge",
        worker_node_count=1,
        spark_configs={
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
        },
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.attribution.provisioningsyncrecord.ParquetSyncRecord",
                executable_path="s3://ttd-build-artefacts/conversion-attribution-spark/release/latest/LogIngestionDataPipe.jar",
                job_name="log_provisioning_sync_record",
                additional_command_line_parameters=[
                    str(TtdEnvFactory.get_from_system()).lower(),
                    '{{ dag_run.get_task_instance(task_id=params.task_service_task_id).start_date }}', task_type
                ],
            )
        ]
    )

    dag >> OpTask(op=tso) >> databricks_workflow

    return dag


hourly_dag = create_dag("HOURLY", "10 * * * *", hourly_exports)
globals()[hourly_dag.airflow_dag.dag_id] = hourly_dag.airflow_dag

daily_dag = create_dag("DAILY", "0 3 * * *", daily_exports)
globals()[daily_dag.airflow_dag.dag_id] = daily_dag.airflow_dag
