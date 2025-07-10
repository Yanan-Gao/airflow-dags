from datetime import datetime, timedelta
from typing import Optional, List

from airflow.models.dag import ScheduleInterval

from dags.dataproc.datalake.datalake_logs_gate_sensor import DatalakeLogsGateSensor
from ttd.cloud_provider import CloudProvider
from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.region import DatabricksRegion
from ttd.eldorado.databricks.tasks.databricks_job_task import DatabricksJobTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.slack.slack_groups import MEASURE_TASKFORCE_CAT, DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory


def create_attribution_dag(
    dag_name: str,
    cloud_provider: CloudProvider,
    start_date: Optional[datetime],
    schedule_interval: ScheduleInterval,
) -> TtdDag:
    return TtdDag(
        dag_id=f"measure-attr-{dag_name}-{cloud_provider.__str__()}",
        start_date=start_date,
        end_date=None,
        schedule_interval=schedule_interval,
        # Built-in catch up, past failure ignored
        depends_on_past=False,
        run_only_latest=True,
        # Max 1 global task in each data domain (cloud provider)
        max_active_tasks=1,
        max_active_runs=1,
        # No automatic retry. Transient & retryable failures are not expected.
        retries=0,
        # Alert Configuration
        contact_email=None,
        enable_slack_alert=True,
        slack_channel="#taskforce-cat-alarms",
        slack_tags=MEASURE_TASKFORCE_CAT.team.jira_team,
        dag_tsg=None,
        slack_alert_only_for_prod=False,
        # Other Configurations
        on_failure_callback=None,
        on_success_callback=None,
        dagrun_timeout=timedelta(hours=12),
        teams_allowed_to_access=[MEASURE_TASKFORCE_CAT.team.jira_team, DATASRVC.team.jira_team],
    )


def log_gate_sensor_task(log_name: str, cloud_provider: CloudProvider) -> OpTask:
    return OpTask(
        op=DatalakeLogsGateSensor(
            logname=log_name,
            cloud_provider=cloud_provider,
            task_id="logs_gate_sensor",
            poke_interval=60,  # in seconds
            timeout=60 * 60 * 2,  # in seconds
            mode="reschedule",
        )
    )


def create_databricks_workflow(
    job_name: str,
    cloud_provider: CloudProvider,
    use_photon: bool,
    driver_node_type: str,
    worker_node_type: str,
    worker_node_count: int,
    tasks: List[DatabricksJobTask],
    spark_config: dict[str, str],
    cluster_tags: dict[str, str],
) -> DatabricksWorkflow:
    job_name = f"measure-attr-{job_name}-{cloud_provider.__str__()}"
    return DatabricksWorkflow(
        job_name=job_name,
        databricks_instance_profile_arn=DatabricksConfig.AWS_ROLE_ARN,
        cluster_tags={
            **DatabricksConfig.BASE_TAG,
            **cluster_tags
        },
        databricks_spark_version=DatabricksConfig.DATABRICKS_VERSION,
        cluster_name=job_name,
        use_photon=use_photon,
        driver_node_type=driver_node_type,
        worker_node_type=worker_node_type,
        worker_node_count=worker_node_count,
        spark_configs=spark_config,
        region=DatabricksRegion.use(),
        tasks=tasks
    )


def get_databricks_env_param() -> str:
    """

    Returns: an environment identifier passed to Databricks jobs to select test/prod catalogue

    """
    match TtdEnvFactory.get_from_system():
        case TtdEnvFactory.prod:
            return "prod"
        case _:
            return "test"


class DatabricksConfig:
    DATABRICKS_VERSION = "16.4.x-scala2.12"

    LOG_DATAPIPE_SPARK_EXECUTABLE_PATH = "s3://ttd-build-artefacts/conversion-attribution-spark/release/latest/LogIngestionDataPipe.jar"
    ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH = "s3://ttd-build-artefacts/conversion-attribution-spark/release/latest/AttributionDAG.jar"

    # Expect ttd.eldorado.databricks.workflow.infer_instance_profile to infer medivac instance
    AWS_ROLE_ARN = None

    BASE_TAG = {
        'Team': MEASURE_TASKFORCE_CAT.team.jira_team,
        'COST_MANAGEMENT_GROUP': 'measure-attr',
    }
    DATA_INGESTION_TAG: dict[str, str] = {
        'COST_MANAGEMENT_TASK': 'measure-attr-input-ingestion',
    }
    DATA_MAINTENANCE_TAG: dict[str, str] = {
        'COST_MANAGEMENT_TASK': 'measure-attr-input-maintenance',
    }
    ATTRIBUTION_JOB_TAG: dict[str, str] = {
        'COST_MANAGEMENT_TASK': 'measure-attr-attribution-job',
    }

    # Spark configuration reserved for non-Photon Job compute ingesting log data
    SPARK_CONF_LOG_INGESTION: dict[str, str] = {
        'fs.s3a.acl.default': 'BucketOwnerFullControl',
        'spark.databricks.adaptive.autoOptimizeShuffle.enabled': 'true',
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
        'spark.databricks.delta.optimize.repartition.enabled': 'true',
        'spark.databricks.nonDelta.partitionLog.enabled': 'true',
        'spark.driver.maxResultSize': '8g',
        'spark.sql.adaptive.autoBroadcastJoinThreshold': '128m',
        'spark.sql.autoBroadcastJoinThreshold': '128m',
        # Dynamic Bloom Filter
        'spark.sql.optimizer.runtime.bloomFilter.applicationSideThreshold': '256g',
        'spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold': '10g',
        'spark.sql.optimizer.runtime.bloomFilter.maxNumBits': '1073741824',
        'spark.sql.optimizer.runtime.bloomFilter.maxNumItems': '2000000',
        'spark.databricks.optimizer.bloomFilter.disableIfBroadCastJoinOverApplicationSide': 'false',
        # Prevent Spark optimizer from generating an invalid 'LIMIT' keyword in SQLServer query
        'spark.databricks.optimizer.jdbcDSv1LimitPushdown.enabled': 'false'
    }

    SPARK_CONF_DELTA_OPTIMIZATION: dict[str, str] = {
        'fs.s3a.acl.default': 'BucketOwnerFullControl',
        'spark.databricks.adaptive.autoOptimizeShuffle.enabled': 'true',
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
        'spark.databricks.delta.optimize.repartition.enabled': 'true',
        'spark.databricks.nonDelta.partitionLog.enabled': 'true',
        'spark.driver.maxResultSize': '8g',
        'spark.sql.adaptive.autoBroadcastJoinThreshold': '128m',
        'spark.sql.autoBroadcastJoinThreshold': '128m',
        # Delta Cache
        'spark.databricks.io.cache.enabled': 'false',
        # File Compaction
        'spark.databricks.delta.optimize.preserveInsertionOrder': 'false'
    }

    SPARK_CONF_JOB_SCHEDULER = {
        'fs.s3a.acl.default': 'BucketOwnerFullControl',
        'spark.databricks.adaptive.autoOptimizeShuffle.enabled': 'true',
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
        'spark.databricks.nonDelta.partitionLog.enabled': 'true',
        # Delta Cache
        'spark.databricks.io.cache.enabled': 'false',
    }

    SPARK_CONF_ATTRIBUTION_PHOTON: dict[str, str] = {
        'fs.s3a.acl.default': 'BucketOwnerFullControl',
        'spark.databricks.nonDelta.partitionLog.enabled': 'true',
        'spark.databricks.adaptive.autoOptimizeShuffle.enabled': 'true',
        'spark.databricks.adaptive.autoOptimizeShuffle.maxPartitionNumber': '30720',
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
        'spark.databricks.optimizer.dynamicFilePruning.enabled': 'false',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        # High broadcast limit requires Databricks Photon runtime
        'spark.driver.maxResultSize': '16g',
        'spark.sql.adaptive.autoBroadcastJoinThreshold': '512m',
        'spark.sql.autoBroadcastJoinThreshold': '512m',
        'spark.sql.cbo.joinReorder.enabled': 'false',
        # Delta Cache
        'spark.databricks.io.cache.enabled': 'false',
        # Dynamic Partition Pruning
        'spark.sql.optimizer.dynamicPartitionPruningInFileIndex.enabled': 'true',
        # Dynamic Bloom Filter
        'spark.sql.optimizer.runtime.bloomFilter.applicationSideThreshold': '256g',
        'spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold': '10g',
        'spark.sql.optimizer.runtime.bloomFilter.maxNumBits': '1073741824',
        'spark.sql.optimizer.runtime.bloomFilter.maxNumItems': '2000000',
        'spark.databricks.optimizer.bloomFilter.disableIfBroadCastJoinOverApplicationSide': 'false',
        # Bucketing Table
        'spark.sql.sources.bucketing.autoBucketedScan.enabled': 'false'
    }
