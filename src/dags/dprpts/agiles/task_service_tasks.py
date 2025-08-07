from datetime import datetime, timedelta
from json import JSONEncoder
from typing import List, Dict, Optional

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.vertica_clusters import (
    get_variants_for_group,
    get_cloud_provider_for_cluster,
    TaskVariantGroup,
    VerticaCluster,
)
from ttd.task_service.k8s_connection_helper import aws
import re

vertica_variant_group = TaskVariantGroup.VerticaEtl
scrum_team = dprpts
job_schedule_interval = "*/15 * * * *"
alert_channel = "#dev-agiles-alerts"
base_task_name = "ts-rti-"
start_date = datetime.now() - timedelta(hours=1)
default_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'owner': scrum_team.jira_team,
}
max_active_runs = 3
retries = 2
retry_delay = timedelta(minutes=5)
task_execution_timeout = timedelta(hours=4)
etl_vertica_cluster_association = {
    VerticaCluster.USEast01: [VerticaCluster.USEast03, VerticaCluster.CNEast01],
    VerticaCluster.USWest01: [VerticaCluster.USWest03, VerticaCluster.CNWest01]
}


def create_phase_task_operator(
    main_args: Dict[str, object],
    phase_origin_id: str,
    depends_on: List[str],
    max_batches_per_run: int,
    batch_origin_clusters: Optional[List[str]] = None,
    phase_origin_types: Optional[List[str]] = None
) -> OpTask:
    if phase_origin_types is None:
        phase_origin_types = ["Incremental", "TrueUp", "LateData"]

    rti_phase_task_base_args = {
        "task_name": "RtiPipePhaseTask",
        **main_args,
        "task_name_suffix": phase_origin_id  # use the name of the phase origin to make it nicer in the UI
    }

    op_task = OpTask(
        op=TaskServiceOperator(
            task_data=JSONEncoder().encode({
                "PhaseOriginId": phase_origin_id,
                "DependsOn": depends_on,
                "MaxBatchesPerRun": max_batches_per_run,
                "BatchOriginClusters": batch_origin_clusters,
                "PhaseOriginTypes": phase_origin_types,
            }),
            **rti_phase_task_base_args
        )
    )

    return op_task


for vertica_cluster in (get_variants_for_group(vertica_variant_group) + get_variants_for_group(TaskVariantGroup.VerticaAliCloud)):
    pipeline_suffix = vertica_cluster.name

    k8s_sovereign_connection_helper = get_cloud_provider_for_cluster(vertica_cluster)

    ttd_dag = TtdDag(
        base_task_name + TaskServiceOperator.format_task_name('pipe-agile-task', pipeline_suffix),
        default_args=default_args,
        schedule_interval=job_schedule_interval,
        slack_channel=alert_channel,
        tags=["TaskService", scrum_team.jira_team],
        max_active_runs=max_active_runs,
        start_date=start_date,
        depends_on_past=False,
        run_only_latest=False,
    )

    # getting cluster name until digits and prefix it with '-': USEast01 -> -useast
    # also replaces cn to us as we only need one type of secrets CNEast01 -> -useast
    secret_suffix = "-" + re.sub('[a-z][a-z]([a-z]+)\\d+', 'us\\1', vertica_cluster.name.lower())

    default_task_args = {
        "task_name_suffix":
        pipeline_suffix,
        "scrum_team":
        scrum_team,
        "resources":
        TaskServicePodResources().small(),
        "retries":
        retries,
        "retry_delay":
        retry_delay,
        "task_execution_timeout":
        task_execution_timeout,
        "vertica_cluster":
        vertica_cluster,
        "k8s_sovereign_connection_helper":
        k8s_sovereign_connection_helper,
        "task_concurrency":
        1,
        "configuration_overrides": {
            "RtiPipeAgileTask.CollectLogFilesEnabledClusters": "",
            "RtiPipeAgileTask.EnableRTBFeatureParityColumns": "true",
            "RtiPipePhaseTaskConfig.OverrideConfigViaSecret": "test/use-dataops003-credentials" + secret_suffix,
            "RtiPipePhaseTaskConfig.SparkJobSecretName": "test/use-dataops003-credentials" + secret_suffix,
            "UsePrefixedConfigurationForAwsKeys": "true",
        },
        "telnet_commands": [
            "try changeField RtiPhaseTrackingQueryProviderUtility.BatchLookbackInDays 31",
            "try changeField MsSqlImportColumnStoreIndexHelper.CommandTimeoutInSeconds 7200",
            "try changeField BaseSqlServerImportStrategy.CommandTimeoutInSeconds 7200",
            "try changeField MsSqlImportColumnStoreIndexHelper.ThresholdForPartitionToTriggerReorg 300000000",
            "try changeField VerticaCloudExportImportHelper.EnableCommandForVertica12Etl.Enabled true",
            "try changeField ElDoradoEmrClusterJobConfigurator.Ec2KeyName TheTradeDeskDeveloper_Agiles",
        ],
    }

    if vertica_cluster == VerticaCluster.USWest01:
        default_task_args["telnet_commands"].append(
            "try changeField VerticaCloudExportImportHelper.EnableCommandForVertica12West02Ui.Enabled true"
        )

        default_task_args["telnet_commands"].append("try changeField VerticaCloudExportImportHelper.VerticaExportTimeoutInSeconds 10800")

        default_task_args["configuration_overrides"]["RtiPipePhaseTaskConfig.OverrideVerticaSubCluster"] = "USWest01sc09"
        default_task_args["configuration_overrides"]["RtiPipePhaseTaskConfig.OverrideVerticaResourcePool"] = "rti_sc09"

    if vertica_cluster == VerticaCluster.USEast01:
        default_task_args["telnet_commands"].append(
            "try changeField VerticaCloudExportImportHelper.EnableCommandForVertica12East02Ui.Enabled true"
        )

        default_task_args["configuration_overrides"]["RtiPipePhaseTaskConfig.OverrideVerticaSubCluster"] = "USEast01sc09"
        default_task_args["configuration_overrides"]["RtiPipePhaseTaskConfig.OverrideVerticaResourcePool"] = "rti_sc09"

    log_collection_task = OpTask(op=TaskServiceOperator(task_name="RtiRawLogCollectionTask", **default_task_args))

    rti_pipe_task = OpTask(op=TaskServiceOperator(task_name="RtiPipeAgileTask", **default_task_args))

    ttd_dag >> log_collection_task >> rti_pipe_task

    if k8s_sovereign_connection_helper == aws:
        # main cluster DAGs
        sql_import_live_dag = TtdDag(
            base_task_name + TaskServiceOperator.format_task_name('sql-import', pipeline_suffix),
            default_args=default_args,
            schedule_interval=job_schedule_interval,
            slack_channel=alert_channel,
            tags=["TaskService", scrum_team.jira_team],
            max_active_runs=max_active_runs,
            start_date=start_date,
            depends_on_past=False,
            run_only_latest=False,
        )

        sql_import_non_live_dag = TtdDag(
            base_task_name + TaskServiceOperator.format_task_name('sql-import-non-live', pipeline_suffix),
            default_args=default_args,
            schedule_interval=job_schedule_interval,
            slack_channel=alert_channel,
            tags=["TaskService", scrum_team.jira_team],
            max_active_runs=max_active_runs,
            start_date=start_date,
            depends_on_past=False,
            run_only_latest=False,
        )

        data_correction_dag = TtdDag(
            base_task_name + TaskServiceOperator.format_task_name('data-correction', pipeline_suffix),
            default_args=default_args,
            schedule_interval=job_schedule_interval,
            slack_channel=alert_channel,
            tags=["TaskService", scrum_team.jira_team],
            max_active_runs=10,  # sql import might be very late and I want to allow many final agg running anyway
            start_date=start_date,
            depends_on_past=False,
            run_only_latest=False,
        )

        parquet_import_dag = TtdDag(
            base_task_name + TaskServiceOperator.format_task_name('parquet-import', pipeline_suffix),
            default_args=default_args,
            schedule_interval=job_schedule_interval,
            slack_channel=alert_channel,
            tags=["TaskService", scrum_team.jira_team],
            max_active_runs=max_active_runs,
            start_date=start_date,
            depends_on_past=False,
            run_only_latest=False,
        )

        export_to_parquet_task = create_phase_task_operator(
            default_task_args, "FinalAggregationAndParquetExport",
            ["RawDataCollection", "PerformanceReportCopyReady", "IntegralDataReadyInPerformanceReport", "AdGroupBatchVerticaImport"], 100
        )
        rti_pipe_task >> export_to_parquet_task

        import_etl_to_vertica_task = create_phase_task_operator(
            default_task_args, "AdGroupBatchVerticaImport", ["AdGroupBatchVerticaExport"], 100,
            [cluster.name for cluster in etl_vertica_cluster_association[vertica_cluster]], ["Incremental"]
        )
        parquet_import_dag >> import_etl_to_vertica_task

        spark_runner_task_live = create_phase_task_operator(
            default_task_args,
            phase_origin_id="SqlServerSparkImportLive",
            depends_on=["FinalAggregationAndParquetExport"],
            phase_origin_types=["Incremental", "TrueUp", "LateData", "DataCorrection"],
            max_batches_per_run=50,
        )
        spark_runner_task_non_live = create_phase_task_operator(
            default_task_args,
            phase_origin_id="SqlServerSparkImportNonLive",
            depends_on=["SqlServerSparkImportLive"],
            phase_origin_types=["Incremental", "TrueUp", "LateData", "DataCorrection"],
            max_batches_per_run=50,
        )
        sql_import_live_dag >> spark_runner_task_live
        sql_import_non_live_dag >> spark_runner_task_non_live

        export_to_parquet_task_dc = create_phase_task_operator(
            default_task_args, "FinalAggregationAndParquetExport", ["PerformanceReportCopyReady"], 8, phase_origin_types=["DataCorrection"]
        )

        data_correction_dag >> export_to_parquet_task_dc

        globals()[sql_import_live_dag.airflow_dag.dag_id] = sql_import_live_dag.airflow_dag
        globals()[sql_import_non_live_dag.airflow_dag.dag_id] = sql_import_non_live_dag.airflow_dag
        globals()[data_correction_dag.airflow_dag.dag_id] = data_correction_dag.airflow_dag
        globals()[parquet_import_dag.airflow_dag.dag_id] = parquet_import_dag.airflow_dag
    else:
        export_etl_to_parquet_task = create_phase_task_operator(
            default_task_args, "AdGroupBatchVerticaExport", ["RawDataCollection"], 100, phase_origin_types=["Incremental"]
        )
        rti_pipe_task >> export_etl_to_parquet_task

    globals()[ttd_dag.airflow_dag.dag_id] = ttd_dag.airflow_dag
