from datetime import datetime, timedelta

from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python import PythonOperator

from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, DEAL_QUALITY_JAR
from dags.dmg.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.interop.vertica_import_operators import LogTypeFrequency
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dataset_date_format = "%Y-%m-%d 00:00:00"

job_name = "dmg-deal-quality-metric-export"
start_date = datetime(2025, 5, 20, 0, 0)
env = TtdEnvFactory.get_from_system()

gating_type_id = 2000535  # dbo.fn_enum_GatingType_ImportDealMetricsFromS3()
gating_type_id_rollup_deal_quality = 2000685

TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
logworkflow_prod_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'

logworkflow_connection = logworkflow_prod_connection if env == TtdEnvFactory.prod else logworkflow_sandbox_connection

decision_power_lookback_window = 7


def get_run_date_time_str(date_format="%Y-%m-%dT%H:00:00", days_to_add=0):
    return f"{{{{ ((macros.datetime.strptime(dag_run.conf[\"run_date_time\"], \"%Y-%m-%dT%H:%M:%S\") if dag_run.run_type==\"manual\" else data_interval_start) + macros.timedelta(days={days_to_add})).strftime(\"{date_format}\") }}}}"


run_date_time_str = get_run_date_time_str()
dataset_date_time_str = get_run_date_time_str(dataset_date_format)
java_options = [("runDateTime", run_date_time_str), ('decisionPowerLookBackWindow', f"{decision_power_lookback_window}"),
                ("ttd.ds.NonCompositeMetricDataset.isInChain", "true"), ("ttd.ds.ExportedMetricDataset.isInChain", "true"),
                ("ttd.ds.RawNonCompositeMetricsDataset.isInChain", "true"),
                ("ignoreMergeForMetricIds", "{{dag_run.conf.get('ignoreMergeForMetricIds', '')}}")]

if env == TtdEnvFactory.prodTest:
    upstream_ds_in_chain = ["DealAddressabilityDataset", "DealDecisionPowerComponentsDataset"]
    java_options += [(f"ttd.ds.{dataset}.isInChain", "{{ dag_run.conf.get('read_upstream_in_test', 'false') }}")
                     for dataset in upstream_ds_in_chain]

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1) if env == TtdEnvFactory.prod else None,
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[DEAL_MANAGEMENT.team.name, "DealQuality"],
    run_only_latest=True
)

dag = ttd_dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(M7g.m7g_4xlarge().cores),
        M7g.m7g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(M7g.m7g_8xlarge().cores),
        M7g.m7g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(M7g.m7g_12xlarge().cores),
        M7g.m7g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(M7g.m7g_16xlarge().cores)
    ],
    on_demand_weighted_capacity=M7g.m7g_16xlarge().cores * 10
)

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": DEAL_MANAGEMENT.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True
)

job_task = EmrJobTask(
    name=job_name + "-task",
    class_name="com.thetradedesk.deals.pipelines.dealquality.exportmetrics.DealQualityMetricJob",
    executable_path=get_jar_file_path(DEAL_QUALITY_JAR),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=java_options
)

cluster_task.add_parallel_body_task(job_task)

datasets = [
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics/pcs/v=2/ExportDealQualityPriceCompetitivenessScore",
        data_name="VerticaAws",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None,
    ),
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics/cpc/v=2/ExportDealQualityCampaignPerformanceComparisonScore",
        data_name="VerticaAws",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None,
    ),
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics/invu/v=2/ExportDealQualityInventoryUniquenessScore",
        data_name="VerticaAws",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None,
    ),
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics/addr/v=1",
        data_name="DealAddressability",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None,
        success_file=None,
    ),
    HourGeneratedDataset(
        bucket="thetradedesk-useast-vertica-parquet-export",
        path_prefix="VerticaExportRollupDealMetricsHourlyNonLateData",
        data_name="VerticaAws",
        version=None,
        env_aware=False
    ).with_check_type('day')
]

# task to wait for input dataset - do this before spinning up EMR cluster
wait_for_input_data = OpTask(
    op=DatasetCheckSensor(
        dag=ttd_dag.airflow_dag,
        ds_date=dataset_date_time_str,
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 20,  # wait up to 20 hours,
        datasets=datasets
    )
)

dailyDecisionPowerdataset = DateGeneratedDataset(
    bucket="ttd-deal-quality",
    path_prefix="DealQualityMetrics/ddp/v=2",
    data_name="DealDecisionPowerComponents",
    env_path_configuration=MigratedDatasetPathConfiguration(),
    version=None,
    success_file=None,
)

# Add an extra task to check for decision power which requires lookback
wait_for_decision_power_with_lookback = OpTask(
    op=DatasetCheckSensor(
        dag=ttd_dag.airflow_dag,
        ds_date=dataset_date_time_str,
        lookback=decision_power_lookback_window - 1,
        task_id="decision_power_with_lookback_dataset_check",
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 20,  # wait up to 12 hours,
        datasets=[dailyDecisionPowerdataset]
    )
)

deal_quality_metrics_dataset = DateGeneratedDataset(
    bucket="ttd-deal-quality",
    path_prefix="DealQualityMetrics/exported",
    data_name="",
    env_path_configuration=MigratedDatasetPathConfiguration(),
    version=2,
)
deal_quality_spend_weighted_metrics_dataset = DateGeneratedDataset(
    bucket="ttd-deal-quality",
    path_prefix="DealQualityMetrics/exported_spend_weighted",
    data_name="",
    env_path_configuration=MigratedDatasetPathConfiguration(),
    version=1,
)


###############################################################################
# functions - DataMover
###############################################################################
def _get_time_slot(dt: datetime):
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    return dt


def check_incoming_data_exists(**context):
    dt = _get_time_slot(context['data_interval_end'])
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    if not deal_quality_metrics_dataset.as_write().check_data_exist(cloud_storage, dt):
        raise AirflowNotFoundException("S3 _SUCCESS file not found for DealQualityMetrics!")
    if not deal_quality_spend_weighted_metrics_dataset.as_write().check_data_exist(cloud_storage, dt):
        raise AirflowNotFoundException("S3 _SUCCESS file not found for spend-weighted DealQualityMetrics!")
    return True


def _open_lwdb_gate(**context):
    dt = _get_time_slot(context['data_interval_end'])
    log_start_time = dt.strftime('%Y-%m-%d %H:00:00')
    ExternalGateOpen(
        mssql_conn_id=logworkflow_connection,
        sproc_arguments={
            'gatingType': gating_type_id,
            'grain': TaskBatchGrain_Daily,
            'dateTimeToOpen': log_start_time
        }
    )


###############################################################################
# operators - DataMover
###############################################################################
check_datasets_are_not_empty = OpTask(
    op=PythonOperator(
        task_id='check_datasets_are_not_empty',
        python_callable=check_incoming_data_exists,
        provide_context=True,
        dag=dag,
    )
)

open_lwdb_gate_task = OpTask(
    op=PythonOperator(
        task_id='open_lwdb_gate_task',
        python_callable=_open_lwdb_gate,
        provide_context=True,
        dag=dag,
    )
)

open_gate_task_rollup_deal_quality_mssql = MsSqlImportFromCloud(
    name="RollupDealQualityMetrics_MSSQL_DataImport_OpenLWGate",
    gating_type_id=gating_type_id_rollup_deal_quality,
    log_type_id=LogTypeFrequency.DAILY.value,
    log_start_time=run_date_time_str,
    mssql_import_enabled=True,
    job_environment=env,
)

ttd_dag >> wait_for_input_data >> wait_for_decision_power_with_lookback >> cluster_task >> check_datasets_are_not_empty >> open_lwdb_gate_task >> open_gate_task_rollup_deal_quality_mssql
