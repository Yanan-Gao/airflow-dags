from datetime import datetime, timedelta

from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, DEAL_QUALITY_JAR
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration
from ttd.ec2.emr_instance_types.general_purpose.m6a import M6a
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.ec2.cluster_params import Defaults
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.interop.vertica_import_operators import LogTypeFrequency
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.dmg.utils import get_jar_file_path

job_name = "dmg-multilevel-deal-metric-export"
start_date = datetime(2025, 6, 16, 0, 0)
env = TtdEnvFactory.get_from_system()
dataset_date_format = "%Y-%m-%d 00:00:00"
gating_type_id_advertiser_deal_quality_metrics_mssql = 2000668


def get_run_date_time_str(date_format="%Y-%m-%dT%H:00:00", days_to_add=0):
    return f"{{{{ ((macros.datetime.strptime(dag_run.conf[\"run_date_time\"], \"%Y-%m-%dT%H:%M:%S\") if dag_run.run_type==\"manual\" else data_interval_start) + macros.timedelta(days={days_to_add})).strftime(\"{date_format}\") }}}}"


run_date_time_str = get_run_date_time_str()
dataset_date_time_str = get_run_date_time_str(dataset_date_format)

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1),
    retries=0,
    max_active_runs=2,
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[DEAL_MANAGEMENT.team.name, "DealQuality"],
    run_only_latest=None
)

dag = ttd_dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M6a.m6a_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6a.m6a_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_iops(Defaults.MAX_GP3_IOPS)
        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
    ],
    on_demand_weighted_capacity=M6a.m6a_8xlarge().cores * 16
)

datasets = [
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics/cpc/v=3/ExportDealQualityCampaignPerformanceIntermediateOutput",
        data_name="VerticaAws",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None,
    )
]

wait_for_input_data = OpTask(
    op=DatasetCheckSensor(
        dag=ttd_dag.airflow_dag,
        ds_date=dataset_date_time_str,
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 20,  # wait up to 20 hours,
        datasets=datasets
    )
)

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    retries=0,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": DEAL_MANAGEMENT.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }]
)

job_task = EmrJobTask(
    name=job_name + "-job-task",
    retries=0,
    class_name="com.thetradedesk.deals.pipelines.dealquality.exportmetrics.MultiLevelDealMetricsJob",
    executable_path=get_jar_file_path(DEAL_QUALITY_JAR),
    additional_args_option_pairs_list=[("conf", "spark.driver.maxResultSize=64g")],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[("runDateTime", run_date_time_str)]
)

open_gate_task_advertiser_deal_quality_metrics_mssql = MsSqlImportFromCloud(
    name="Advertiser_Deal_Quality_Metrics_MSSQL_DataImport_OpenLWGate",
    gating_type_id=gating_type_id_advertiser_deal_quality_metrics_mssql,
    log_type_id=LogTypeFrequency.DAILY.value,
    log_start_time=run_date_time_str,
    mssql_import_enabled=True,
    job_environment=env,
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> wait_for_input_data >> cluster_task >> open_gate_task_advertiser_deal_quality_metrics_mssql
