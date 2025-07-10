from datetime import datetime, timedelta

from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, DEAL_QUALITY_JAR
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r8g import R8g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration
from dags.dmg.utils import get_jar_file_path

job_name = "dmg-deal-quality-avails-based-metrics"
start_date = datetime(2025, 5, 20, 0, 0)
env = TtdEnvFactory.get_from_system()

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1) if env == TtdEnvFactory.prod else None,
    retries=0,
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[DEAL_MANAGEMENT.team.name, "DealQuality"],
    run_only_latest=True
)

dag = ttd_dag.airflow_dag

datasets = [
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics",
        data_name="OpenMarketAggregatedAvails",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=2,
        success_file=None,
    ),
    DateGeneratedDataset(
        bucket="ttd-deal-quality",
        path_prefix="DealQualityMetrics",
        data_name="DealAggregatedAvails",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=2,
        success_file=None,
    ),
    HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        env_aware=True,
        data_name="dooh-avails-agg/v=1",
        version=None,
        date_format="date=%Y%m%d",
        hour_format="hour={hour:0>2d}"
    )
]

check_required_datasets = OpTask(
    op=DatasetCheckSensor(
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler,
        task_id='check_required_datasets',
        datasets=datasets,
        ds_date="{{ data_interval_start.strftime('%Y-%m-%d 00:00:00')}}",
        timeout=60 * 60 * 16,  # 16 hours
        dag=dag
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R8g.r8g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R8g.r8g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_4xlarge().cores),
        R8g.r8g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_8xlarge().cores),
        R8g.r8g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_12xlarge().cores),
        R8g.r8g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(R8g.r8g_16xlarge().cores)
    ],
    on_demand_weighted_capacity=R8g.r8g_16xlarge().cores * 10
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
)

# TODO(DMG-3030): Align schedule and manual runs start and end dates.
config_options = [("runStartInclusive", "{{ data_interval_start.strftime(\"%Y-%m-%dT00:00:00\") }}"),
                  ("runEndExclusive", "{{ data_interval_start.add(days=1).strftime(\"%Y-%m-%dT00:00:00\") }}"), ('lookBackWindow', '7'),
                  ('outputPartitionCount', '100')]

addressability_job_task = EmrJobTask(
    name=job_name + "-addressability-job-task",
    retries=0,
    class_name="com.thetradedesk.deals.pipelines.dealquality.addressabilityunsampledavails.AddressabilityUnsampledAvailsJob",
    executable_path=get_jar_file_path(DEAL_QUALITY_JAR),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_options
)

forecasted_addressability_job_task = EmrJobTask(
    name=job_name + "-forecasting-addressability-job-task",
    retries=0,
    class_name="com.thetradedesk.deals.pipelines.dealquality.addressabilityunsampledavails.ForecastedAddressabilityJob",
    executable_path=get_jar_file_path(DEAL_QUALITY_JAR),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_options
)

cluster_task.add_parallel_body_task(addressability_job_task)
cluster_task.add_parallel_body_task(forecasted_addressability_job_task)

ttd_dag >> check_required_datasets >> cluster_task
