from datetime import timedelta, datetime

from dags.tv.constants import FORECAST_JAR_PATH
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import PFX

run_date = """{{ data_interval_start.to_date_string() }}"""

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

jar_path = FORECAST_JAR_PATH

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_4xlarge().with_fleet_weighted_capacity(16),
        R6g.r6g_8xlarge().with_fleet_weighted_capacity(32),
        R6g.r6g_12xlarge().with_fleet_weighted_capacity(48),
        R6g.r6g_16xlarge().with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=960,
)

dag = TtdDag(
    dag_id="deal-forecasting-data",
    start_date=datetime(year=2025, month=5, day=28),
    schedule_interval="0 6 * * *",
    max_active_runs=1,
    slack_channel=PFX.team.alarm_channel,
    tags=[PFX.team.jira_team],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30),
    default_args={"owner": "PFX"},
    dag_tsg=None,
)

emr_cluster_step = EmrClusterTask(
    name="deal-desk-forecasting-cpm-data",
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags={
        "Team": PFX.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri=None,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True,
    maximize_resource_allocation=True
)

daily_aggregation_step = EmrJobTask(
    name="deal-desk-forecasting-cpm-data-daily",
    executable_path=jar_path,
    class_name="com.thetradedesk.etlforecastjobs.preprocessing.dealdesk.CreateDailyCPMAndWinRateDataset",
    eldorado_config_option_pairs_list=[
        ("date", run_date),
        ("enableLogging", "true"),
        ("ttd.ds.HourlyCPMAndWinRateDataset.isInChain", "true"),
        ("ttd.ds.DailyCPMAndWinRateDataset.isInChain", "true"),
    ],
    maximize_resource_allocation=True,
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.enableVectorizedReader=false")],
    # Expected duration is 3-4 hours
    timeout_timedelta=timedelta(hours=6)
)

weekly_aggregation_step = EmrJobTask(
    name="deal-desk-forecasting-cpm-data-weekly",
    executable_path=jar_path,
    class_name="com.thetradedesk.etlforecastjobs.preprocessing.dealdesk.CreateWeeklyCPMAndWinRateDataset",
    eldorado_config_option_pairs_list=[
        ("date", run_date),
        ("enableLogging", "true"),
        ("ttd.ds.DailyJoinedCPMAndWinRateDataset.isInChain", "true"),
    ],
    maximize_resource_allocation=True,
    # Step generally takes a few minutes at most
    timeout_timedelta=timedelta(minutes=30)
)

emr_cluster_step.add_parallel_body_task(daily_aggregation_step)

emr_cluster_step.add_sequential_body_task(weekly_aggregation_step)

dag >> emr_cluster_step

adag = dag.airflow_dag
