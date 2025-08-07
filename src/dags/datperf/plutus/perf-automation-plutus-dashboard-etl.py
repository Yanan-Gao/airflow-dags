import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DATPERF, AUDAUTO
from dags.datperf.utils.spark_config_utils import get_spark_args
from dags.datperf.datasets import adgroup_dataset, campaign_dataset, \
    plutus_dataset, hades_campaignadjustmentspacing_dataset

# Instance configuration
instance_type = R5.r5_8xlarge()
base_ebs_size = 1024
on_demand_weighted_capacity = 10

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)
spark_args = get_spark_args(cluster_params)

# Jar
PLUTUS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar"

plutus_dashboards_etl = TtdDag(
    dag_id="perf-automation-plutus-dashboard-etl",
    start_date=datetime(2024, 6, 7),
    schedule_interval=timedelta(hours=24),
    max_active_runs=2,
    run_only_latest=False,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/yrMMCQ',
    retries=0,
    tags=['DATPERF', "Plutus"],
    enable_slack_alert=False,
    default_args={"owner": "DATPERF"},
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutus_dashboards_etl.airflow_dag

# These two datasets are read for data_interval_start & the next day
# So we run them with `lookback=1` and `.add(days=1)`
twodays_dataset_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='twodays_datasets_sensor',
    poke_interval=60 * 30,
    timeout=60 * 60 * 12,
    lookback=1,
    ds_date="{{ data_interval_start.add(days=1).to_datetime_string() }}",
    datasets=[campaign_dataset, adgroup_dataset]
)

plutus_dataset_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='plutus_dataset_sensor',
    poke_interval=60 * 30,
    timeout=60 * 60 * 12,
    ds_date="{{ data_interval_start.to_datetime_string() }}",
    datasets=[plutus_dataset, hades_campaignadjustmentspacing_dataset]
)

plutus_dashboard_cluster = EmrClusterTask(
    name="plutus_dashboard_cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[instance_type.with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            instance_type.with_ebs_size_gb(base_ebs_size).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_16xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=on_demand_weighted_capacity
    ),
    enable_prometheus_monitoring=True,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

campaign_da_plutus_dash_job = EmrJobTask(
    name="CampaignDAPlutusDashboardDataProcessor",
    class_name="job.dashboard.CampaignDAPlutusDashboardDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

da_plutus_dash_job = EmrJobTask(
    name="DAPlutusDashboardDataProcessor",
    class_name="job.dashboard.DAPlutusDashboardDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

plutus_dash_job = EmrJobTask(
    name="PlutusDashboardDataProcessor",
    class_name="job.dashboard.PlutusDashboardDataProcessor",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

backoff_monitoring_job = EmrJobTask(
    name="BackoffMonitoringJob",
    class_name="job.monitoring.BackoffMonitoringJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=[('date', "{{ds}}")],
    executable_path=PLUTUS_JAR,
    timeout_timedelta=timedelta(hours=6)
)

plutus_dashboard_cluster.add_parallel_body_task(campaign_da_plutus_dash_job)
plutus_dashboard_cluster.add_parallel_body_task(da_plutus_dash_job)
plutus_dashboard_cluster.add_parallel_body_task(plutus_dash_job)
plutus_dashboard_cluster.add_parallel_body_task(backoff_monitoring_job)

plutus_dashboards_etl >> plutus_dashboard_cluster
[twodays_dataset_sensor, plutus_dataset_sensor] >> plutus_dashboard_cluster.first_airflow_op()
