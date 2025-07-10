from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

run_date = "{{ data_interval_start.to_date_string() }}"
FS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"

fs_user_bimp_etl_dag = TtdDag(
    dag_id="perf-automation-feature-store-user-bidimpression-feature-etl",
    start_date=datetime(2024, 7, 16, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#scrum-perf-auto-audience-alerts",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "FEATURE_STORE"]
)

adag = fs_user_bimp_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
bidsimpressions_data = HourGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/koav4/v=1/prod",
    data_name="bidsimpressions",
    date_format="year=%Y/month=%m/day=%d",
    hour_format="hourPart={hour}",
    env_aware=False,
    version=None,
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[bidsimpressions_data],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 5,
    )
)

###############################################################################
# clusters
###############################################################################
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
    ],
    on_demand_weighted_capacity=960,
)

fs_user_bidimpression_feature_cluster_task = EmrClusterTask(
    name="FS_User_BImp_ETL_Cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={'Team': AUDAUTO.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=60
)

fs_user_bid_impression_feature_generation_step = EmrJobTask(
    name="UserBidImpressionFeatureJob",
    class_name="com.thetradedesk.featurestore.jobs.UserImpressionFeatureJob",
    eldorado_config_option_pairs_list=[('date', run_date)],
    executable_path=FS_JAR,
    timeout_timedelta=timedelta(hours=8)
)
fs_user_bidimpression_feature_cluster_task.add_parallel_body_task(fs_user_bid_impression_feature_generation_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
fs_user_bimp_etl_dag >> dataset_sensor >> fs_user_bidimpression_feature_cluster_task >> final_dag_status_step
