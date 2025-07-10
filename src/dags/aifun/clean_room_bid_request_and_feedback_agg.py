from datetime import datetime, timedelta

from datasources.sources.common_datasources import CommonDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import AIFUN
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask

from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.openlineage import OpenlineageConfig

jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-cleanroom.jar"
aws_region = "us-east-1"
log_uri = "s3://thetradedesk-useast-avails/emr-logs"
core_fleet_capacity = 1024

standard_cluster_tags = {'Team': AIFUN.team.jira_team}

std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
    spot_weighted_capacity=0
)

std_core_instance_types = [
    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
]

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

clean_room_dag = TtdDag(
    dag_id="clean-room-bid-request-and-feedback-agg",
    start_date=datetime(2024, 4, 1, 1, 0),
    schedule_interval=timedelta(days=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_tags=AIFUN.team.jira_team,
    enable_slack_alert=False
)

# EMR cluster definition
emr_cluster = EmrClusterTask(
    name="CleanRoomBidRequestBidFeedbackAgg",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Bid-Request-Bid-Feedback-agg"
    },
    enable_prometheus_monitoring=True,
    enable_spark_history_server_stats=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region
)

# EMR step to run clean room agg
clean_room_agg_step = EmrJobTask(
    cluster_specs=emr_cluster.cluster_specs,
    name="CleanRoomBidRequestFeedbackAgg",
    class_name="com.thetradedesk.availspipeline.spark.cleanroom.jobs.CleanRoomBidRequestFeedbackAgg",
    executable_path=jar_path,
    additional_args_option_pairs_list=[],
    eldorado_config_option_pairs_list=[('date', '{{ execution_date.strftime(\"%Y-%m-%d\") }}')],
    region_name=aws_region,
    openlineage_config=OpenlineageConfig(enabled=False)
)

emr_cluster.add_parallel_body_task(clean_room_agg_step)

clean_room_dag >> emr_cluster

# task to wait for input dataset - do this before spinning up EMR cluster
wait_for_input_data = DatasetCheckSensor(
    dag=clean_room_dag.airflow_dag,
    ds_date="{{ execution_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    datasets=[CommonDatasources.rtb_bidfeedback_v5, CommonDatasources.rtb_bidrequest_v5]
)
wait_for_input_data >> emr_cluster.first_airflow_op()

dag = clean_room_dag.airflow_dag
