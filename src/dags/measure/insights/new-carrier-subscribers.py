from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ec2.emr_instance_types.storage_optimized.i3en import I3en
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from datetime import datetime
from ttd.slack.slack_groups import MEASURE_TASKFORCE_MEX

job_schedule_interval = "0 12 * * 2"
job_start_date = datetime(2025, 3, 1)
job_name = "new-carrier-subscribers"
num_cores = 1600

# Set to your custom jar file if testing. Otherwise, set it to None for production jar
# spark3_jar_path = 's3://ttd-build-artefacts/eldorado/mergerequests/dgz-MEASURE-5391-add-unit-tests-NCS-switched-devices/latest/el-dorado-assembly.jar'
spark3_jar_path = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        I3.i3_16xlarge().with_fleet_weighted_capacity(64),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(96),
        M5d.m5d_24xlarge().with_fleet_weighted_capacity(96),
        M6g.m6gd_16xlarge().with_fleet_weighted_capacity(64),
        I3en.i3en_24xlarge().with_fleet_weighted_capacity(96),
    ],
    spot_weighted_capacity=num_cores
)

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    run_only_latest=True,
    slack_tags=MEASURE_TASKFORCE_MEX.team.sub_team,
    slack_channel=MEASURE_TASKFORCE_MEX.team.alarm_channel,
    tags=["measurement"]
)

cluster = EmrClusterTask(
    name=job_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": MEASURE_TASKFORCE_MEX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    enable_prometheus_monitoring=False,
    cluster_auto_termination_idle_timeout_seconds=40 * 60,
)

weekly_new_device_activations_task = EmrJobTask(
    name="ncs-weekly-new-device-activations",
    class_name="jobs.insights.newCarrierSubscribers.WeeklyNewDeviceActivations",
    executable_path=spark3_jar_path,
    configure_cluster_automatically=True
)

weekly_switched_devices_task = EmrJobTask(
    name="ncs-weekly-switched-devices",
    class_name="jobs.insights.newCarrierSubscribers.SwitchedCarrierSubscribers",
    executable_path=spark3_jar_path,
    configure_cluster_automatically=True
)

weekly_indicator_task = EmrJobTask(
    name="ncs-weekly-indicator",
    class_name="jobs.insights.newCarrierSubscribers.NewCarrierSubscribersIndicator",
    executable_path=spark3_jar_path,
    configure_cluster_automatically=True
)

cluster.add_parallel_body_task(weekly_new_device_activations_task)
cluster.add_parallel_body_task(weekly_switched_devices_task)
cluster.add_parallel_body_task(weekly_indicator_task)

weekly_new_device_activations_task >> weekly_switched_devices_task >> weekly_indicator_task

dag >> cluster
ncs_dag = dag.airflow_dag
