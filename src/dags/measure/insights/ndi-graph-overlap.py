from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack import slack_groups

job_schedule_interval = "0 8 * * 2"
job_start_date = datetime(2024, 12, 10)

dag = TtdDag(
    dag_id="ndi-graph-overlap-all-devices",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    run_only_latest=True,
    slack_channel=slack_groups.MEASURE_TASKFORCE_MEX.team.alarm_channel,
    tags=["measurement"],
    retries=1
)

# Set to your custom jar file if testing. Otherwise set it to None for production jar
# spark3_jar_path = 's3://ttd-build-artefacts/eldorado/mergerequests/vsh-MEASURE-6135-NDI-migration-to-spark3/latest/eldorado-measure-assembly.jar'
spark3_jar_path = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

cluster_tags = {
    "process": "ndi-graph-overlap",
    "Team": slack_groups.MEASURE_TASKFORCE_MEX.team.jira_team,
}

instance_configuration_spark_log4j = {
    'log4j.rootCategory': 'WARN, console',
    'log4j.appender.console': 'org.apache.log4j.ConsoleAppender',
    'log4j.appender.console.layout': 'org.apache.log4j.PatternLayout',
    'log4j.appender.console.layout.ConversionPattern': '%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n',
    'log4j.appender.console.target': 'System.err',
}

spark_options_list = [("conf", "spark.dynamicAllocation.enable=true"), ("conf", "spark.sql.shuffle.partitions=10000"),
                      ("conf", "spark.yarn.maxAppAttempts=1"), ("conf", "spark.driver.maxResultSize=10G"),
                      ("conf", "spark.executor.memoryOverhead=9000")]

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_24xlarge().with_ebs_size_gb(500).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=30
)

cluster = EmrClusterTask(
    name="ndi-graph-overlap-all-devices",
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
    enable_prometheus_monitoring=True
)

step = EmrJobTask(
    name="ndi-graph-overlap-all-devices",
    class_name="com.thetradedesk.jobs.insights.newdevices.GraphOverlapNewDeviceAdditions",
    additional_args_option_pairs_list=spark_options_list,
    configure_cluster_automatically=True,
    executable_path=spark3_jar_path,
    timeout_timedelta=timedelta(hours=24)
)

filter_devices_step = EmrJobTask(
    name="ndi-filter-by-deviceModels",
    class_name="com.thetradedesk.jobs.insights.newdevices.NewDevicesIndicatorFilteredDeviceModels",
    additional_args_option_pairs_list=spark_options_list,
    configure_cluster_automatically=True,
    executable_path=spark3_jar_path,
    timeout_timedelta=timedelta(hours=24)
)

cluster.add_parallel_body_task(step)
cluster.add_parallel_body_task(filter_devices_step)
step >> filter_devices_step

dag >> cluster
DAG = dag.airflow_dag
