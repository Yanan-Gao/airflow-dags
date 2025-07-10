from datetime import datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack import slack_groups

ttddag = TtdDag(
    dag_id="ibi-contextual",
    start_date=datetime(2024, 9, 12, 0, 0),
    schedule_interval='45 6 * * *',  # run on 06:45 UTC every day
    slack_channel=slack_groups.MEASURE_TASKFORCE_MEX.team.alarm_channel,
    tags=['measurement'],
    run_only_latest=True,
    retries=2
)

instance_configuration_spark_log4j = {
    'log4j.rootCategory': 'WARN, console',
    'log4j.appender.console': 'org.apache.log4j.ConsoleAppender',
    'log4j.appender.console.layout': 'org.apache.log4j.PatternLayout',
    'log4j.appender.console.layout.ConversionPattern': '%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n',
    'log4j.appender.console.target': 'System.err',
}

spark_options_list = [("conf", "spark.driver.maxResultSize=5g")]

options_list = [('date', '{{ds}}'), ('ContextualWindow', 4)]

cluster_tags = {
    "process": "ibi-contextual",
    "Team": slack_groups.MEASURE_TASKFORCE_MEX.team.jira_team,
}

exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        EmrInstanceType("i3.16xlarge", 64, 488).with_fleet_weighted_capacity(2),
        EmrInstanceType("i3.8xlarge", 32, 244).with_fleet_weighted_capacity(1),
        R5.r5_16xlarge().with_fleet_weighted_capacity(2),
        R5.r5_24xlarge().with_fleet_weighted_capacity(4)
    ],
    on_demand_weighted_capacity=100
)

cluster = EmrClusterTask(
    name="IBI_Cluster",
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
    name="IBI",
    class_name="com.thetradedesk.jobs.insights.ibi.ContextualIbi",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=options_list,
    configure_cluster_automatically=True,
    executable_path=exec_jar
)

cluster.add_sequential_body_task(step)
ttddag >> cluster
DAG = ttddag.airflow_dag
