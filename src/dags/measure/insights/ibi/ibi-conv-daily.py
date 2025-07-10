from datetime import datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups

ttddag = TtdDag(
    dag_id="ibi-conv-daily",
    start_date=datetime(2024, 9, 26, 0, 0),
    schedule_interval='0 6 * * *',  # run on 06:00 UTC every day
    depends_on_past=False,
    slack_channel=slack_groups.MEASURE_TASKFORCE_MEX.team.alarm_channel,
    tags=['measurement'],
    run_only_latest=True,
    retries=1
)

instance_configuration_spark_log4j = {
    'log4j.rootCategory': 'WARN, console',
    'log4j.appender.console': 'org.apache.log4j.ConsoleAppender',
    'log4j.appender.console.layout': 'org.apache.log4j.PatternLayout',
    'log4j.appender.console.layout.ConversionPattern': '%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n',
    'log4j.appender.console.target': 'System.err',
}

spark_options_list = [("conf", "spark.dynamicAllocation.maxExecutors=250"), ("conf", "spark.dynamicAllocation.enable=true"),
                      ("conf",
                       "spark.sql.shuffle.partitions=5000"), ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", 'spark.driver.maxResultSize=0'), ("conf", "spark.driver.cores=5"),
                      ("conf", "spark.executor.memoryOverhead=3G"), ("conf", "spark.executor.cores=4"), ("conf", "spark.driver.memory=35G"),
                      ("conf", "spark.driver.memoryOverhead=4G"), ("conf", "spark.executor.memory=35G")]

options_list = [('date', '{{ds}}')]

cluster_tags = {
    "process": "ibi-conv-daily",
    "Team": slack_groups.MEASURE_TASKFORCE_MEX.team.jira_team,
}

exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(100).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        EmrInstanceType("r6gd.4xlarge", 16, 128).with_fleet_weighted_capacity(1),
        EmrInstanceType("r6id.4xlarge", 16, 128).with_fleet_weighted_capacity(1)
    ],
    spot_weighted_capacity=0,
    on_demand_weighted_capacity=64
)

cluster = EmrClusterTask(
    name="identity-inferred-brand-impact-conv-daily",
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }]  # remove this parameter once you uncomment below line
    # instance_configuration_spark_log4j=instance_configuration_spark_log4j
    # ^this is temporary workaround until airflow team fixes this so leaving this line without commenting. Thread: https://thetradedesk.slack.com/archives/CK2QM4EHH/p1745367758380879
)

step = EmrJobTask(
    name="identity-inferred-brand-impact-conv-daily",
    class_name="com.thetradedesk.jobs.insights.ibi.ConversionDaily",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=options_list,
    configure_cluster_automatically=False,
    executable_path=exec_jar
)

cluster.add_sequential_body_task(step)
ttddag >> cluster
DAG = ttddag.airflow_dag
