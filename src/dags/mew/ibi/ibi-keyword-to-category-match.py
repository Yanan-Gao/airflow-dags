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

ibi_kwd_to_match_ttddag = TtdDag(
    dag_id="ibi-keyword-to-category-match",
    start_date=datetime(2024, 9, 12, 0, 0),
    schedule_interval='0 6 * * *',  # run on 06:00 UTC every day
    depends_on_past=False,
    slack_channel=slack_groups.MEW.team.alarm_channel,
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

spark_options_list = [("executor-memory", "14G"), ("executor-cores", "7"), ("driver-memory", "30G"), ("driver-cores", "10"),
                      ("conf", "spark.blacklist.enabled=true"), ("conf", "spark.blacklist.killBlacklistedExecutors=true"),
                      ("conf", "spark.executor.memoryOverhead=9000"), ('conf', 'spark.driver.memoryOverhead=9000'),
                      ("conf", "spark.dynamicAllocation.maxExecutors=100"), ("conf", "spark.dynamicAllocation.enable=true"),
                      ("conf", "spark.sql.shuffle.partitions=10000"), ("conf", "spark.yarn.maxAppAttempts=1")]

ibi_options_list = [('date', '{{ds}}'), ('CategoryParsingWindow', 10)]

cluster_tags = {
    "process": "ibi-keyword-to-category-match",
    "Team": slack_groups.MEW.team.jira_team,
}

exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        EmrInstanceType("i3.8xlarge", 4, 16).with_ebs_size_gb(500).with_fleet_weighted_capacity(2),
        M5.m5_24xlarge().with_ebs_size_gb(500).with_fleet_weighted_capacity(4),
        R5.r5_8xlarge().with_ebs_size_gb(500).with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=40
)

cluster = EmrClusterTask(
    name="IbiKeywordToCategoryMatch_Cluster",
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

ibi_step = EmrJobTask(
    name="IBI_Keyword_To_Category_Matching",
    class_name="com.thetradedesk.jobs.insights.ibi.IbiKeywordToCategoryMatch",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=ibi_options_list,
    configure_cluster_automatically=True,
    executable_path=exec_jar
)

page_level_cluster = EmrClusterTask(
    name="PageLevelIbiCategoryMatching_Cluster",
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
    enable_prometheus_monitoring=True,
)

page_level_step = EmrJobTask(
    name="IBI_Category_Matching",
    class_name="com.thetradedesk.jobs.insights.ibi.PageLevelIbiCategoryMatching",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=ibi_options_list,
    configure_cluster_automatically=True,
    executable_path=exec_jar
)

cluster.add_sequential_body_task(ibi_step)
page_level_cluster.add_sequential_body_task(page_level_step)
ibi_kwd_to_match_ttddag >> cluster >> page_level_cluster
DAG = ibi_kwd_to_match_ttddag.airflow_dag
