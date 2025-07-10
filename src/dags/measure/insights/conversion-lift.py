from datetime import datetime, timedelta

from dags.idnt.log4j import Log4j
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups

ttddag = TtdDag(
    dag_id="insights-conversion-lift",
    start_date=datetime(2024, 9, 11, 0, 0),
    schedule_interval='0 7 * * *',  # run on 07:00 UTC every day
    depends_on_past=False,
    slack_channel=slack_groups.MEASURE_TASKFORCE_LIFT.team.alarm_channel,
    tags=['measurement']
)

spark_options = [("conf", "spark.kryoserializer.buffer.max=2047m"), ("conf", "spark.driver.maxResultSize=0"),
                 ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"), ("conf", "spark.executor.memoryOverhead=10g"),
                 ("conf", "spark.yarn.maxAppAttempts=1"), ("conf", "spark.executor.heartbeatInterval=120000"),
                 ("conf", "spark.network.timeout=1800000"), ("conf", "fs.s3.maxRetries=30"), ("driver-cores", "12"),
                 ("driver-memory", "24G")]

instance_configuration_spark_log4j = Log4j.get_default_configuration() | {
    # Set INFO logging for all sub-packages under com.thetradedesk.jobs.insights.conversionlift
    "log4j.logger.com.thetradedesk.jobs.insights": "INFO, insightsConsole",
    "log4j.additivity.com.thetradedesk.jobs.insights": "false",
    'log4j.appender.insightsConsole': 'org.apache.log4j.ConsoleAppender',
    'log4j.appender.insightsConsole.layout': 'org.apache.log4j.PatternLayout',
    'log4j.appender.insightsConsole.layout.ConversionPattern': '%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n',
    'log4j.appender.insightsConsole.target': 'System.out',
}

cluster_tags = {
    "process": "conversion-lift",
    "Team": slack_groups.MEASURE_TASKFORCE_LIFT.team.jira_team,
}

common_options = [('date', "{{ ds }}")]
outlier_option = [('outlier_threshold', 0.99)]

# for production testing, set to 's3://ttd-build-artefacts/eldorado/mergerequests/BRANCH_NAME/latest/eldorado-measure-assembly.jar'
exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

# bid aggregation
bids_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
bids_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=60,
    spot_weighted_capacity=0
)
bids_cluster_task = EmrClusterTask(
    name="bids-cluster",
    core_fleet_instance_type_configs=bids_core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=bids_master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
bids_step = EmrJobTask(
    name="bids",
    class_name="com.thetradedesk.jobs.insights.conversionlift.BidsDailyAgg",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
bids_cluster_task.add_sequential_body_task(bids_step)

# experiment contamination alarm
contamination_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
contamination_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=30,
    spot_weighted_capacity=0
)
contamination_cluster_task = EmrClusterTask(
    name="contamination-alarm-cluster",
    core_fleet_instance_type_configs=contamination_core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=contamination_master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
contamination_step = EmrJobTask(
    name="contamination-alarm",
    class_name="com.thetradedesk.jobs.insights.conversionlift.ExperimentContaminationAlarm",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
contamination_cluster_task.add_sequential_body_task(contamination_step)

# conversion aggregation
conversions_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
conversions_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=60,
    spot_weighted_capacity=0
)
conversions_cluster_task = EmrClusterTask(
    name="conversions-cluster",
    master_fleet_instance_type_configs=conversions_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=conversions_core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
conversions_step = EmrJobTask(
    name="conversions",
    class_name="com.thetradedesk.jobs.insights.conversionlift.ConversionsDailyAgg",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
conversions_cluster_task.add_sequential_body_task(conversions_step)

# offline universe
offline_universe_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
offline_universe_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=30,
    spot_weighted_capacity=0
)
offline_universe_cluster_task = EmrClusterTask(
    name="offline-universe-cluster",
    master_fleet_instance_type_configs=offline_universe_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=offline_universe_core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
offline_universe_step = EmrJobTask(
    name="offline-universe",
    class_name="com.thetradedesk.jobs.insights.conversionlift.OfflineUniverse",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
offline_universe_cluster_task.add_sequential_body_task(offline_universe_step)

# timeseries
timeseries_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
timeseries_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=40,
    spot_weighted_capacity=0
)
timeseries_cluster_task = EmrClusterTask(
    name="timeseries-cluster",
    master_fleet_instance_type_configs=timeseries_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=timeseries_core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
timeseries_step = EmrJobTask(
    name="timeseries",
    class_name="com.thetradedesk.jobs.insights.conversionlift.TimeSeries",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options,
    timeout_timedelta=timedelta(hours=5),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
timeseries_cluster_task.add_sequential_body_task(timeseries_step)

# daily calculation prepare
prep_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
prep_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=60,
    spot_weighted_capacity=0
)
prep_cluster_task = EmrClusterTask(
    name="prep-cluster",
    master_fleet_instance_type_configs=prep_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=prep_core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
prep_step = EmrJobTask(
    name="prep",
    class_name="com.thetradedesk.jobs.insights.conversionlift.DailyCalculationPrepare",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options + outlier_option,
    timeout_timedelta=timedelta(hours=8),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
prep_cluster_task.add_sequential_body_task(prep_step)

# daily calculation run
main_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
main_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5.r5_24xlarge().with_ebs_size_gb(3600).with_fleet_weighted_capacity(1),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=60,
    spot_weighted_capacity=0
)
main_cluster_task = EmrClusterTask(
    name="main-cluster",
    master_fleet_instance_type_configs=main_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=main_core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=[{
        "Classification": "spark-log4j2",
        "Properties": instance_configuration_spark_log4j
    }],
)
main_step = EmrJobTask(
    name="main",
    class_name="com.thetradedesk.jobs.insights.conversionlift.DailyCalculationRun",
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=common_options + outlier_option,
    timeout_timedelta=timedelta(hours=8),
    configure_cluster_automatically=True,
    executable_path=exec_jar
)
main_cluster_task.add_sequential_body_task(main_step)

# dependencies
ttddag >> bids_cluster_task
ttddag >> conversions_cluster_task
ttddag >> offline_universe_cluster_task
bids_cluster_task >> contamination_cluster_task
offline_universe_cluster_task >> prep_cluster_task
bids_cluster_task >> prep_cluster_task
conversions_cluster_task >> prep_cluster_task
conversions_cluster_task >> timeseries_cluster_task
offline_universe_cluster_task >> timeseries_cluster_task
prep_cluster_task >> main_cluster_task
timeseries_cluster_task >> main_cluster_task

DAG = ttddag.airflow_dag
