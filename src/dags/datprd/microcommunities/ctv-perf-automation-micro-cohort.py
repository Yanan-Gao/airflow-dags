# from airflow import DAG
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime, timedelta
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from datasources.datasources import Datasources
from ttd.slack.slack_groups import DATPRD

dag_id = 'perf-automation-micro-cohort'
job_start_date = datetime(2024, 8, 23, 4, 0, 0)


def orig_execution_date(date_format):
    return '{{ logical_date.strftime("$format$") }}'.replace("$format$", date_format)


def execution_date(days):
    # we use replace because format causes issues with {}
    return '{{ logical_date.add(days=$interval$).strftime(\"%Y-%m-%dT%H:00:00\") }}' \
        .replace("$interval$", str(days))


mc_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=timedelta(days=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/kQKdBw',
    slack_channel=DATPRD.team.alarm_channel,
    slack_tags=DATPRD.team.jira_team,
    tags=[DATPRD.team.jira_team],
    default_args={'owner': DATPRD.team.jira_team}
)

# Bid Request Agg
mc_etl_spark_options_list = [("executor-memory", "60G"), ("executor-cores", "5"),
                             ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=60G"),
                             ("conf", "spark.driver.core=5"), ("conf", "spark.sql.shuffle.partitions=2000"),
                             ("conf", "spark.driver.maxResultSize=10G")]

bidrequest_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='bidrequest_data_available',
        datasets=[Datasources.rtb_datalake.rtb_bidrequest_v5],
        ds_date='{{ logical_date.strftime("%Y-%m-%d 00:00:00") }}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 6,  # wait up to 6 hours
    )
)

bidrequest_agg_cluster = EmrClusterTask(
    name="mc_br_agg",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_16xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={"Team": DATPRD.team.jira_team},
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R5.r5_24xlarge().with_ebs_size_gb(6144).with_max_ondemand_price().with_fleet_weighted_capacity(6)
        ],
        on_demand_weighted_capacity=75
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2
)

# Bid Request Daily Agg Job
jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-tv-assembly.jar"

daily_agg_step = EmrJobTask(
    cluster_specs=bidrequest_agg_cluster.cluster_specs,
    name="mcDailyAggregation",
    class_name="jobs.ctv.microcommunities.DailyAggregation",
    executable_path=jar,
    additional_args_option_pairs_list=mc_etl_spark_options_list,
    eldorado_config_option_pairs_list=[("runTime", execution_date(1)), ("date", orig_execution_date('%Y-%m-%d'))]
)

# Bid Request Weekly Agg Job
multi_day_rollup = EmrJobTask(
    cluster_specs=bidrequest_agg_cluster.cluster_specs,
    name="mcMultiDayMerge",
    class_name="jobs.ctv.microcommunities.MultiDayMerge",
    executable_path=jar,
    additional_args_option_pairs_list=mc_etl_spark_options_list,
    eldorado_config_option_pairs_list=[("runTime", execution_date(1)), ("date", orig_execution_date('%Y-%m-%d'))]
)

# MC Segments
segment_creation_spark_options_list = [("executor-memory", "40G"), ("executor-cores", "5"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=40G"), ("conf", "spark.driver.core=5"),
                                       ("conf", "spark.sql.shuffle.partitions=400"), ("conf", "spark.driver.maxResultSize=6G")]

segment_cluster = EmrClusterTask(
    name="mc_percentile_and_segment_creation",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={"Team": DATPRD.team.jira_team},
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R5.r5_24xlarge().with_ebs_size_gb(6144).with_max_ondemand_price().with_fleet_weighted_capacity(6)
        ],
        on_demand_weighted_capacity=9
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2
)

# Percentile Config Job
percentile_step = EmrJobTask(
    cluster_specs=segment_cluster.cluster_specs,
    name="McPercentile",
    class_name="jobs.ctv.microcommunities.PercentileConfigJob",
    executable_path=jar,
    additional_args_option_pairs_list=segment_creation_spark_options_list,
    eldorado_config_option_pairs_list=[("date", orig_execution_date('%Y-%m-%d'))]
)

# Segment Creation Job
segment_creation_step = EmrJobTask(
    cluster_specs=segment_cluster.cluster_specs,
    name="McSegmentCreation",
    class_name="jobs.ctv.microcommunities.SegmentCreation",
    executable_path=jar,
    additional_args_option_pairs_list=segment_creation_spark_options_list,
    eldorado_config_option_pairs_list=[("date", orig_execution_date('%Y-%m-%d')),
                                       ("outputLocation", "s3://thetradedesk-useast-data-import/combined-data-import/collected/"),
                                       ("copyToDataImport", "true")]
)

# Segment Volume Control Job
volumeControl_step = EmrJobTask(
    cluster_specs=segment_cluster.cluster_specs,
    name="MC_volume_control",
    class_name="jobs.ctv.microcommunities.MCVolumeControl",
    executable_path=jar,
    additional_args_option_pairs_list=segment_creation_spark_options_list
)

# Reporting Job
reporting_step = EmrJobTask(
    cluster_specs=segment_cluster.cluster_specs,
    name="McOtpReporting",
    class_name="jobs.ctv.microcommunities.hawkverification.Reporting",
    executable_path=jar,
    additional_args_option_pairs_list=segment_creation_spark_options_list,
    eldorado_config_option_pairs_list=[("runTime", execution_date(1)), ("date", orig_execution_date('%Y-%m-%d'))]
)

dag = mc_dag.airflow_dag

bidrequest_agg_cluster.add_sequential_body_task(daily_agg_step)
bidrequest_agg_cluster.add_sequential_body_task(multi_day_rollup)

segment_cluster.add_sequential_body_task(percentile_step)
segment_cluster.add_sequential_body_task(segment_creation_step)
segment_cluster.add_sequential_body_task(volumeControl_step)
segment_cluster.add_sequential_body_task(reporting_step)

mc_dag >> bidrequest_sensor >> bidrequest_agg_cluster >> segment_cluster
