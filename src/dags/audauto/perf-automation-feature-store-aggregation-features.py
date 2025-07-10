import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.dataset import default_date_part_format, default_hour_part_format
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
num_workers = 128
num_partitions = int(round(3.1 * num_workers)) * 10

# Base spark configuration
base_spark_options = [
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.8"),
    ("conf", "spark.memory.storageFraction=0.25"),
    ("conf", "spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED"),
]

# Spark options for ETL steps
etl_spark_options_list = base_spark_options + [
    ("executor-memory", "65G"),
    ("executor-cores", "8"),
    ("conf", "spark.driver.memory=65G"),
    ("conf", "spark.driver.cores=8"),
    ("conf", "spark.driver.maxResultSize=65G"),
    ("conf", "spark.driver.memoryOverhead=6656m"),
    ("conf", "spark.executor.memoryOverhead=6656m"),
    ("conf", "spark.sql.adaptive.enabled=true"),
    ("conf", "spark.default.parallelism=%s" % 14320),
]

# Spark options for aggregation steps
agg_spark_options_list = base_spark_options + [
    ("executor-memory", "100G"),
    ("executor-cores", "16"),
    ("conf", "spark.driver.memory=100G"),
    ("conf", "spark.driver.cores=32"),
    ("conf", "spark.driver.maxResultSize=100G"),
    ("conf", "spark.sql.shuffle.partitions=%s" % 16000),
    ("conf", "spark.default.parallelism=%s" % 8000),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-"?03 00:00)
DATE_MACRO = '{{ (data_interval_start).strftime("%Y-%m-%d") }}'
date_str = '{{ (data_interval_start).strftime("%Y%m%d") }}'

# Jar
FEATURE_STORE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"
# FEATURE_STORE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/mergerequests/svz-AUDAUTO-3442-enable-daily-agg-click-job/latest/feature_store.jar"

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = '#dev-perf-auto-alerts-rsm'
    slack_tags = AUDAUTO.team.sub_team
    enable_slack_alert = True
else:
    slack_channel = '#scrum-perf-automation-alerts-testing'
    slack_tags = None
    enable_slack_alert = True

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
offline_feature_store_dag = TtdDag(
    dag_id="perf-automation-feature-store-aggregation-features",
    start_date=datetime(2025, 5, 20),
    schedule_interval='0 10 * * *',  # run daily at 10:00 for upstream data ready
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=2,
    max_active_runs=1,
    depends_on_past=True,
    enable_slack_alert=enable_slack_alert,
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    retry_delay=timedelta(minutes=15),
    tags=["AUDAUTO", "FEATURE_STORE"]
)
dag = offline_feature_store_dag.airflow_dag

# S3 datasets
geronimo_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/koav4/v=1/prod",
    data_name="bidsimpressions",
    date_format="year=%Y/month=%m/day=%d",
    hour_format="hourPart={hour}",
    version=None,
    env_aware=False,
)

# attributedevent normally ready before UTC 10
attribute_event_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline/sources/firstpartydata_v2",
    data_name="attributedevent",
    date_format="date=%Y-%m-%d",
    version=None,
    env_aware=False,
)

# attributedeventresult normally ready before UTC 10
attribute_event_result_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline/sources/firstpartydata_v2",
    data_name="attributedeventresult",
    date_format="date=%Y-%m-%d",
    version=None,
    env_aware=False,
)
# s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=5/
bid_feedback_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-datapipe-data",
    path_prefix="parquet",
    data_name="rtb_bidfeedback_cleanfile",
    date_format=default_date_part_format,
    hour_format=default_hour_part_format,
    success_file="_COUNTS",
    version=5,
    env_aware=False,
)
# "s3://ttd-datapipe-data/parquet/rtb_clicktracker_cleanfile/v=5"
click_tracker_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-datapipe-data",
    path_prefix="parquet",
    data_name="rtb_clicktracker_cleanfile",
    date_format=default_date_part_format,
    hour_format=default_hour_part_format,
    success_file="_COUNTS",
    version=5,
    env_aware=False,
)

# S3 sensors
data_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="attribution_available",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    raise_exception=True,
    ds_date='{{data_interval_start.to_datetime_string()}}',
    datasets=[geronimo_dataset, attribute_event_dataset, attribute_event_result_dataset, bid_feedback_dataset, click_tracker_dataset],
)

# CLUSTER CONFIGS
master_fleet_instance_config = EmrFleetInstanceTypes(
    instance_types=[R5.r5_16xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_config = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_16xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=num_workers,
)

feature_store_etl_cluster = EmrClusterTask(
    name="GenerateData_OfflineFeatureStore",
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    master_fleet_instance_type_configs=master_fleet_instance_config,
    core_fleet_instance_type_configs=core_fleet_instance_config,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
)

# Data ETL steps
daily_gen_attribution = EmrJobTask(
    name="GenDailyAttribution",
    class_name="com.thetradedesk.featurestore.jobs.GenDailyAttribution",
    additional_args_option_pairs_list=copy.deepcopy(etl_spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=2)
)
feature_store_etl_cluster.add_parallel_body_task(daily_gen_attribution)

daily_gen_click_bidfeedback = EmrJobTask(
    name="GenDailyClickBidFeedback",
    class_name="com.thetradedesk.featurestore.jobs.GenDailyClickBidFeedback",
    additional_args_option_pairs_list=copy.deepcopy(etl_spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=2)
)
feature_store_etl_cluster.add_parallel_body_task(daily_gen_click_bidfeedback)

daily_gen_converted_impression = EmrJobTask(
    name="GenDailyConvertedImpressions",
    class_name="com.thetradedesk.featurestore.jobs.GenDailyConvertedImpressions",
    additional_args_option_pairs_list=copy.deepcopy(etl_spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [
        ("date", DATE_MACRO),
    ],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=6)
)
feature_store_etl_cluster.add_parallel_body_task(daily_gen_converted_impression)

# JOB DICT - <JOB_NAME> : <LIST_OF_AGG_LEVELS>
agg_job_dict = {
    "AggConvertedImpressions": ["CampaignId", "AdvertiserId", "TDID"],
    "AggAttributions": ["CampaignId", "AdvertiserId", "TrackingTagId", "TDID"],
    "AggClickBidFeedback": ["CampaignId", "AdvertiserId", "TDID"],
}

agg_clusters = []

for job_name, agg_levels in agg_job_dict.items():
    # put different agg level tasks of same dataset into one job
    agg_core_fleet_instance_configs = core_fleet_instance_config

    feature_store_agg_job_cluster = EmrClusterTask(
        name=f"Agg_{job_name}",
        cluster_tags={
            "Team": AUDAUTO.team.jira_team,
        },
        master_fleet_instance_type_configs=master_fleet_instance_config,
        core_fleet_instance_type_configs=agg_core_fleet_instance_configs,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
    )
    for agg_level in agg_levels:
        agg_task = EmrJobTask(
            name=f"{job_name}_{agg_level}",
            class_name=f"com.thetradedesk.featurestore.jobs.{job_name}",
            additional_args_option_pairs_list=copy.deepcopy(agg_spark_options_list),
            eldorado_config_option_pairs_list=java_settings_list + [
                ("date", DATE_MACRO),
                ("aggLevel", agg_level),
            ],
            executable_path=FEATURE_STORE_JAR,
            timeout_timedelta=timedelta(hours=6)
        )
        feature_store_agg_job_cluster.add_parallel_body_task(agg_task)

    agg_clusters.append(feature_store_agg_job_cluster)

final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

offline_feature_store_dag >> feature_store_etl_cluster
data_sensor >> feature_store_etl_cluster.first_airflow_op()
daily_gen_attribution >> daily_gen_converted_impression
daily_gen_click_bidfeedback >> daily_gen_converted_impression

for cluster in agg_clusters:
    daily_gen_converted_impression >> cluster
    cluster.last_airflow_op() >> final_dag_status_step
