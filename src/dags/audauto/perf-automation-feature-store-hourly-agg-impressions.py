import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import AUDAUTO
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
num_workers = 16
num_partitions = int(round(3.1 * num_workers)) * 10
spark_options_list = [
    ("executor-memory", "100G"),
    ("executor-cores", "16"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=110G"),
    ("conf", "spark.driver.cores=15"),
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % num_partitions),
    ("conf", "spark.driver.maxResultSize=50G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.memory.storageFraction=0.25"),
    ("conf", "spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED"),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

DATE_MACRO = '{{ (data_interval_start).strftime("%Y-%m-%d") }}'
date_str = '{{ (data_interval_start).strftime("%Y%m%d") }}'
hour_str = '{{ (data_interval_start).strftime("%H") }}'

# Jar
FEATURE_STORE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"

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
    dag_id="perf-automation-feature-store-hourly-agg-impressions",
    start_date=datetime(2025, 5, 17),
    schedule_interval=timedelta(hours=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    max_active_runs=7,
    retry_delay=timedelta(minutes=5),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
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
).with_check_type(check_type="hour")

# S3 sensors
geronimo_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="BidsImpressions_DatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{data_interval_start.to_datetime_string()}}',
    datasets=[geronimo_dataset],
)

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_16xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_16xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=num_workers,
)

# Data ETL steps

agg_job_dict = {
    "HourlyAggImpressions": ["TDID"],
}

agg_clusters = []

for job_name, profile_list in agg_job_dict.items():
    # put different agg level tasks of same dataset into one job
    agg_core_fleet_instance_configs = core_fleet_instance_configs

    feature_store_agg_job_cluster = EmrClusterTask(
        name=f"{job_name}",
        cluster_tags={
            "Team": AUDAUTO.team.jira_team,
        },
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        core_fleet_instance_type_configs=agg_core_fleet_instance_configs,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
    )
    for pl in profile_list:
        agg_task = EmrJobTask(
            name=f"{job_name}_{pl}",
            class_name=f"com.thetradedesk.featurestore.jobs.{job_name}",
            additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
            eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO), ("hour", hour_str), ("numPartitions", 256)],
            executable_path=FEATURE_STORE_JAR,
            timeout_timedelta=timedelta(hours=12)
        )
        feature_store_agg_job_cluster.add_parallel_body_task(agg_task)

    agg_clusters.append(feature_store_agg_job_cluster)

for cluster in agg_clusters:
    offline_feature_store_dag >> cluster
    geronimo_sensor >> cluster.first_airflow_op()
    cluster.last_airflow_op()
