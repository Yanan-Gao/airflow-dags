import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
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
num_workers = 32
parallelism = 64 * (num_workers - 1) * 2
num_partitions = parallelism * 2
spark_options_list = [
    ("executor-memory", "100G"),
    ("executor-cores", "16"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=110G"),
    ("conf", "spark.driver.cores=15"),
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % parallelism),
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
    dag_id="perf-automation-feature-store-daily-new-seed-density-feature",
    start_date=datetime(2025, 7, 6),
    schedule_interval=timedelta(hours=24),
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

policy_table_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata/prod",
    data_name="audience/policyTable/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=False,
)

# S3 sensors
geronimo_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="BidsImpressions_DatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{data_interval_start.to_datetime_string()}}',
    datasets=[geronimo_dataset],
)

policy_table_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="PolicyTable_DatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start + macros.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S") }}',
    datasets=[policy_table_dataset],
)

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=num_workers,
)

# Data ETL steps

feature_store_new_seed_non_sensitive_job_cluster = EmrClusterTask(
    name="NewSeedDensityNonSensitiveJobCluster",
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    core_fleet_instance_type_configs=core_fleet_instance_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
)

new_seed_nonsensitive_task = EmrJobTask(
    name="NewSeedDailyDensityFeatureNonsensitive",
    class_name="com.thetradedesk.featurestore.jobs.DailyNewSeedFeaturePairDensityScore",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=12)
)

feature_store_new_seed_non_sensitive_job_cluster.add_parallel_body_task(new_seed_nonsensitive_task)

feature_store_new_seed_sensitive_job_cluster = EmrClusterTask(
    name="NewSeedDensitySensitiveJobCluster",
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    core_fleet_instance_type_configs=core_fleet_instance_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
)

new_seed_sensitive_task = EmrJobTask(
    name="NewSeedDailyDensityFeatureSensitive",
    class_name="com.thetradedesk.featurestore.jobs.DailyNewSeedFeaturePairDensityScore",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO), ('seedGroupFeatureKeys', "AliasedSupplyPublisherId,City"),
                                                            ("IsSensitive", "true")],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=12)
)
feature_store_new_seed_sensitive_job_cluster.add_parallel_body_task(new_seed_sensitive_task)

for cluster in [feature_store_new_seed_non_sensitive_job_cluster, feature_store_new_seed_sensitive_job_cluster]:
    offline_feature_store_dag >> cluster
    geronimo_sensor >> policy_table_sensor >> cluster.first_airflow_op()
    cluster.last_airflow_op()
