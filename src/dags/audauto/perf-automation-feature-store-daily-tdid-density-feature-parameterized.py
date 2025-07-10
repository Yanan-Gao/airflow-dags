import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import AUDAUTO
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
NUM_WORKERS = 128
DISK_SIZE_GB = 512

num_partitions = 16380
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

# Jar
PROD_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"
TEST_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"
FEATURE_STORE_JAR = PROD_JAR if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else TEST_JAR

environment = TtdEnvFactory.get_from_system()
env = environment.execution_env

# Route errors to test channel in test environment
if environment == TtdEnvFactory.prod:
    slack_channel = '#dev-perf-auto-alerts-rsm'
    slack_tags = AUDAUTO.team.sub_team
    enable_slack_alert = True
else:
    slack_channel = '#scrum-perf-automation-alerts-testing'
    slack_tags = None
    enable_slack_alert = True

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
offline_feature_store_dag = TtdDag(
    dag_id="perf-automation-feature-store-daily-tdid-density-feature-parameterized",
    start_date=datetime(2025, 7, 3),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    tags=["AUDAUTO", "FEATURE_STORE"]
)
dag = offline_feature_store_dag.airflow_dag

# S3 datasets
seed_density_score_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"features/feature_store/{env}/profiles/source=bidsimpression/index=SeedId",
    data_name="job=DailySeedDensityScore",
    date_format="date=%Y%m%d",
    version=1,
    env_aware=False,
)

tdid_site_zip_mapping: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"features/feature_store/{env}/profiles/source=bidsimpression/index=TDID",
    data_name="job=DailyTDIDFeaturePairMapping/config=SiteZip",
    date_format="date=%Y%m%d",
    version=1,
    env_aware=False,
)

tdid_aliased_city_mapping: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"features/feature_store/{env}/profiles/source=bidsimpression/index=TDID",
    data_name="job=DailyTDIDFeaturePairMapping/config=AliasedSupplyPublisherIdCity",
    date_format="date=%Y%m%d",
    version=1,
    env_aware=False,
)

policy_table: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata/prod",
    data_name="audience/policyTable/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=False,
)

# S3 sensors
upstream_data_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="UpstreamDatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 16,
    ds_date='{{data_interval_start.to_datetime_string()}}',
    datasets=[seed_density_score_dataset, tdid_site_zip_mapping, tdid_aliased_city_mapping],
)

policy_table_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="PolicyTable_DatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start).strftime("%Y-%m-%d %H:%M:%S") }}',
    datasets=[policy_table],
)

# cluster configs
master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_16xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

small_core_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(DISK_SIZE_GB).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=NUM_WORKERS,
)

large_core_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_16xlarge().with_ebs_size_gb(DISK_SIZE_GB).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=NUM_WORKERS,
)

# Data ETL steps
SYNTHETIC_ID_JOB_NAME = "DailyDensityScoreReIndexingJob"
TDID_JOB_NAME = "DailyTDIDDensityScoreSplitJobParameterized"
Graph_JOB_NAME = "DailyGroupIdDensityScoreSplitJobParameterized"
SPLIT_INDICES = {1: "0,3,6,9", 2: "1,4,7", 3: "2,5,8"}
CLUSTER_INDICES = list(range(1, len(SPLIT_INDICES) + 1))

synthetic_id_job_cluster = EmrClusterTask(
    name="ReIndexingJobCluster",
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    core_fleet_instance_type_configs=small_core_fleet_instance_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
)

synthetic_id_job_task = EmrJobTask(
    name=f"{SYNTHETIC_ID_JOB_NAME}",
    class_name=f"com.thetradedesk.featurestore.jobs.{SYNTHETIC_ID_JOB_NAME}",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO), ("numPartitions", 4096)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=16)
)
synthetic_id_job_cluster.add_parallel_body_task(synthetic_id_job_task)

tdid_split_clusters = []
for cluster_idx in CLUSTER_INDICES:
    feature_store_agg_job_cluster = EmrClusterTask(
        name=f"{TDID_JOB_NAME}_Cluster{cluster_idx}of{len(CLUSTER_INDICES)}",
        cluster_tags={
            "Team": AUDAUTO.team.jira_team,
        },
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        core_fleet_instance_type_configs=large_core_fleet_instance_configs,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
    )

    splits = SPLIT_INDICES[cluster_idx]
    tdid_density_score_task = EmrJobTask(
        name=f"Splits_{'_'.join(splits.split(','))}",
        class_name=f"com.thetradedesk.featurestore.jobs.{TDID_JOB_NAME}",
        additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
        eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO), ("numPartitions", 16380), ("splitIndex", splits)],
        executable_path=FEATURE_STORE_JAR,
        timeout_timedelta=timedelta(hours=16)
    )

    feature_store_agg_job_cluster.add_parallel_body_task(tdid_density_score_task)
    tdid_split_clusters.append(feature_store_agg_job_cluster)

graph_density_score_job_cluster = EmrClusterTask(
    name="GraphDensityScoreJobCluster",
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    core_fleet_instance_type_configs=small_core_fleet_instance_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
)

graph_density_score_job_task = EmrJobTask(
    name=f"{Graph_JOB_NAME}",
    class_name=f"com.thetradedesk.featurestore.jobs.{Graph_JOB_NAME}",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO), ("numPartitions", 8192)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=16)
)

graph_density_score_job_cluster.add_parallel_body_task(graph_density_score_job_task)

offline_feature_store_dag >> synthetic_id_job_cluster
policy_table_sensor >> upstream_data_sensor >> synthetic_id_job_cluster.first_airflow_op()

for idx, cluster in enumerate(tdid_split_clusters):
    offline_feature_store_dag >> synthetic_id_job_cluster >> cluster >> graph_density_score_job_cluster
    upstream_data_sensor >> cluster.first_airflow_op()
    cluster.last_airflow_op()
