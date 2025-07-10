import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
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
from ttd.tasks.op import OpTask
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [
    ("executor-memory", "203G"),
    ("executor-cores", "32"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=128G"),
    ("conf", "spark.driver.cores=20"),
    ("conf", "spark.sql.shuffle.partitions=8192"),
    ("conf", "spark.default.parallelism=8192"),
    ("conf", "spark.driver.maxResultSize=80G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.memory.storageFraction=0.25"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=LEGACY"),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32",
    },
}]

run_date = "{{ data_interval_start.to_date_string() }}"
previous = "{{ (data_interval_start + macros.timedelta(days=-1)).strftime(\"%Y-%m-%d %H:%M:%S\") }}"
env = TtdEnvFactory.get_from_system().execution_env

experiment = ""
experiment_suffix = f"/experiment={experiment}" if experiment else ""

PROD_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
TEST_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
AUDIENCE_JAR = PROD_JAR if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else TEST_JAR

policy_table_read_env = "prod"
feature_store_read_env = "prod"
override_env = f"test{experiment_suffix}" if env == "prodTest" else env  # only apply experiment suffix in prodTest

relevance_online_data_generation_etl_dag = TtdDag(
    dag_id="perf-automation-relevance-online-data",
    start_date=datetime(2025, 5, 25, 3, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "RSM", "RSMV2"],
)

dag = relevance_online_data_generation_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
campaignSeed_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/campaignseed",
    version=1,
    env_aware=False,
    success_file=None
)

aggregatedseed_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="prod/audience/aggregatedSeed",
    version=1,
    env_aware=False,
)

bidsimpressions_data = HourGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/koav4/v=1/prod",
    data_name="bidsimpressions",
    date_format="year=%Y/month=%m/day=%d",
    hour_format="hourPart={hour}",
    env_aware=False,
    version=None,
)

policytable_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata",
    data_name="prod/audience/policyTable/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=False,
)

devicetype_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/devicetype",
    version=1,
    env_aware=False,
    success_file=None,
)

# split=9 should be the latest one to be success
# use split=1 to be consistent with the sampling logic on relevance data generation
TDIDDensityScore_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/feature_store",
    data_name=f"{feature_store_read_env}/profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob",
    version=1,
    date_format="date=%Y%m%d/split=1",
    env_aware=False,
)

DailySeedDensityScore_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/feature_store",
    data_name=f"{feature_store_read_env}/profiles/source=bidsimpression/index=SeedId/job=DailySeedDensityScore",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="data_available",
        datasets=[
            campaignSeed_dataset,
            aggregatedseed_dataset,
            bidsimpressions_data,
            policytable_dataset,
            devicetype_dataset,
        ],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 23,
    )
)

dataset_previous_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="previous_data_available",
        datasets=[
            TDIDDensityScore_dataset,
            DailySeedDensityScore_dataset,
        ],
        ds_date=previous,
        poke_interval=60 * 10,
        timeout=60 * 60 * 23,
    )
)
###############################################################################
# clusters
###############################################################################
relevance_online_data_generation_etl_task = EmrClusterTask(
    name="AudienceRelevanceOnlineDataCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32)],
        on_demand_weighted_capacity=7680,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
)

###############################################################################
# steps
###############################################################################
audience_relevance_online_data_generation_step = EmrJobTask(
    name="RelevanceOnlineBiddingDataGenerator",
    class_name="com.thetradedesk.audience.jobs.RelevanceOnlineBiddingDataGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[
        ("date", run_date),
        ("hashSampleObject", "TDID"),
        ("onlineBiddingDownSampleRate", "100000"),
        ("AggregatedSeedReadableDatasetReadEnv", policy_table_read_env),
        ("AudienceModelPolicyReadableDatasetReadEnv", policy_table_read_env),
        ("TDIDDensityScoreReadableDatasetReadEnv", feature_store_read_env),
        ("DailySeedDensityScoreReadableDatasetReadEnv", feature_store_read_env),
        ("ttdWriteEnv", override_env),
        ("subFolderKey", "split"),
        ("subFolderValue", "OOS"),
        ("Model", "RSMV2"),
        ("tag", "Seed_None"),
        ("version", "1"),
    ],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=4),
)

date_str = '{{ data_interval_start.strftime("%Y%m%d000000") }}'

write_oos_etl_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="write_oos_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_OOS_SUCCESS",
        date="",
        append_file=False,
        dag=dag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

relevance_online_data_generation_etl_task.add_parallel_body_task(audience_relevance_online_data_generation_step)

(
    relevance_online_data_generation_etl_dag >> dataset_sensor >> dataset_previous_sensor >> relevance_online_data_generation_etl_task >>
    write_oos_etl_success_file_task >> final_dag_status_step
)
