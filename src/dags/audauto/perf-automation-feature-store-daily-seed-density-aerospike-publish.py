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
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
NUM_FEATURE_GENERATION_WORKERS = 128
DISK_SIZE_GB = 512


def get_spark_options_list(num_workers):
    num_partitions = int(round(3.1 * num_workers)) * 40
    base_spark_options_list = [
        ("executor-memory", "100G"),
        ("executor-cores", "16"),
        ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
        ("conf", "spark.driver.memory=110G"),
        ("conf", "spark.driver.cores=15"),
        ("conf", "spark.driver.maxResultSize=50G"),
        ("conf", "spark.dynamicAllocation.enabled=true"),
        ("conf", "spark.memory.fraction=0.7"),
        ("conf", "spark.memory.storageFraction=0.25"),
        ("conf", "spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED"),
    ]

    return base_spark_options_list + [
        ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
        ("conf", "spark.default.parallelism=%s" % num_partitions),
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
FEATURE_STORE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"
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
    dag_id="perf-automation-feature-store-daily-seed-density-aerospike-publish",
    start_date=datetime(2025, 5, 28),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
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
    path_prefix="features/feature_store/prod/profiles/source=bidsimpression/index=FeatureKeyValue",
    data_name="job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScoreCategorized",
    version=None,
    date_format="date=%Y%m%d",
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

# s3 sensors
policy_table_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="PolicyTable_DatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start + macros.timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S") }}',
    datasets=[policy_table],
)

upstream_data_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="UpstreamDatasetsCheck",
    poke_interval=60 * 10,
    timeout=60 * 60 * 16,
    ds_date='{{data_interval_start.to_datetime_string()}}',
    datasets=[seed_density_score_dataset],
)

# cluster configs
master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_4xlarge().with_ebs_size_gb(DISK_SIZE_GB).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

feature_generation_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(DISK_SIZE_GB).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=NUM_FEATURE_GENERATION_WORKERS,
)

# Data ETL steps

# seed mapping id density score generation
FEATURE_GENERATION_CLUSTER_NAME = "DailySeedMappingIdDensityScoreFeatureGenerationCluster"

feature_generation_cluster = EmrClusterTask(
    name=f"{FEATURE_GENERATION_CLUSTER_NAME}",
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    core_fleet_instance_type_configs=feature_generation_fleet_instance_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
)

DAILY_SEED_MAPPING_ID_DENSITY_SCORE_JOB_NAME = "DailySeedMappingIdDensityScore"
mapping_id_density_score_task = EmrJobTask(
    name=f"{FEATURE_GENERATION_CLUSTER_NAME}_{DAILY_SEED_MAPPING_ID_DENSITY_SCORE_JOB_NAME}_Task",
    class_name=f"com.thetradedesk.featurestore.jobs.{DAILY_SEED_MAPPING_ID_DENSITY_SCORE_JOB_NAME}",
    additional_args_option_pairs_list=copy.deepcopy(get_spark_options_list(NUM_FEATURE_GENERATION_WORKERS)),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO), ("maxNumMappingIdsInAerospike", 3000),
                                                            ("campaignFlightStartingBufferInDays", 5)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=4)
)
feature_generation_cluster.add_sequential_body_task(mapping_id_density_score_task)

SEED_DENSITY_FEATURE_JOB_NAME = "SeedDensityFeatureJob"
seed_density_feature_task = EmrJobTask(
    name=f"{FEATURE_GENERATION_CLUSTER_NAME}_{SEED_DENSITY_FEATURE_JOB_NAME}_Task",
    class_name=f"com.thetradedesk.featurestore.jobs.{SEED_DENSITY_FEATURE_JOB_NAME}",
    additional_args_option_pairs_list=copy.deepcopy(get_spark_options_list(NUM_FEATURE_GENERATION_WORKERS)),
    eldorado_config_option_pairs_list=java_settings_list + [("date", DATE_MACRO)],
    executable_path=FEATURE_STORE_JAR,
    timeout_timedelta=timedelta(hours=4)
)
feature_generation_cluster.add_sequential_body_task(seed_density_feature_task)

update_seed_density_feature_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_seed_density_feature_meta_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"features/feature_store/{TtdEnvFactory.get_from_system().execution_env}/seed_density_feature_meta/v=1/_CURRENT",
        date="{{ data_interval_start.strftime(\"%Y%m%d00\") }}",
        append_file=True,
        dag=dag,
    )
)

# aerospike publish
AEROSPIKE_PUBLISH_JOB_NAME = "DailySeedMappingIdDensityScoreAerospikePublishingJob"

AEROSPIKE_CLUSTERS_PROD = {
    "wa2": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-wa2-main.aerospike.service.wa2.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 64
    },
    "vam": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-vam-main.aerospike.service.vam.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 16
    },
    "ca2": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-ca2-main.aerospike.service.ca2.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 32
    },
    "va6": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-va6-main.aerospike.service.va6.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 16
    },
    "ny1": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-ny1-main.aerospike.service.ny1.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 16
    },
    # None US DCs use larger clusters due to higher latency
    "jp1": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-jp1-main.aerospike.service.jp1.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 64
    },
    "sg2": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-sg2-main.aerospike.service.sg2.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 64
    },
    "de2": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-de2-main.aerospike.service.de2.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 64
    },
    "ie1": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-ie1-main.aerospike.service.ie1.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 64
    },
    "vad": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-vad-main.aerospike.service.vad.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 16
    },
    "vae": {
        "namespace": "ttd-user",
        "address": '{{macros.ttd_extras.resolve_consul_url("aerospike-vae-main.aerospike.service.vae.consul", port=3000, limit=1)}}',
        "workers": 10,
        "aerospikeMaxConcurrencyPerTask": 16
    }
}

feature_store_publish_job_clusters = []

for dc, aerospike_config in AEROSPIKE_CLUSTERS_PROD.items():
    feature_store_publish_job_cluster = EmrClusterTask(
        name=f"{AEROSPIKE_PUBLISH_JOB_NAME}_AerospikeCluster_{dc}",
        cluster_tags={
            "Team": AUDAUTO.team.jira_team,
        },
        master_fleet_instance_type_configs=master_fleet_instance_configs,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R5.r5_8xlarge().with_ebs_size_gb(DISK_SIZE_GB).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            ],
            on_demand_weighted_capacity=aerospike_config["workers"],
        ),
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
    )

    feature_store_publish_task = EmrJobTask(
        name=f"{AEROSPIKE_PUBLISH_JOB_NAME}_AerospikeCluster_{dc}_Task",
        class_name=f"com.thetradedesk.featurestore.jobs.{AEROSPIKE_PUBLISH_JOB_NAME}",
        additional_args_option_pairs_list=copy.deepcopy(get_spark_options_list(aerospike_config["workers"])),
        eldorado_config_option_pairs_list=java_settings_list +
        [("date", DATE_MACRO), ("aerospikeAddress", aerospike_config["address"]), ("aerospikeNamespace", aerospike_config["namespace"]),
         ("aerospikeSet", "sds"), ("ttl", 86400 * 180),
         ("aerospikeMaxConcurrencyPerTask", aerospike_config["aerospikeMaxConcurrencyPerTask"]), ("aerospikeDc", dc)],
        executable_path=FEATURE_STORE_JAR,
        timeout_timedelta=timedelta(hours=4)
    )

    feature_store_publish_job_cluster.add_parallel_body_task(feature_store_publish_task)
    feature_store_publish_job_clusters.append(feature_store_publish_job_cluster)

offline_feature_store_dag >> feature_generation_cluster >> update_seed_density_feature_current_file_task
policy_table_sensor >> upstream_data_sensor >> feature_generation_cluster.first_airflow_op()
for _, cluster in enumerate(feature_store_publish_job_clusters):
    update_seed_density_feature_current_file_task >> cluster
    cluster.last_airflow_op()
