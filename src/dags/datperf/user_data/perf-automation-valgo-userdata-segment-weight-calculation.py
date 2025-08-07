from datetime import datetime
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from dags.datperf.datasets import geronimo_dataset, adgroup_dataset
from ttd.slack.slack_groups import DATPERF
from ttd.ttdenv import TtdEnvFactory

# Constants
exec_date_formated = '{{ data_interval_start.strftime("%Y-%m-%d") }}'
geronimo_jar = 's3://thetradedesk-mlplatform-us-east-1/libs/geronimo/jars/prod/geronimo.jar'

environment = TtdEnvFactory.get_from_system()

schedule_interval = "0 4 * * 1"
# DAG Definition
segment_selection_dag = TtdDag(
    dag_id="perf-automation-valgo-userdata-weight-calculation",
    start_date=datetime(2025, 2, 25),
    schedule_interval=schedule_interval,
    dag_tsg='',
    retries=1,
    max_active_runs=1,
    enable_slack_alert=False,
    tags=['DATPERF']
)
dag = segment_selection_dag.airflow_dag

# Sensors
# S3 sensors
trackingtag_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="trackingtag",
    version=1,
    env_aware=False,
    success_file=None,
)

kongming_conversion_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod/kongming",
    data_name="dailyconversion",
    version=1,
    env_aware=False,
    success_file=None,
)

roas_attribution_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod/roas",
    data_name="dailyattributionevents",
    version=1,
    env_aware=False,
    success_file=None,
)

kongming_attribution_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod/kongming",
    data_name="dailyattributionevents",
    version=1,
    env_aware=False,
    success_file=None,
)

upstream_etl_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="upstream_etl_available",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start - macros.timedelta(days=1)).strftime("%Y-%m-%d 23:00:00") }}',
    datasets=[
        geronimo_dataset, adgroup_dataset, kongming_conversion_dataset, trackingtag_dataset, roas_attribution_dataset,
        kongming_attribution_dataset
    ],
)

# Cluster Setup
instance_configs = {
    'master':
    EmrFleetInstanceTypes(instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1),
    'core':
    EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_16xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(4),
        ],
        on_demand_weighted_capacity=56
    )
}

etl_cluster = EmrClusterTask(
    name="valgo-userdata-segment-selection-cluster",
    cluster_tags={"Team": DATPERF.team.jira_team},
    master_fleet_instance_type_configs=instance_configs['master'],
    core_fleet_instance_type_configs=instance_configs['core'],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=-1,
    environment=environment
)

# configs for extreme data skew
spark_config = [("conf", "spark.executor.memory=48g"), ("conf", "spark.executor.cores=8"), ("conf", "spark.executor.instances=112"),
                ("conf", "spark.executor.memoryOverhead=8g"),
                ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"),
                ("conf", "spark.driver.memory=32g"), ("conf", "spark.driver.cores=8"), ("conf", "spark.driver.memoryOverhead=4g"),
                ("conf", "spark.sql.shuffle.partitions=2000"), ("conf", "spark.default.parallelism=2000"),
                ("conf", "spark.driver.maxResultSize=20G"), ("conf", "spark.dynamicAllocation.enabled=false"),
                ("conf", "spark.memory.fraction=0.8"), ("conf", "spark.memory.storageFraction=0.3"),
                ("conf", "spark.sql.adaptive.enabled=true"), ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
                ("conf", "spark.sql.adaptive.coalescePartitions.minPartitionSize=256MB"),
                ("conf", "spark.sql.adaptive.coalescePartitions.initialPartitionNum=2000"),
                ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
                ("conf", "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=1GB"),
                ("conf", "spark.sql.adaptive.skewJoin.skewedPartitionFactor=10"),
                ("conf", "spark.sql.adaptive.localShuffleReader.enabled=true"), ("conf", "spark.executor.memoryOffHeap.enabled=true"),
                ("conf", "spark.executor.memoryOffHeap.size=8g"), ("conf", "spark.network.timeout=800s"),
                ("conf", "spark.executor.heartbeatInterval=60s"), ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

job_config = [("date", exec_date_formated), ("targetingDataSource", "SIB"), ("candidateTargetingDataSource", "TTD-KID-KDT"),
              ("biddingLookbackDays", "3"), ("segmentSizeMin", "10"), ("totalSegmentsCap", "8000"), ("openlineage.enable", "false"),
              ("repartitionCount", "2000"), ("saltRange", "100")]

# Tasks
tasks = {
    'campaign-cpa':
    EmrJobTask(
        name="etl-segmentselection-campaign-cpa",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "CPA"), ("IdType", "CampaignId"),
                                                        ("conversionLookBackDays", "7")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'advertiser-cpa':
    EmrJobTask(
        name="etl-segmentselection-advertiser-cpa",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "CPA"), ("IdType", "AdvertiserId"),
                                                        ("conversionLookBackDays", "7")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'platform-cpa':
    EmrJobTask(
        name="etl-segmentselection-platform-cpa",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "CPA"), ("IdType", "PlatformWise"),
                                                        ("conversionLookBackDays", "1")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'advertiser-reach':
    EmrJobTask(
        name="etl-segmentselection-advertiser-reach",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "Reach"), ("IdType", "AdvertiserId"),
                                                        ("conversionLookBackDays", "7")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'platform-reach':
    EmrJobTask(
        name="etl-segmentselection-platform-reach",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "Reach"), ("IdType", "PlatformWise"),
                                                        ("conversionLookBackDays", "1")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'advertiser-roas':
    EmrJobTask(
        name="etl-segmentselection-advertiser-roas",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "ROAS"), ("IdType", "AdvertiserId"),
                                                        ("conversionLookBackDays", "30")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'platform-roas':
    EmrJobTask(
        name="etl-segmentselection-platform-roas",
        class_name="job.SegmentWeightCalculationProcessor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config + [("KPIOfConsideration", "ROAS"), ("IdType", "PlatformWise"),
                                                        ("conversionLookBackDays", "30")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'union-all-level-IV':
    EmrJobTask(
        name="etl-segmentselection-union-all-level-IV",
        class_name="job.SegmentWeightUnionResult",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config,
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'monitor':
    EmrJobTask(
        name="etl-metric-calculation",
        class_name="job.SegmentInDaMonitor",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[("date", exec_date_formated), ("universalImpSampleRate", "0.01"),
                                           ("grainBidCountNeeded", "5000"), ("openlineage.enable", "false")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    )
}

# Add tasks to cluster and set dependencies
for task_name, task in tasks.items():
    if task_name in ['advertiser-cpa', 'monitor']:
        etl_cluster.add_parallel_body_task(task)

# advertiser-cpa has to happen before all platform tasks
# tasks['campaign-cpa'] >> tasks['advertiser-cpa'] >> tasks['advertiser-reach'] >> tasks['platform-cpa'] >> tasks['platform-reach'] >> tasks[
#     'advertiser-roas'] >> tasks['platform-roas'] >> tasks['union-all-level-IV'] >> tasks['monitor']
tasks['advertiser-cpa'] >> tasks['monitor']

final_status_check = FinalDagStatusCheckOperator(dag=dag)

# DAG Dependencies
segment_selection_dag >> etl_cluster
upstream_etl_sensor >> etl_cluster.first_airflow_op()
etl_cluster.last_airflow_op() >> final_status_check
