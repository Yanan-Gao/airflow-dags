from datetime import datetime, timedelta

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.memory_optimized.r8gd import R8gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.openlineage import OpenlineageConfig
from ttd.slack.slack_groups import AIFUN
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask

from ttd.operators.dataset_check_sensor import DatasetCheckSensor

cleanroom_jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-cleanroom.jar"
availspipeline_jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline.jar"
aws_region = "us-east-1"
log_uri = "s3://thetradedesk-useast-avails/emr-logs"
small_cluster_fleet_capacity = 5400
core_fleet_capacity_adbrain = 10650
core_fleet_capacity = 12288  # More than the AdBrain graph needed, as we've more HouseholdIds in OpenGraph

standard_cluster_tags = {'Team': AIFUN.team.jira_team}

std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
        R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
        R8gd.r8gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
    ],
    on_demand_weighted_capacity=1,
)

# We also used to leverage R6gd.r6gd_16xlarge as well but it has only twice the
# disk of 4xl instead of the expected 4 times so we end up running out of disk.
# As you can see in the following table, we only scale disk linearly for 8xl.
# In fact, the 12/16 do have more disk, but they split into 2 disks and Spark
# only uses the primary - and we haven't taken time to figure out how to make
# it use the secondary one.
#
# Type | Disk
# -----|-----
#  2xl |  474
#  4xl |  950
#  8xl | 1900
# 12xl | 1425
# 16xl | 1900
std_core_instance_types = [
    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R8gd.r8gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R8gd.r8gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32)
]

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

clean_room_dag = TtdDag(
    dag_id="clean-room-avails-agg-us-east-1",
    start_date=datetime(2024, 7, 26, 8, 0),
    schedule_interval=timedelta(days=1),
    retries=1,
    retry_delay=timedelta(hours=3),
    slack_tags=AIFUN.team.jira_team,
    enable_slack_alert=True
)

# This cluster only needs to read and copy the day's worth of data = 136 TB
# into an S3 structure expected by Snowflake - there is no aggregation
# A smaller cluster will work - large clusters end up spamming S3 with too
# fast a write rate and run into upload errors
emr_cluster_spark_3_5_small = EmrClusterTask(
    name="CleanRoomAvailsAgg-Daily-Copy-Spark-3.5",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=small_cluster_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
    retries=0,
    retry_delay=timedelta(days=1)
)

emr_cluster_spark_3_5_aggs = EmrClusterTask(
    name="CleanRoomAvailsAgg-Aggs-Spark-3.5",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
    retries=0,
    retry_delay=timedelta(days=1)
)

emr_cluster_spark_3_5_aggs_compacted = EmrClusterTask(
    name="CleanRoomAvailsAgg-Aggs-Spark-3.5-Compacted",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
    retries=0,
    retry_delay=timedelta(days=1)
)

emr_cluster_spark_3_5_aggs_adbrain = EmrClusterTask(
    name="CleanRoomAvailsAgg-Aggs-Spark-3.5-AdBrain",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity_adbrain),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
    retries=0,
    retry_delay=timedelta(days=1)
)

emr_cluster_spark_3_5_aggs_adbrain_compacted = EmrClusterTask(
    name="CleanRoomAvailsAgg-Aggs-Spark-3.5-AdBrain-Compacted",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity_adbrain),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
    retries=0,
    retry_delay=timedelta(days=1)
)

# EMR cluster definition
# Exploded deal avails agg is initially written in parquet
# Then we copy it to Uniform. We had perf issues writing Uniform directly, so this
# is a workaround. Ideally we should have figured out the issues, but we're going
# to move to a different scheme entirely - using Unity Catalog, and using the open source
# Iceberg runtime anyway - vs our current approach of using the Delta.IO library.
# So not worth figuring out how to get rid of the workaround at this point.
emr_cluster_spark_3_5_agg5_uniform_copy = EmrClusterTask(
    name="CleanRoomAvailsAgg-Agg5-Uniform-Copy-Spark-3.5",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region
)


def create_step(uniform_enabled: bool, emr_cluster: EmrClusterTask, transformer_name: str, shuffle_partitions: int) -> EmrJobTask:
    if uniform_enabled:
        jar_path = cleanroom_jar_path
        class_name = 'com.thetradedesk.availspipeline.spark.cleanroom.jobs.CleanRoomTransform'
        enable_openlineage = False
    else:
        jar_path = availspipeline_jar_path
        class_name = 'com.thetradedesk.availspipeline.spark.jobs.TransformEntryPoint'
        enable_openlineage = True

    job_task = EmrJobTask(
        cluster_specs=emr_cluster.cluster_specs,
        name=transformer_name,
        class_name=class_name,
        executable_path=jar_path,
        additional_args_option_pairs_list=[
            ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
            ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            # Default is 1G, and the CleanRoomAvailsAgg-Daily-Copy job fails with:
            #   Total size of serialized results of 949374 tasks (1024.0 MiB) is bigger than
            #   spark.driver.maxResultSize (1024.0 MiB)
            ("conf", "spark.driver.maxResultSize=4G"),
            ("conf", f"spark.sql.shuffle.partitions={shuffle_partitions}"),
        ],
        eldorado_config_option_pairs_list=[('hourToTransform', '{{ logical_date.strftime(\"%Y-%m-%dT00:00:00\") }}'),
                                           ('transformer', transformer_name), ('availsRawS3Region', aws_region)],
        region_name=aws_region,
        openlineage_config=OpenlineageConfig(enabled=enable_openlineage)
    )
    return job_task


# Setup the tasks
avails_agg = create_step(
    uniform_enabled=False,
    emr_cluster=emr_cluster_spark_3_5_small,
    transformer_name='CleanRoomForecastingAvailsAggTransformer',
    shuffle_partitions=small_cluster_fleet_capacity * 2
)
exploded_deals_agg_adbrain = create_step(
    uniform_enabled=False,
    emr_cluster=emr_cluster_spark_3_5_aggs_adbrain,
    transformer_name='CleanRoomExplodedDealAvailsAggTransformer_AdBrain',
    shuffle_partitions=core_fleet_capacity_adbrain * 2
)
# This is the value that works in practice. core_fleet_capacity*2 ends up
# with the cluster failing
shuffle_partitions = int((core_fleet_capacity * 4) / 3)
exploded_deals_agg = create_step(
    uniform_enabled=False,
    emr_cluster=emr_cluster_spark_3_5_aggs,
    transformer_name='CleanRoomExplodedDealAvailsAggTransformer',
    shuffle_partitions=shuffle_partitions
)

compacted_agg_adbrain = create_step(
    uniform_enabled=False,
    emr_cluster=emr_cluster_spark_3_5_aggs_adbrain_compacted,
    transformer_name='CleanRoomCompactedAvailsAggTransformer_AdBrain',
    shuffle_partitions=core_fleet_capacity_adbrain
)
compacted_agg = create_step(
    uniform_enabled=False,
    emr_cluster=emr_cluster_spark_3_5_aggs_compacted,
    transformer_name='CleanRoomCompactedAvailsAggTransformer',
    shuffle_partitions=core_fleet_capacity
)
convert_avails_agg_adbrain = create_step(
    uniform_enabled=True,
    emr_cluster=emr_cluster_spark_3_5_agg5_uniform_copy,
    transformer_name='CleanRoomExplodedDealsAvailsFmtConversionTransformer_AdBrain',
    shuffle_partitions=core_fleet_capacity * 2
)
convert_avails_agg = create_step(
    uniform_enabled=True,
    emr_cluster=emr_cluster_spark_3_5_agg5_uniform_copy,
    transformer_name='CleanRoomExplodedDealsAvailsFmtConversionTransformer',
    shuffle_partitions=core_fleet_capacity * 2
)

emr_cluster_spark_3_5_small.add_sequential_body_task(avails_agg)

# Exploded & compacted aggs, using the legacy AdBrain Ids
# These will be removed once we've enough data using OpenGraph
emr_cluster_spark_3_5_aggs_adbrain.add_sequential_body_task(exploded_deals_agg_adbrain)

# Separating compacted into it's own cluster. This had a bunch of recent failures, and we
# want to be able to run it separately in order to recover. The exploded job succeeded, and
# does not need to be run again. These are expensive jobs, so being more granular in recovery
# is valuable
emr_cluster_spark_3_5_aggs_adbrain_compacted.add_sequential_body_task(compacted_agg_adbrain)

# Exploded & compacted aggs, using the newer OpenGraph Ids
# These need a larger sized cluster, as there is more OpenGraph data
emr_cluster_spark_3_5_aggs.add_sequential_body_task(exploded_deals_agg)
emr_cluster_spark_3_5_aggs_compacted.add_sequential_body_task(compacted_agg)

emr_cluster_spark_3_5_agg5_uniform_copy.add_sequential_body_task(convert_avails_agg_adbrain)
emr_cluster_spark_3_5_agg5_uniform_copy.add_sequential_body_task(convert_avails_agg)

# We could do things in parallel, but this job has extraordinarily high compute demands
# and we've been failing to acquire enough EC2 instances. By going sequentially, we hope
# to have better luck acquiring instances
avails_agg >> \
    emr_cluster_spark_3_5_aggs_adbrain >> \
    emr_cluster_spark_3_5_aggs_adbrain_compacted >> \
    emr_cluster_spark_3_5_aggs >> \
    emr_cluster_spark_3_5_aggs_compacted >> \
    emr_cluster_spark_3_5_agg5_uniform_copy

clean_room_dag >> emr_cluster_spark_3_5_small

# task to wait for input dataset - do this before spinning up EMR cluster
wait_for_input_data = DatasetCheckSensor(
    dag=clean_room_dag.airflow_dag,
    ds_date="{{ logical_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    datasets=[
        AvailsDatasources.clean_room_avails_agg_hourly_dataset.with_check_type("day"),
    ]
)

wait_for_input_data >> emr_cluster_spark_3_5_small.first_airflow_op()

dag = clean_room_dag.airflow_dag
