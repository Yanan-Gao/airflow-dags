from datetime import datetime, timedelta

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.openlineage import OpenlineageConfig
from ttd.slack.slack_groups import AIFUN
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask

from ttd.operators.dataset_check_sensor import DatasetCheckSensor

availspipeline_jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline.jar"
aws_region = "us-east-1"
log_uri = "s3://thetradedesk-useast-avails/emr-logs"
core_fleet_capacity = 2000
standard_cluster_tags = {'Team': AIFUN.team.jira_team}
std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
)

# We also used to leverage R6gd.r6gd_16xlarge as well but it has only twice the
# disk of 4xl instead of the expected 4 times so we end up running out of disk.
# As you can see in the following table, we only scale disk linearly for 8xl.
# In fact, the 12/16 do have more disk, but they split into 2 disks and Spark
# only uses the primary - and we haven't taken time to figure out how to make
# it use the secondary one.
#
# Type | Disk | Memory
# -----|------|-------
#  2xl |  474 |
#  4xl |  950 | 128
#  8xl | 1900 | 256
# 12xl | 1425 |
# 16xl | 1900 |
#
# A parquet file on disk of size 230 MB loads into Spark memory of 1200 MB, i.e. 6x increase
# Since this is a groupby-sum we need to hold the entire original dataframe in memory
# Total input data size on disk = 10 TB, so 60 TB data needed to do the groupby-agg
# 1 machine has 128 GB
#
# 60 TB = 60,000 GB / 128 GB = 468 machines needed to do the job without spilling
# These machines have SSD though, and seem to do fine with spill and we are able to
# get away with just half the number of machines
#
# During testing, even 256 machines was overkill and resulted in 20% CPU usage.
# Dropping the machine count to 100 resulted in 80% CPU usage and job finished in roughly
# 1 hour
std_core_instance_types = [
    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
]

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

clean_room_dag = TtdDag(
    dag_id="clean-room-avails-hourly-agg-us-east-1",
    start_date=datetime(2025, 3, 1, 6, 0),
    # Upstream dataset availability is between 1.5 to 4.5 hours after the hour.
    # The mean is 3.8 hours. This DAG will start polling for the dataset 1 hour
    # after - since that is too early, we will bump up the dataset checker's timeout
    # to 7 hours. The downstream DAG is a daily, and runs at a 24 hour delay by default
    # anyway - which gives us enough buffer. In all cases the DAGs will fail if the
    # upstream data isn't available on time. On the snowflake side _MOST_ load jobs try
    # to check whether the tables have data in the prior 10 days, and fill try to fill
    # any missing days - so we are even more defensive when it comes to trying to load
    # any data that may have been missed previously.
    schedule_interval=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=15),
    slack_tags=AIFUN.team.jira_team,
    max_active_runs=6,  # Higher than typical, because we spend hours waiting for upstream dataset
    enable_slack_alert=True
)

# EMR cluster definition
emr_cluster_spark_3_5 = EmrClusterTask(
    name="CleanRoomAvailsAgg-us-east-1-Spark-3.5",
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags, "Process": "Clean-Room-Avails-Hourly-Agg-us-east-1"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
    retries=0,
    retry_delay=timedelta(days=1)
)

avails_agg = EmrJobTask(
    cluster_specs=emr_cluster_spark_3_5.cluster_specs,
    name='CleanRoomForecastingAvailsAggHourlyTransformer',
    class_name='com.thetradedesk.availspipeline.spark.jobs.TransformEntryPoint',
    executable_path=availspipeline_jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", f"spark.sql.shuffle.partitions={core_fleet_capacity}"),
    ],
    eldorado_config_option_pairs_list=[('hourToTransform', '{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                       ('transformer', 'CleanRoomForecastingAvailsAggHourlyTransformer')],
    region_name=aws_region,
    openlineage_config=OpenlineageConfig(enabled=False)
)

emr_cluster_spark_3_5.add_sequential_body_task(avails_agg)
clean_room_dag >> emr_cluster_spark_3_5

# task to wait for input dataset - do this before spinning up EMR cluster
wait_for_input_data = DatasetCheckSensor(
    dag=clean_room_dag.airflow_dag,
    ds_date="{{ logical_date.strftime(\"%Y-%m-%d %H:00:00\") }}",
    poke_interval=60 * 20,  # poke every 20 minutes - more friendly to the scheduler
    timeout=60 * 60 * 7,  # See note above in the DAG schedule_interval for why we bump this up
    datasets=[
        AvailsDatasources.identity_and_deal_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1"),
    ]
)
wait_for_input_data >> emr_cluster_spark_3_5.first_airflow_op()

dag = clean_room_dag.airflow_dag
