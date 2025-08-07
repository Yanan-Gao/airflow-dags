import logging
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator
from typing import List, Optional

from dags.forecast.aerospike_set_utils import create_activate_and_unlock_set_version_task, \
    create_get_inactive_set_version_and_lock_task
from dags.forecast.sketches.operators.hardcoded_successful_dow_operator import \
    FindMostRecentSuccessfulDowDatesInPipeline

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import FORECAST

jar_path = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
dag_name = 'user-sampled-avails.RAM'
ram_timestamp_key = 'ram_timestamp'
calculate_ram_timestamp_task_id = 'calc_ram_timestamp'
last_good_iso_weekdays_key = 'last_good_iso_weekdays'
ram_generation_iso_weekday_key = 'ram_generation_iso_weekday'

aerospike_hosts = '{{macros.ttd_extras.resolve_consul_url("aerospike-use-ramv.aerospike.service.useast.consul", port=3000, limit=1)}}'
aerospike_namespace = 'ttd-ramv'
aerospike_set = 'rv4'
aerospike_metadata_set_name = 'metadata'
aerospike_use_buffered_writes = 'true'
# Below are experimentally chosen parameters for optimal BufferedWrites operation

# server_wps_quota = 7_920_000
# concurrent_tasks (number_of_cores) = 715
# transaction_rate = server_wps_quota / concurrent_tasks - tolerance
aerospike_transaction_rate = '11000'
aerospike_max_async_connections_per_node = '100'

lock_inactive_aerospike_set_task_id = 'lock_inactive_aerospike_set'
update_active_aerospike_set_task_id = 'update_active_aerospike_set'

# The XCOM key used to pass the inactive aerospike set between Airflow dag steps
aerospike_xcom_inactive_set_key = 'inactive_aerospike_set_name'
# The XCOM key used to pass the inactive set number
aerospike_xcom_inactive_set_number_key = 'inactive_aerospike_set_number'
# Aerospike records have "gen" a.k.a like a version of the record. This variable defines the XCOM key where we store
# the gen value of received/written records
aerospike_xcom_gen_key = 'aerospike_gen'

cluster_additional_application_configurations: list[dict[str, dict[str, str] | str]] = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.resourcemanager.am.max-attempts": "1"
    },
}]


def get_ram_timestamp_template():
    global calculate_ram_timestamp_task_id
    return f'{{{{ ' \
           f'task_instance.xcom_pull(dag_id="{dag_name}", ' \
           f'task_ids="{calculate_ram_timestamp_task_id}", ' \
           f'key="{ram_timestamp_key}") ' \
           f'}}}}'


def get_ram_partition_weekdays_and_days():
    global last_good_iso_weekdays_key
    global dag_name

    return f'{{{{ task_instance.xcom_pull(dag_id="{dag_name}", key="{last_good_iso_weekdays_key}") }}}}'


def get_ram_generation_iso_weekday():
    global last_good_iso_weekdays_key
    global dag_name

    return f'{{{{ task_instance.xcom_pull(dag_id="{dag_name}", key="{ram_generation_iso_weekday_key}") }}}}'


# DAG: this uppercase string must appear in file for airflow to scan it
# constants used in steps
hmh_p = 12
hmh_r = 52
scala_ns = 'com.thetradedesk.etlforecastjobs.universalforecasting.ram.usersampled'

standard_cluster_tags = {"Env": "prod", "Application": "RAM", "Team": FORECAST.team.jira_team}

spark_options_list_type1 = [("executor-memory", "30G"), ("executor-cores", "5"),
                            ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=30G"),
                            ("conf", "spark.driver.cores=5"), ("conf", "spark.driver.maxResultSize=6G"),
                            ("conf", "spark.network.timeout=1200"), ("conf", "spark.shuffle.registration.timeout=90000"),
                            ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                            ("conf", "spark.sql.shuffle.partitions=9000")]

# Copied the spark_options_list_type1 and changed according to
# prodTest configuration:
# https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusterDetails/j-3R1US91PF7AT9
spark_options_list_type2 = [("executor-memory", "57500M"), ("executor-cores", "8"), ("conf", "spark.task.cpus=8"),
                            ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=57500M"),
                            ("conf", "spark.driver.cores=8"), ("conf", "spark.driver.maxResultSize=6G"),
                            ("conf", "spark.network.timeout=1200"), ("conf", "spark.shuffle.registration.timeout=90000"),
                            ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                            ("conf", "spark.sql.shuffle.partitions=3000")]

# Taken from this prod test https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusterDetails/j-11FAUI9UQFR2F
spark_options_list_containment_records = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                          ("conf", "spark.sql.autoBroadcastJoinThreshold=-1"), ("conf", "spark.driver.memory=38445m"),
                                          ("conf", "spark.driver.cores=5"), ("conf", "spark.driver.memoryOverhead=4271m"),
                                          ("conf", "spark.network.timeout=1200"), ("conf", "spark.shuffle.registration.timeout=90000"),
                                          ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                          ("conf", "spark.sql.shuffle.partitions=145920")]

dag_idx = 1


def create_dag(name: Optional[str] = None) -> TtdDag:
    global dag_idx
    dag_id = f'{dag_idx:02d}.{name}' if name else dag_name
    ttd_dag = TtdDag(
        dag_id=dag_id,
        # Should start running at wall clock date/time 4-1 using the 3-31 logical date
        start_date=datetime(2024, 9, 11, 0, 0),

        # Cron expression, everyday at 6am, see: https://crontab.guru/#0_6_*_*_*
        # We take the current scheduled date, and calculate ramTimestamp to be 1 day
        # before to ensure the input data SIBv2 and Avails7Day is fully logged. A 1 day
        # lookback is fine, but there is a corner case, if the DAG happens to run shortly
        # after midnight, say Oct 2nd at 00:01:00, then the ramTimestamp will be Oct 1st
        # and at 1 minute past midnight on Oct 2nd we will probably not have ALL the data
        # for Oct 1st available yet. So we actually run at 6am UTC every day, so in this
        # example we would run no earlier than Oct 2nd 6am and ramTimestamp is still Oct 1.
        # By this time Oct 1st data should be complete.
        schedule_interval=None if name else '0 8 * * *',
        slack_channel=FORECAST.team.alarm_channel,
        slack_tags=FORECAST.data_charter().sub_team,
        tags=[FORECAST.team.jira_team],
        dag_tsg="https://atlassian.thetradedesk.com/confluence/x/AnydBw",
        max_active_runs=7
    )

    # Airflow needs the DAG to be in a global variable for the DAG to be visible
    # We've a DAG for each step to support testing (in "test mode") so to avoid having
    # some ~10 global DAG declarations, we create a global variable on the fly for each
    # test-dag
    globals()[f'dag{dag_idx}'] = ttd_dag.airflow_dag
    dag_idx += 1
    return ttd_dag


master_fleet_instance_type_configs_type = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(64)],
    on_demand_weighted_capacity=1,
)


class ClusterAndTask:

    def __init__(self, cluster: EmrClusterTask, task: EmrJobTask):
        self.cluster = cluster
        self.task = task


def create_cluster(
    cluster_name: str,
    instance_types: List[EmrInstanceType],
    on_demand_weighted_capacity: int,
    maximize_resource_allocation: bool = False
) -> EmrClusterTask:
    return EmrClusterTask(
        name=cluster_name,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs_type,
        core_fleet_instance_type_configs=
        EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity),
        cluster_tags={
            **standard_cluster_tags, "Process": cluster_name
        },
        enable_prometheus_monitoring=True,
        # Setting this to true means, after the last step has finished, the cluster will
        # terminate immediately. This is not good because our operators:
        # (a) Create the cluster, and then
        # (b) Add the step
        # There is some latency between (a) and (b) and in this time, "all steps have finished"
        # and EMR may terminate the cluster BEFORE (b) runs.
        #
        # Setting this to false also does NOT imply that we are relying on the kill-task from
        # airflow. It does set the option on the cluster to "Terminate if idle" (20 minutes is

        # the duration. So either this cluster will be killed quickly by the kill task, or if
        # airflow somehow fails to - we still won't have a runaway because the "kill if idle" will
        # get it
        cluster_auto_terminates=False,
        maximize_resource_allocation=maximize_resource_allocation,
        additional_application_configurations=cluster_additional_application_configurations,
        retries=0,
    )


def create_map_avails_step(ram_timestamp: str) -> ClusterAndTask:
    map_avail_cluster = create_cluster(
        'SetupUserSampledAvailsTable',
        instance_types=[
            R6g.r6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=5120,
        maximize_resource_allocation=True
    )

    setup_and_map_avail_step = EmrJobTask(
        name='SetupUserSampledAvailsTable',
        class_name=f'{scala_ns}.SetupUserSampledAvailsTable',
        additional_args_option_pairs_list=[("conf", "spark.driver.memory=50G"), ("conf", "spark.driver.maxResultSize=15G")],
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('availsSampledRate', '1.0'), ('hmhP', str(hmh_p)),
                                           ('hmhR', str(hmh_r))],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=5)
    )  # Usually takes about 4 hours.

    map_avail_cluster.add_parallel_body_task(setup_and_map_avail_step)
    return ClusterAndTask(map_avail_cluster, setup_and_map_avail_step)


def create_pre_process_pre_bid_step(ram_timestamp: str) -> ClusterAndTask:
    name = 'PreprocessPreBid'
    preprocess_pre_bid_cluster = create_cluster(
        f'{name}Cluster',
        instance_types=[
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=2880
    )

    preprocess_pre_bid_step = EmrJobTask(
        name=name,
        class_name=f'{scala_ns}.{name}',
        additional_args_option_pairs_list=spark_options_list_type1 + [("conf", "spark.driver.memory=50G"),
                                                                      ("conf", "spark.driver.maxResultSize=15G")],
        eldorado_config_option_pairs_list=[
            ('ramGenerationTimestamp', ram_timestamp),
            ('availsSampledRate', '1.0'),
        ],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=1, minutes=15)
    )  # Usually takes about 40 minutes.

    preprocess_pre_bid_cluster.add_parallel_body_task(preprocess_pre_bid_step)
    return ClusterAndTask(preprocess_pre_bid_cluster, preprocess_pre_bid_step)


def create_prebid_categories_hmh_agg_step(ram_timestamp: str) -> ClusterAndTask:
    # This task loads the HmhByRCUKHWithPreBidCategoriesDataSet, explodes it by the
    # SerializedIds array column and then regroups it on SerializedId
    # The input data on S3 is 1700 Gb (1.7Tb) in size

    #
    # One complication with sizing though, is that we explode the SerializedIds
    # column, which should greatly multiply the number of rows and increase the memory
    # requirement, but we don't have good visitibility into how much that is, as the
    # explode results in an RDD, and the size of an RDD in the "Storage" tab of the
    # Spark WebUI isn't available.
    #
    # So we go with a bit more than 3000 GB memory, and tune from there
    name = 'PreBidCategoriesHmhAggregate'
    cluster = create_cluster(
        f'{name}Cluster',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
        ],
        on_demand_weighted_capacity=4320
    )

    step = EmrJobTask(
        name=f"{name}",
        class_name=f"{scala_ns}.{name}",
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r))],
        additional_args_option_pairs_list=spark_options_list_type2,
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=3)
    )  # Usually takes about 2 hours.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_targeting_data_step(ram_timestamp: str) -> ClusterAndTask:
    targeting_data_cluster = create_cluster(
        'SetupTargetingData',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
        ],
        on_demand_weighted_capacity=4320
    )

    setup_targeting_data_step = EmrJobTask(
        name='SetupTargetingData',
        class_name=f"{scala_ns}.SetupTargetingDataTable",
        additional_args_option_pairs_list=spark_options_list_type1,
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r))],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=1, minutes=45)
    )  # Usually takes about an hour.

    targeting_data_cluster.add_parallel_body_task(setup_targeting_data_step)
    return ClusterAndTask(targeting_data_cluster, setup_targeting_data_step)


def create_xd_targeting_data_step(ram_timestamp: str) -> ClusterAndTask:
    xd_targeting_data_cluster = create_cluster(
        'SetupXDTargetingData',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
        ],
        on_demand_weighted_capacity=4320
    )

    setup_xd_targeting_data_step = EmrJobTask(
        name="SetupXDTargetingData",
        class_name=f"{scala_ns}.SetupXDeviceTargetingDataTable",
        additional_args_option_pairs_list=spark_options_list_type1,
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r))],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=1, minutes=15)
    )  # Usually takes about 45 minutes.
    xd_targeting_data_cluster.add_parallel_body_task(setup_xd_targeting_data_step)
    return ClusterAndTask(xd_targeting_data_cluster, setup_xd_targeting_data_step)


def create_targeting_data_aggregate_step(slice_idx: int, num_slices: int, ram_timestamp: str) -> ClusterAndTask:
    # The sizing of this cluster has been based on this prod test:
    #   https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusterDetails/j-33P3879NUVTHU
    # And this Slack thread: https://thetradedesk.slack.com/archives/C05MA5A6Z6U/p1749029425889749
    #
    # Things to keep in mind:
    #
    # 1. This is based on having 3 slices, which creates 3 partitions (mutually exclusive)
    #    of targeting data IDs, and process them in parallel
    # 2. At the moment of sizing, there were around 6 million of different TargetingDataIds
    # 3. The partitions are created by taking modulo num_slices. I tested that this method ends up
    #    creating 3 more or less uniform partitions of around 2 million unique targeting data ids.
    # 4. Each TargetingDataId requires 32Kb at least. This means that each slice cluster will
    #    need at least 2_000_000*32Kb =64Gb.
    # 5. The memory overhead by default is 0.1845*executorMemory.
    #    We are allocating 100Gb per executor, this ends up being 18.45Gb of overhead.
    # 6. Total of 118.45GB memory per executor will be needed. The R5g machine-types make sense for this workload
    # 7. We allocate 15 cores per executor and per task. As we are doing .mapPartitions to aggregate within partitions,
    #    and within that, we create a Map[TargetingDataId, HMH], that needs at least 64GB of memory,
    #    each task needs half of the memory of a VM. So, each VM has 16, or 32 cores. If we get the 32 cores ones,
    #    we will allocate 2 executors per VM.
    #    As a side note, we use the cores/thread to iterate a list in parallel and update the Map.
    # 8. The coalescePartitions controls how big the input for each task on the .mapPartitions part will be.
    #    Smaller -> more compression within partitions -> less shuffle across partitions.
    #    I just keep it as is, but I think it can be smaller. At the moment, these partitions are [3, 4) Gb.
    #
    # If at some point we need to improve performance further, we could test/fix the "mergeInPlace" approach
    # discussed in the slack thread. This theoretically should improve the performance due to less
    # HMHs serialization/deserialization.
    spark_options_list_type3 = [("executor-memory", "100000M"), ("executor-cores", "15"),
                                ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                ("conf", "spark.driver.memory=115000M"), ("conf", "spark.driver.cores=15"), ("conf", "spark.task.cpus=15"),
                                ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                ("conf", "spark.shuffle.registration.timeout=90000"), ('conf', 'spark.sql.shuffle.partitions=6000')]
    cluster = create_cluster(
        f'TargetingDataAggCluster-{slice_idx + 1}',
        instance_types=[
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48).with_ebs_size_gb(1536)
        ],
        on_demand_weighted_capacity=5616
    )

    step = EmrJobTask(
        name=f"TargetingDataAgg-part-{slice_idx + 1}-of-{num_slices}",
        class_name=f"{scala_ns}.TargetingDataAggregate",
        additional_args_option_pairs_list=spark_options_list_type3,
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('dataElementsHmhCoalescePartitions', '2000'),
                                           ('sliceIdx', slice_idx), ('numSlices', num_slices)],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=4)
    )  # Usually takes almost 2 hours.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_targeting_data_aggregate_merge_step(ram_timestamp: str) -> ClusterAndTask:
    targeting_data_aggregate_merge_cluster = create_cluster(
        'TargetingDataAggregateMerge',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2).with_ebs_size_gb(1024),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4).with_ebs_size_gb(1536)
        ],
        on_demand_weighted_capacity=10
    )

    targeting_data_aggregate_merge_step = EmrJobTask(
        name='TargetingDataAggregateMerge',
        class_name=f'{scala_ns}.TargetingDataAggregateMerge',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('isInChain', 'true')],
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=30)
    )  # Usually takes almost 15 minutes.

    targeting_data_aggregate_merge_cluster.add_parallel_body_task(targeting_data_aggregate_merge_step)
    return ClusterAndTask(targeting_data_aggregate_merge_cluster, targeting_data_aggregate_merge_step)


def filter_sib_for_containment_records(ram_timestamp: str):
    name = 'FilterSibForContainmentRecords'
    cluster = create_cluster(
        # Cluster usually uses around 5TB of memory
        name,
        instance_types=[
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(768),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(512),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(384)
        ],
        on_demand_weighted_capacity=12288,
        maximize_resource_allocation=True,
    )

    filter_sib_spark_config = [("conf", "spark.sql.shuffle.partitions=4500")]

    step = EmrJobTask(
        name=name,
        class_name=f'{scala_ns}.{name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp)],
        additional_args_option_pairs_list=filter_sib_spark_config,
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=2)
    )  # Usually takes around 30 minutes

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def filter_xd_for_containment_records(ram_timestamp: str):
    name = 'FilterXDeviceForContainmentRecords'
    cluster = create_cluster(
        name,
        # Cluster usually takes sub 20tb of RAM
        instance_types=[
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(768),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(512),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(384)
        ],
        on_demand_weighted_capacity=19968
    )

    step = EmrJobTask(
        name=name,
        class_name=f'{scala_ns}.{name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp)],
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=30)
    )  # Usually takes almost 20 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_targeting_containment_records(ram_timestamp: str):
    name = 'CreateTargetingContainmentRecords'
    cluster = create_cluster(
        name,
        # This job is CPU bound since the main time is spent in creating bloom filters, hence we can use the
        # m-type instances
        instance_types=[
            M6g.m6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            M6g.m6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=9484,
        maximize_resource_allocation=True
    )

    step = EmrJobTask(
        name=name,
        class_name=f'{scala_ns}.{name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp)],
        additional_args_option_pairs_list=spark_options_list_containment_records,
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=8)
    )  # Usually takes about 4 hours.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_pre_bid_containment_records(ram_timestamp: str):
    name = 'ContainmentRecordsPreBid'
    cluster = create_cluster(
        name,
        # This setup is not tuned. It's just the same as the Targeting data containment records.
        instance_types=[
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(768),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(512),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(384)
        ],
        on_demand_weighted_capacity=18432
    )

    step = EmrJobTask(
        name=name,
        class_name=f'{scala_ns}.{name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp)],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=1, minutes=30)
    )  # Usually takes about 50 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_user_sampled_avails_vector_hmh_step(ram_timestamp: str):
    name = 'UserSampledAvailsVectorsHMH'
    cluster = create_cluster(
        f'{name}Cluster',
        # We have 4.8 million vector-list-values and a single HMH buffer (which we have per value) is 32kb
        # For an "in-memory" dictionary merge - we need 4.8 million * 32kb = 128 GB memory at least
        # So a single node needs to be larger than that by a decent margin
        # We go for r5d.8xl nodes at a minimum - which have 256 GB memory
        instance_types=[
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
            R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64).with_ebs_size_gb(2048)
        ],
        on_demand_weighted_capacity=8640
    )

    step = EmrJobTask(
        name=f"{name}",
        class_name=f"{scala_ns}.{name}",
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('coalescePartitions', '0'), ('hmhP', str(hmh_p)),
                                           ('hmhR', str(hmh_r)), ('version', '3')],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=8)
    )  # Usually takes about 4h 30mn

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_partial_agg_hmh_on_rcukh_step(ram_timestamp: str):
    name = 'AggAvailsByReferrerCacheUrlKeyHash'
    cluster = create_cluster(
        f'{name}Cluster',
        # We need ~370 GB of memory to process all the (RCUKH, hash(availableBidRequestId)) tuples
        # This step does a groupBy/agg(LazyHmh) which should strictly reduce the size of the input
        # dataset, so we add some buffer and use 420GB of cluster memory - which is ~30 r6gd.4xl
        # machines, running $1/hr each = $30/hr total
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=2160,
        maximize_resource_allocation=True
    )

    step = EmrJobTask(
        name=f"{name}",
        class_name=f"{scala_ns}.{name}",
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r)),
                                           ('version', '3')],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=1),
    )  # Usually takes about 18-30 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_join_agg_hmh_on_rcukh_and_preprocessed_pre_bid_step(ram_timestamp: str):
    name = 'JoinAvailsAndPreBid'
    cluster = create_cluster(
        f'{name}Cluster',
        # Tested empirically. Started from 6000 cores this ended up finishing in 3mins. Reduced this to 1500 and finished
        # in less than 10mins
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=1500
    )

    step = EmrJobTask(
        name=f"{name}",
        class_name=f"{scala_ns}.{name}",
        eldorado_config_option_pairs_list=[
            ('ramGenerationTimestamp', ram_timestamp),
        ],
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=45)
    )  # Usually takes about 20 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_containment_meta_records_step(ram_timestamp: str):
    class_name = 'ContainmentMetaRecords'
    cluster = create_cluster(
        f'{class_name}Cluster',
        # This step is very similar to tdids_for_targeting_containment_records
        # The inputs are exactly the same:
        # - UserSampledAvailsDailyHmh and
        # - RamDailyVectors (targeting & non-targeting)
        # These are joined on avails-hash. The main difference is, post-join
        # we select not just the TDID, but also the availableBidRequest and
        # packedRegister (avails-hash) of each individual avail, so the join
        # result is larger
        #
        # tdids_for_targeting_containment_records requires 24TB for the join,
        # and we up that to 36TB, and this should require somewhat more than that
        # so we will go with 36TB, or ~88 r5d.12xl instances at $300/hr for the
        # cluster
        instance_types=[
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(768),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(512),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(384)
        ],
        on_demand_weighted_capacity=46800
    )

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r))],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=2, minutes=30)
    )  # Usually takes about 1 hour and 30 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_containment_records_vector_values_step(ram_timestamp: str):
    # Joining per-avail data (we don't need HMH, we use the available-bid-request-id & vectors)
    # this has a memory footprint of 20 Tb
    #
    # ...with the containment-meta-records data this contains available-bid-request-ids for which
    # containment records are needed. This has a footprint of 500 GB
    #
    # So we roughly need 20 Tb for the join
    #
    # this translates into 20e12/128e9 = 156 r5.4xl machines (each has 128 Gb RAM)
    # We'll "round up" to 200 machines
    #
    class_name = 'ContainmentRecordsVectorValues'
    cluster = create_cluster(
        f'{class_name}Cluster',
        instance_types=[
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(512),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(384),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(256),
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(128),
        ],
        on_demand_weighted_capacity=38400
    )

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r))],
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=45)
    )  # Usually takes about 20 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_weekly_pre_bid_weekly_cluster(
    ram_timestamp: str, partition_weekdays_and_dates: str, ram_generation_iso_weekday: str
) -> ClusterAndTask:
    class_name = 'WeeklyContainmentPreBid'
    # This cluster is different from the other because pre-bid containment records daily dataset are about
    # 7TB each and there should be 7 of them loaded.
    #
    # The following cluster configuration follows this experiment:
    # https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusterDetails/j-365WP16RJIZ7K
    #
    # This is a memory-bounded job, so the weighted capacity corresponds to memory available per instance type.
    # Considering that the containment pre-bid records are about 7TB and there are some that are less than that,
    # It's needed around 50TB.
    cluster = create_cluster(
        'UserSampledPreBidWeeklyProcessingCluster',
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(128),
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(256),
            R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(384),
            R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(512)
        ],
        on_demand_weighted_capacity=50_000
    )

    weekly_pre_bid_eldorado_options = [
        ('ramGenerationTimestamp', ram_timestamp),
        ('hmhP', str(hmh_p)),
        ('hmhR', str(hmh_r)),
        ('partitionWeekdaysAndDates', partition_weekdays_and_dates),
        ('ramGenerationIsoWeekday', ram_generation_iso_weekday),
    ]

    weekly_pre_bid_spark_options = [("executor-memory", "36809m"), ("executor-cores", "5"),
                                    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                    ("conf", "spark.driver.memory=36809m"), ("conf", "spark.driver.cores=5"),
                                    ("conf", "spark.driver.maxResultSize=4G"), ("conf", "spark.sql.shuffle.partitions=18000"),
                                    ("conf", "spark.driver.memoryOverhead=4089m")]

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=weekly_pre_bid_eldorado_options,
        additional_args_option_pairs_list=weekly_pre_bid_spark_options,
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=1)
    )  # Usually takes about 30 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


# This function creates Cluster with multiple steps, not unlike the other clusters in this file. The task returned
# in the result is the last task in the sequence of tasks. This is done because steps are quite quick and recreating
# cluster for each one of them adds a lot of overhead
def create_weekly_processing_cluster(
    ram_timestamp: str, partition_weekdays_and_dates: str, ram_generation_iso_weekday: str
) -> ClusterAndTask:
    cluster = create_cluster(
        'UserSampledWeeklyProcessingCluster',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48).with_ebs_size_gb(1536)
        ],
        on_demand_weighted_capacity=1600,  # Same as forecasting-ram-pipeline
        maximize_resource_allocation=True,
    )

    weeklyStepsOptions = [('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r)),
                          ('partitionWeekdaysAndDates', partition_weekdays_and_dates),
                          ('ramGenerationIsoWeekday', ram_generation_iso_weekday)]
    weeklyStepsSparkOptions = [("conf", "spark.sql.shuffle.partitions=18000")]

    gen_task = lambda c: EmrJobTask(
        name=c,
        class_name=f'{scala_ns}.{c}',
        eldorado_config_option_pairs_list=weeklyStepsOptions,
        executable_path=jar_path,
        additional_args_option_pairs_list=weeklyStepsSparkOptions
    )

    last = None
    for i in ['WeeklyUserSampledRamVectors', 'WeeklyContainmentMetaRecords']:
        task = gen_task(i)
        if last is not None:
            last >> task
        last = task
        cluster.add_parallel_body_task(task)

    return ClusterAndTask(cluster, last)


def create_weekly_containment_vector_values_cluster(
    ram_timestamp: str, partition_weekdays_and_dates: str, ram_generation_iso_weekday: str
) -> ClusterAndTask:
    cluster = create_cluster(
        'UserSampledWeeklyVectorValuesContainmentRecordsCluster',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48).with_ebs_size_gb(1536)
        ],
        on_demand_weighted_capacity=1600,  # Same as forecasting-ram-pipeline
        maximize_resource_allocation=True,
    )

    weeklyStepsOptions = [('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r)),
                          ('partitionWeekdaysAndDates', partition_weekdays_and_dates),
                          ('ramGenerationIsoWeekday', ram_generation_iso_weekday)]
    weeklyStepsSparkOptions = [("conf", "spark.sql.shuffle.partitions=18000")]
    class_name = 'WeeklyContainmentVectorValues'

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=weeklyStepsOptions,
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=75),
        additional_args_option_pairs_list=weeklyStepsSparkOptions,
    )  # Usually takes about 45 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_weekly_containment_targeting_data_cluster(
    ram_timestamp: str, partition_weekdays_and_dates: str, ram_generation_iso_weekday: str
) -> ClusterAndTask:
    cluster = create_cluster(
        'UserSampledWeeklyTargetingDataContainmentRecordsCluster',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48).with_ebs_size_gb(1536)
        ],
        on_demand_weighted_capacity=1600,  # Same as forecasting-ram-pipeline
        maximize_resource_allocation=True,
    )

    weeklyStepsOptions = [('ramGenerationTimestamp', ram_timestamp), ('hmhP', str(hmh_p)), ('hmhR', str(hmh_r)),
                          ('partitionWeekdaysAndDates', partition_weekdays_and_dates),
                          ('ramGenerationIsoWeekday', ram_generation_iso_weekday)]
    weeklyStepsSparkOptions = [("conf", "spark.sql.shuffle.partitions=18000")]
    class_name = 'WeeklyContainmentTargetingData'

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=weeklyStepsOptions,
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=120),
        additional_args_option_pairs_list=weeklyStepsSparkOptions,
    )  # Usually takes about 90 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_prepare_data_for_export_step(ram_timestamp: str):
    class_name = 'PrepareDataForExport'
    cluster = create_cluster(
        f'{class_name}Cluster',
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        ],
        on_demand_weighted_capacity=800
    )

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=[
            ('ramGenerationTimestamp', ram_timestamp),
        ],
        executable_path=jar_path,
        timeout_timedelta=timedelta(minutes=45)
    )  # Usually takes about 30 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def create_write_to_aerospike_step(
    ram_timestamp: str, partition_weekdays_and_dates: str, ram_generation_iso_weekday: str
) -> ClusterAndTask:
    name = 'WriteDataToAerospike'
    cluster = create_cluster(
        name,
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(1024),
            R5.r5_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48).with_ebs_size_gb(1536)
        ],
        on_demand_weighted_capacity=256,
    )

    step = EmrJobTask(
        name=name,
        class_name=f'{scala_ns}.{name}',
        eldorado_config_option_pairs_list=[
            ('ramGenerationTimestamp', ram_timestamp),
            ('partitionWeekdaysAndDates', partition_weekdays_and_dates),
            ('ramGenerationIsoWeekday', ram_generation_iso_weekday),
            ('aerospikeHosts', aerospike_hosts),
            ('aerospikeNamespace', aerospike_namespace),
            ('aerospikeSet', f'{{{{ task_instance.xcom_pull(key="{aerospike_xcom_inactive_set_key}") }}}}'),
            ('aerospikeUseBufferedWrites', aerospike_use_buffered_writes),
            # Below are experimentally chosen parameters for optimal BufferedWrites operation
            ('aerospikeTransactionRate', aerospike_transaction_rate),
            ('aerospikeMaxAsyncConnectionsPerNode', aerospike_max_async_connections_per_node),
        ],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=6)
    )

    cluster.add_parallel_body_task(step)

    return ClusterAndTask(cluster, step)


def create_validate_and_collect_data_metrics_step(ram_timestamp: str) -> ClusterAndTask:
    class_name = 'ValidateAndCollectDataMetrics'
    cluster = create_cluster(
        f'{class_name}Cluster',
        instance_types=[
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
        ],
        on_demand_weighted_capacity=800
    )

    step = EmrJobTask(
        name=class_name,
        class_name=f'{scala_ns}.{class_name}',
        eldorado_config_option_pairs_list=[('ramGenerationTimestamp', ram_timestamp),
                                           ('ramGenerationIsoWeekday', get_ram_generation_iso_weekday()),
                                           ('partitionWeekdaysAndDates', get_ram_partition_weekdays_and_days())],
        executable_path=jar_path,
        timeout_timedelta=timedelta(hours=2)
    )  # Usually takes about 50 minutes.

    cluster.add_parallel_body_task(step)
    return ClusterAndTask(cluster, step)


def calc_ram_timestamp(**context):
    run_date_str = context['ds']
    run_date = datetime.strptime(run_date_str, '%Y-%m-%d')
    # run_date is wall-clock time minus 1 day, see:
    # https://stackoverflow.com/questions/67251814/airflow-execution-date-is-confusing
    # So if it is April-1 today (wall clock time), then the run_date airflow gives us
    # will be 3-31
    ram_timestamp = run_date
    ram_timestamp_str = ram_timestamp.strftime('%Y%m%d')
    logging.info(f'Calculated ram_timestamp = {ram_timestamp_str} for run_date = {run_date_str}')
    task_instance = context['task_instance']
    task_instance.xcom_push(key=ram_timestamp_key, value=ram_timestamp_str)

    ram_generation_iso_weekday = str(ram_timestamp.isoweekday())
    logging.info(f'Setting {ram_generation_iso_weekday_key} to {ram_generation_iso_weekday}')
    context["task_instance"].xcom_push(key=ram_generation_iso_weekday_key, value=ram_generation_iso_weekday)


# DAG declaration to make visible to airflow
dag: DAG


class DeviceSampledDags:

    def __init__(self, global_mode: bool):
        if global_mode:
            # prod-mode, where steps are wired as a DAG
            # The date is calculated based on DAG scheduled run date
            # It's calculated to be 1 day prior to the DAG run date to ensure
            # SIB and avails data is populated
            self.ram_timestamp = get_ram_timestamp_template()
            self.ram_partition_weekday_and_days = get_ram_partition_weekdays_and_days()
            self.ram_generation_iso_weekday = get_ram_generation_iso_weekday()
        else:
            # test mode running individual steps as separate DAGs
            # The RAM timestamp is provided as a parameter
            self.ram_timestamp = '{{ dag_run.conf["ram_timestamp"] }}'

        self.map_avail = create_map_avails_step(self.ram_timestamp)
        self.preprocess_pre_bid_step = create_pre_process_pre_bid_step(self.ram_timestamp)
        self.vector_values_hmh = create_user_sampled_avails_vector_hmh_step(self.ram_timestamp)
        self.partial_agg_hmh_on_rcukh = create_partial_agg_hmh_on_rcukh_step(self.ram_timestamp)
        self.join_agg_hmh_on_rcukh_and_pre_bid = create_join_agg_hmh_on_rcukh_and_preprocessed_pre_bid_step(self.ram_timestamp)
        self.prebid_categories_hmh_agg = create_prebid_categories_hmh_agg_step(self.ram_timestamp)

        self.targeting_data = create_targeting_data_step(self.ram_timestamp)
        self.xd_targeting_data = create_xd_targeting_data_step(self.ram_timestamp)
        self.num_slices = 3
        self.targeting_data_aggregate_steps = [
            create_targeting_data_aggregate_step(slice_idx, self.num_slices, self.ram_timestamp) for slice_idx in range(0, self.num_slices)
        ]
        self.targeting_data_aggregate_merge_step = \
            create_targeting_data_aggregate_merge_step(self.ram_timestamp)

        # Containment record steps
        self.create_containment_meta_records = \
            create_containment_meta_records_step(self.ram_timestamp)
        self.create_containment_records_vector_values = \
            create_containment_records_vector_values_step(self.ram_timestamp)
        self.filter_sib_for_containment_records_step = \
            filter_sib_for_containment_records(self.ram_timestamp)
        self.filter_xd_for_containment_records_step = \
            filter_xd_for_containment_records(self.ram_timestamp)
        self.create_tgtng_and_ctxtl_cntnmt_recs_step = \
            create_targeting_containment_records(self.ram_timestamp)
        self.create_pre_bid_containment_records_step = create_pre_bid_containment_records(self.ram_timestamp)

        self.weekly_vectors_and_meta_records = create_weekly_processing_cluster(
            self.ram_timestamp, self.ram_partition_weekday_and_days, self.ram_generation_iso_weekday
        )

        self.weekly_vv_containment_records = create_weekly_containment_vector_values_cluster(
            self.ram_timestamp, self.ram_partition_weekday_and_days, self.ram_generation_iso_weekday
        )

        self.weekly_targeting_containment_records = create_weekly_containment_targeting_data_cluster(
            self.ram_timestamp, self.ram_partition_weekday_and_days, self.ram_generation_iso_weekday
        )

        # Adding a different step and cluster to process pre-bid containment records data.
        # This is needed because pre-bid daily containment records dataset is 3x bigger in TB
        # than TargetingData daily containment records.
        self.weekly_pre_bid_containment_records = create_weekly_pre_bid_weekly_cluster(
            self.ram_timestamp,
            self.ram_partition_weekday_and_days,
            self.ram_generation_iso_weekday,
        )

        self.prepare_data_for_export_step = \
            create_prepare_data_for_export_step(self.ram_timestamp)

        self.validate_and_collect_data_metrics_step = \
            create_validate_and_collect_data_metrics_step(self.ram_timestamp)

        self.write_data_to_aerospike_step = create_write_to_aerospike_step(
            self.ram_timestamp, self.ram_partition_weekday_and_days, self.ram_generation_iso_weekday
        )

    def wire_up_global_dag(self):
        ttd_dag = create_dag()
        calc_ram_date = PythonOperator(
            python_callable=calc_ram_timestamp, provide_context=True, task_id=calculate_ram_timestamp_task_id, dag=ttd_dag.airflow_dag
        )

        lock_inactive_aerospike_set_task = create_get_inactive_set_version_and_lock_task(
            dag=ttd_dag.airflow_dag,
            task_id=lock_inactive_aerospike_set_task_id,
            aerospike_hosts=aerospike_hosts,
            namespace=aerospike_namespace,
            metadata_set_name=aerospike_metadata_set_name,
            set_key=aerospike_set,
            inactive_xcom_set_number_key=aerospike_xcom_inactive_set_number_key,
            aerospike_gen_xcom_key=aerospike_xcom_gen_key,
            inactive_xcom_set_key=aerospike_xcom_inactive_set_key
        )

        update_active_aerospike_set_task = create_activate_and_unlock_set_version_task(
            dag=ttd_dag.airflow_dag,
            task_id=update_active_aerospike_set_task_id,
            aerospike_hosts=aerospike_hosts,
            inactive_get_task_id=lock_inactive_aerospike_set_task_id,
            job_name=dag_name,
            namespace=aerospike_namespace,
            metadata_set_name=aerospike_metadata_set_name,
            set_key=aerospike_set,
            inactive_xcom_set_number_key=aerospike_xcom_inactive_set_number_key,
            aerospike_gen_xcom_key=aerospike_xcom_gen_key
        )

        # This is a very complicated DAG, so refer to the diagram here that captures the core,
        # highly interconnected part:
        #
        # https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?spaceKey=EN&title=Guide%3A+RAM%2C+User-sampled-avails%2C+SIBv2%2C+Containment+records#Guide:RAM,Usersampledavails,SIBv2,Containmentrecords-DailyRAMJob:Sparkprocessingsteps
        #
        # It's tedious to make sure the diagram matches how we're setting up the DAG. Viewing the DAG in airflow
        # is unhelpful as it's very verbose with all the EMR sub-tasks. We've also made errors faithfully transcribing
        # the diagram to code, so here is a suggested procedure to reliably confirm we have it correctly done:
        # - Go line by line, double clicking on each task on the left side of the >> operator to highlight
        #   it in your editor
        # - Your editor should highlight everywhere else that task appears in this code block
        #   and confirm that the tasks' parents and children are correct
        ttd_dag >> self.map_avail.cluster
        ttd_dag >> self.preprocess_pre_bid_step.cluster

        calc_ram_date >> self.map_avail.cluster.first_airflow_op()
        calc_ram_date >> self.preprocess_pre_bid_step.cluster.first_airflow_op()

        self.map_avail.cluster >> self.targeting_data.cluster
        self.map_avail.cluster >> self.xd_targeting_data.cluster
        self.map_avail.cluster >> self.vector_values_hmh.cluster
        self.map_avail.cluster >> self.partial_agg_hmh_on_rcukh.cluster
        self.partial_agg_hmh_on_rcukh.cluster >> self.join_agg_hmh_on_rcukh_and_pre_bid.cluster
        self.preprocess_pre_bid_step.cluster >> self.join_agg_hmh_on_rcukh_and_pre_bid.cluster

        for i in range(0, self.num_slices):
            self.targeting_data.cluster >> self.targeting_data_aggregate_steps[i].cluster
            self.xd_targeting_data.cluster >> self.targeting_data_aggregate_steps[i].cluster
            self.targeting_data_aggregate_steps[i].cluster >> self.targeting_data_aggregate_merge_step.cluster

        self.join_agg_hmh_on_rcukh_and_pre_bid.cluster >> self.prebid_categories_hmh_agg.cluster

        self.weekly_vectors_and_meta_records.cluster >> self.weekly_vv_containment_records.cluster
        self.weekly_vectors_and_meta_records.cluster >> self.weekly_targeting_containment_records.cluster
        self.weekly_vectors_and_meta_records.cluster >> self.weekly_pre_bid_containment_records.cluster

        self.prebid_categories_hmh_agg.cluster >> self.create_containment_meta_records.cluster
        self.targeting_data_aggregate_merge_step.cluster >> self.create_containment_meta_records.cluster
        self.vector_values_hmh.cluster >> self.create_containment_meta_records.cluster

        # Actually, this step does not depend on the output of the meta records step. They can be started in the very
        # early steps of the pipeline. However, if the meta records step fails, it doesn't make sense
        # to start these steps.
        self.create_containment_meta_records.cluster >> self.create_containment_records_vector_values.cluster
        self.create_containment_meta_records.cluster >> self.create_pre_bid_containment_records_step.cluster
        self.create_containment_meta_records.cluster >> self.filter_sib_for_containment_records_step.cluster
        self.create_containment_meta_records.cluster >> self.filter_xd_for_containment_records_step.cluster

        self.filter_sib_for_containment_records_step.cluster >> self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster
        self.filter_xd_for_containment_records_step.cluster >> self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster

        # I have renamed the target_tasks so there are not enough ocurrences (at least not 7 days at the moment) of
        # these tasks in the current DAG. That's why we need to pass some hardcoded dates. This can be removed after 7
        # successful runs.
        hardcoded_dates = [
            date(2024, 9, 30),
            date(2024, 9, 24),
            date(2024, 10, 2),
            date(2024, 9, 26),
            date(2024, 9, 27),
            date(2024, 9, 28),
            date(2024, 9, 29),
        ]
        lookup_last_good_iso_weekdays_task = FindMostRecentSuccessfulDowDatesInPipeline(
            xcom_key=last_good_iso_weekdays_key,
            target_tasks=[
                'CreateTargetingContainmentRecords_watch_task_CreateTargetingContainmentRecords',
                'ContainmentRecordsVectorValuesCluster_watch_task_ContainmentRecordsVectorValues',
                'ContainmentRecordsPreBid_watch_task_ContainmentRecordsPreBid'
            ],
            dag=ttd_dag.airflow_dag,
            task_id='lookup_last_good_iso_weekdays',
            hardcoded_dates=hardcoded_dates
        )
        self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster.last_airflow_op() >> lookup_last_good_iso_weekdays_task
        self.create_containment_records_vector_values.cluster.last_airflow_op() >> lookup_last_good_iso_weekdays_task

        lookup_last_good_iso_weekdays_task >> self.weekly_vectors_and_meta_records.cluster.first_airflow_op()
        lookup_last_good_iso_weekdays_task >> self.weekly_pre_bid_containment_records.cluster.first_airflow_op()

        self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster >> self.weekly_targeting_containment_records.cluster
        self.create_containment_records_vector_values.cluster >> self.weekly_vv_containment_records.cluster

        self.create_containment_records_vector_values.cluster >> self.weekly_vectors_and_meta_records.cluster

        # Note that this dependency is needed. If we make a dependency from last_good_iso_weekdays to first_airflow_op
        # of pre-bid only, then the graph will have only the step of "select subnets" but not the whole cluster
        self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster >> self.weekly_pre_bid_containment_records.cluster
        self.create_pre_bid_containment_records_step.cluster >> self.weekly_pre_bid_containment_records.cluster

        self.create_containment_records_vector_values.cluster >> self.weekly_vectors_and_meta_records.cluster
        self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster >> self.weekly_vectors_and_meta_records.cluster
        self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster >> self.weekly_targeting_containment_records.cluster

        self.weekly_vectors_and_meta_records.cluster >> self.prepare_data_for_export_step.cluster
        self.weekly_vv_containment_records.cluster >> self.prepare_data_for_export_step.cluster
        self.weekly_targeting_containment_records.cluster >> self.prepare_data_for_export_step.cluster
        self.weekly_pre_bid_containment_records.cluster >> self.prepare_data_for_export_step.cluster
        self.prepare_data_for_export_step.cluster >> self.validate_and_collect_data_metrics_step.cluster

        self.validate_and_collect_data_metrics_step.cluster >> lock_inactive_aerospike_set_task
        self.validate_and_collect_data_metrics_step.cluster >> self.write_data_to_aerospike_step.cluster

        lock_inactive_aerospike_set_task >> self.write_data_to_aerospike_step.cluster

        self.write_data_to_aerospike_step.cluster >> update_active_aerospike_set_task

        global dag
        dag = ttd_dag.airflow_dag

    def wire_up_individual_dags(self):
        map_avails_dag = create_dag('MapAvails')
        map_avails_dag >> self.map_avail.cluster

        preprocess_pre_bid_dag = create_dag('PreprocessPreBid')
        preprocess_pre_bid_dag >> self.preprocess_pre_bid_step.cluster

        vector_values_hmh_dag = create_dag('VectorValuesHMH')
        vector_values_hmh_dag >> self.vector_values_hmh.cluster

        partial_agg_hmh_on_rcukh_dag = create_dag('PartialAggOnRcukhDag')
        partial_agg_hmh_on_rcukh_dag >> self.partial_agg_hmh_on_rcukh.cluster

        join_hmh_rcukh_with_prebid_data_dag = create_dag('JoinAvailsWithPrebidData')
        join_hmh_rcukh_with_prebid_data_dag >> self.join_agg_hmh_on_rcukh_and_pre_bid.cluster

        prebid_categories_hmh_agg_dag = create_dag('PreBidCategoriesHmhAggregate')
        prebid_categories_hmh_agg_dag >> self.prebid_categories_hmh_agg.cluster

        targeting_data_dag = create_dag('TargetingData')
        targeting_data_dag >> self.targeting_data.cluster

        xd_targeting_data_dag = create_dag('XdTargetingData')
        xd_targeting_data_dag >> self.xd_targeting_data.cluster

        targeting_data_aggregate_dag = create_dag('TargetingDataAggregate')
        for i in range(0, self.num_slices):
            targeting_data_aggregate_dag >> self.targeting_data_aggregate_steps[i].cluster

        targeting_data_aggregate_merge_dag = create_dag('TargetingDataAggregateMerge')
        targeting_data_aggregate_merge_dag >> self.targeting_data_aggregate_merge_step.cluster

        create_containment_meta_records_dag = create_dag('ContainmentMetaRecords')
        create_containment_meta_records_dag >> self.create_containment_meta_records.cluster

        create_containment_records_vector_values_dag = create_dag('ContainmentRecordsVectorValues')
        create_containment_records_vector_values_dag >> self.create_containment_records_vector_values.cluster

        filter_sib_for_containment_records__dag = create_dag('FilterSibForContainmentRecordsStep')
        filter_sib_for_containment_records__dag >> self.filter_sib_for_containment_records_step.cluster

        filter_xd_for_containment_records_dag = create_dag('FilterXdForContainmentRecords')
        filter_xd_for_containment_records_dag >> self.filter_xd_for_containment_records_step.cluster

        create_tgtng_and_ctxtl_cntnmnt_recs_dag = create_dag('CreateTgtngAndCtxtlCntnmntRecs')
        create_tgtng_and_ctxtl_cntnmnt_recs_dag >> self.create_tgtng_and_ctxtl_cntnmt_recs_step.cluster

        create_pre_bid_containment_records_dag = create_dag('CreatePreBidContainmentRecords')
        create_pre_bid_containment_records_dag >> self.create_pre_bid_containment_records_step.cluster

        create_pre_bid_weekly_records_dag = create_dag('WeeklyPreBidContainmentRecords')
        create_pre_bid_weekly_records_dag >> self.weekly_pre_bid_containment_records


# Uncomment to create individual dags for each step. This is helpful during testing/debugging
# to run a single step at a time
# individual_dags = DeviceSampledDags(False)
# individual_dags.wire_up_individual_dags()

# In production, we will have all the steps wired up in a single DAG
prod_dags = DeviceSampledDags(True)
prod_dags.wire_up_global_dag()
