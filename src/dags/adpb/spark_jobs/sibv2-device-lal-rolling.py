import copy
import logging

from datetime import datetime, timedelta

from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_provider import CloudProviders
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.adpb.datasets.datasets import sib_rolling_device_data_uniques, targeting_data_permissions_rolling

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import ADPB

dag_name = "adpb-sibv2-device-lal-rolling"
owner = ADPB.team
cluster_tags = {"Team": owner.jira_team}

# Job config
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
job_start_date = datetime(2024, 8, 22, 6, 0)
job_schedule_interval = timedelta(hours=12)
job_environment = TtdEnvFactory.get_from_system()
env_str = "prod" if job_environment == TtdEnvFactory.prod else "test"

execution_date_time = "{{ data_interval_end.strftime(\"%Y-%m-%dT%H:00:00\") }}"
delta_days = 0
run_date_str = "{{ data_interval_end.strftime(\"%Y%m%d\") }}"
run_hour_str = "{{ data_interval_end.strftime(\"%H\") }}"
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

# Cluster config
num_containers_pixel_selection = 16  # r5_4xlarge
num_containers_lal_ratio = 32  # r5_4xlarge
num_containers_lal_counts = 60  # r5_24xlarge
num_cores_per_container = R5a.r5a_24xlarge().cores - 1
num_containers_plus_driver = num_containers_lal_counts + 1
least_num_cores_per_node = num_cores_per_container + 1
num_fleet_cores_needed = num_containers_plus_driver * least_num_cores_per_node
allocatedMemory = "600G"
batch_size = 1

sib_lookback_days = 5
sib_date_key = "sib_date_key"
read_sib_date_task_id = "read-sib-date"
sib_date_value = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='" + read_sib_date_task_id + "', key='" + sib_date_key + "') }}"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000"
    }
}, {
    "Classification": "core-site",
    "Properties": {
        "fs.s3a.connection.maximum": "1000",
        "fs.s3a.threads.max": "50"
    }
}, {
    "Classification": "capacity-scheduler",
    "Properties": {
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}, {
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.vmem-check-enabled": "false"
    }
}]

sibv2_rolling_lal_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    retries=0
)


def skip_downstream_on_timeout(context):
    exception = context['exception']
    if isinstance(exception, AirflowSensorTimeout):
        context['task_instance'].set_state("skipped")
        logging.info("Sensor skipped on timeout")


rolling_uniques_overlaps_sensor_task = OpTask(
    op=DatasetCheckSensor(
        task_id='check_rolling_uniques_overlaps',
        datasets=[sib_rolling_device_data_uniques.with_check_type("hour")],
        ds_date='{{data_interval_end.to_datetime_string()}}',
        cloud_provider=CloudProviders.aws,
        dag=sibv2_rolling_lal_dag.airflow_dag,
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 6,  # wait up to 6 hours,
        on_failure_callback=skip_downstream_on_timeout
    )
)


# read pre-calculated sib date from s3 (_SIB_DATE)
def read_sib_date_file(**kwargs):
    s3_hook = AwsCloudStorage(conn_id='aws_default')
    key = kwargs['key']
    bitmap_key = kwargs['bitmap_key']
    bucket = kwargs['bucket']
    sib_date = s3_hook.read_key(key=key, bucket_name=bucket)
    bitmap_sib_date = s3_hook.read_key(key=bitmap_key, bucket_name=bucket)
    if sib_date != bitmap_sib_date:
        raise ValueError(f'sib date in uniques ({sib_date}) differs from sib date in bitmap ({bitmap_sib_date})')
    kwargs['task_instance'].xcom_push(key=sib_date_key, value=sib_date)
    test_date = kwargs['task_instance'].xcom_pull(dag_id=dag_name, task_ids=read_sib_date_task_id, key=sib_date_key)
    logging.info(f"Read sib date: {test_date} from file {bucket}/{key} and {bucket}/{bitmap_key}")
    return f'{bucket}/{key} - {test_date}'


read_sib_date_task = OpTask(
    op=PythonOperator(
        task_id=read_sib_date_task_id,
        provide_context=True,
        op_kwargs={
            'key': f"datapipeline/{env_str}/seeninbiddingrollingdevicedatauniques/v=2/date={run_date_str}/hour={run_hour_str}/_SIB_DATE",
            'bitmap_key':
            f"datapipeline/{env_str}/seeninbiddingrollingdevicesevendayrollupindexbitmap/v=2/date={run_date_str}/hour={run_hour_str}/_SIB_DATE",
            'bucket': "ttd-identity"
        },
        python_callable=read_sib_date_file,
        dag=sibv2_rolling_lal_dag.airflow_dag,
    )
)

pixel_selection_and_permission_cluster = EmrClusterTask(
    name="LAL-pixel-selection-permission-rolling-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5a.r5a_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5a.r5a_12xlarge().with_ebs_size_gb(384).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(384).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=num_containers_pixel_selection,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=30 * 60,
    environment=job_environment
)

pixel_selection_and_permission_step = EmrJobTask(
    name="LAL-pixel-selection-permission",
    class_name="jobs.lal.device.RollingDevicePixelSelectionAndPermissionComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=[("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=47"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000"),
                                       ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")],
    eldorado_config_option_pairs_list=[
        ("seedTotalUniquesCount", "3000000000"),
        ("seedTotalPixelsCount", "1000"),  # maximum select 1k seeds each run
        ("seedPixelSelectionLookBackDays", 7),
        ("dateTime", execution_date_time),
        ("sibDate", sib_date_value)
    ],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=pixel_selection_and_permission_cluster.cluster_specs,
)
pixel_selection_and_permission_cluster.add_sequential_body_task(pixel_selection_and_permission_step)

# check if there are any pixels to process, skip otherwise
rolling_pixel_permissions_sensor_task = OpTask(
    op=DatasetCheckSensor(
        task_id='check_rolling_pixel_permissions',
        datasets=[targeting_data_permissions_rolling.with_check_type("hour")],
        ds_date='{{data_interval_end.to_datetime_string()}}',
        cloud_provider=CloudProviders.aws,
        dag=sibv2_rolling_lal_dag.airflow_dag,
        poke_interval=60,  # poke every 1 minute
        timeout=60 * 5,  # wait up to 5 minutes
        on_failure_callback=skip_downstream_on_timeout
    )
)


def generate_targeting_data_count_subdag(batch_size):
    user_lal_clusters = []

    def generate_count_clusters(batch_id):
        lal_cluster = EmrClusterTask(
            name=f"LAL-data-counts-batch-{batch_id + 1}-rolling-cluster",
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)],
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    R5a.r5a_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(96),
                    R5.r5_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(96),
                    R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
                ],
                on_demand_weighted_capacity=num_fleet_cores_needed
            ),
            cluster_tags=cluster_tags,
            additional_application_configurations=copy.deepcopy(application_configuration),
            emr_release_label=emr_release_label,
            enable_prometheus_monitoring=True,
            environment=job_environment
        )

        lal_counts_step = EmrJobTask(
            name=f"LAL-counts-{batch_id + 1}-of-{batch_size}",
            class_name="jobs.lal.device.RoaringBitmapBasedRollingDeviceTargetingDataCountJob",
            executable_path=jar_path,
            additional_args_option_pairs_list=[
                ("executor-memory", allocatedMemory), ("executor-cores", str(num_cores_per_container)),
                ("num-executors", str(num_containers_lal_counts)), ("conf", "spark.driver.memory=" + allocatedMemory),
                ("conf", "spark.driver.cores=" + str(num_cores_per_container)), ("conf", "spark.dynamicAllocation.enabled=false"),
                ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.maxResultSize=6G"),
                ("conf", "spark.sql.shuffle.partitions=" + str(num_containers_lal_counts * num_cores_per_container)),
                ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")
            ],
            eldorado_config_option_pairs_list=[
                ("dateTime", execution_date_time),
                ("sibDate", sib_date_value),
                ("numPartitions", str(num_containers_lal_counts * num_cores_per_container)),
                ("numNodes", str(num_containers_lal_counts)),
                ("batchSize", batch_size),
                ("batch", batch_id),
            ],
            timeout_timedelta=timedelta(hours=5),
            cluster_specs=lal_cluster.cluster_specs,
        )
        lal_cluster.add_sequential_body_task(lal_counts_step)
        return lal_cluster, lal_counts_step

    for batchId in range(batch_size):
        user_lal_cluster, lal_count_step = generate_count_clusters(batchId)
        user_lal_clusters.append(user_lal_cluster)

    return user_lal_clusters


user_lal_clusters = generate_targeting_data_count_subdag(batch_size)

user_lal_ratio_cluster = EmrClusterTask(
    name="LAL-relevance-ratio-rolling-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=num_containers_lal_ratio
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=30 * 60,
    environment=job_environment
)

lal_relevance_ratio_step = EmrJobTask(
    name="LAL-relevance-ratio",
    class_name="jobs.lal.device.RollingDeviceRelevanceRatioComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=[("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=95"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000"),
                                       ("conf", "spark.sql.parquet.enableVectorizedReader=false"),
                                       ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")],
    eldorado_config_option_pairs_list=[("minimumThirdPartyDataUserOverlap", 1), ("minimumRelevanceRatioThreshold", 1.25), ("alpha", 0.05),
                                       ("lowValueOverlapThreshold", 0.00001), ("highValeOverlapThreshold", 0.0001),
                                       ("minimumUniquesUsersThreshold", 6100), ("maxResultsPerPixel", 500),
                                       ("fillCompleteResultLowLALPercentage", 0.2), ("dateTime", execution_date_time),
                                       ("sibDate", sib_date_value)],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=user_lal_ratio_cluster.cluster_specs,
)
user_lal_ratio_cluster.add_sequential_body_task(lal_relevance_ratio_step)

final_dag_check = OpTask(
    op=FinalDagStatusCheckOperator(dag=sibv2_rolling_lal_dag.airflow_dag, trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
)

# Wait for Rolling Uniques and Overlaps, and read sib date
sibv2_rolling_lal_dag >> rolling_uniques_overlaps_sensor_task >> read_sib_date_task >> pixel_selection_and_permission_cluster >> rolling_pixel_permissions_sensor_task

for user_lal_cluster in user_lal_clusters:
    rolling_pixel_permissions_sensor_task >> user_lal_cluster >> user_lal_ratio_cluster

user_lal_ratio_cluster >> final_dag_check

dag = sibv2_rolling_lal_dag.airflow_dag
