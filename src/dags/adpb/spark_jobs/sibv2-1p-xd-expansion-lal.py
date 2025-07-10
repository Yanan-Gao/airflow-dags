"""
1p device LAL Calculation with expanded 1PD via XD Graph

Job Details:
    - Runs every 24 hours
    - LAL contains 3 stages: pixel selection, overlap count, relevance ratio computer
    - After LAL results are computed, next phrase is validating results and publishing to Insight Aerospike
"""
import copy
import logging

from datetime import datetime, timedelta, date

from airflow.operators.python import PythonOperator
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

from datasources.sources.sib_datasources import SibDatasources
from ttd.constants import DataTypeId
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m7a import M7a
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

# sib date
sib_lookback_days = 5
sib_date_key = "sib_date_key"
check_sib_date_task_id = "get-most-recent-sib-date"

calculation_date = "{{ ds }}"
calculation_date_nodash = "{{ ds_nodash }}"
# Config values
executionIntervalDays = 1
numContainers = 239
numCoresPerContainer = 31
numContainersPlusDriver = numContainers + 1
leastNumCoresPerNode = numCoresPerContainer + 1
numFleetCoresNeeded = numContainersPlusDriver * leastNumCoresPerNode
containerMemoryGb = 128
executorAllocatedMemoryGb = int(containerMemoryGb * 0.8)
memoryOverheadMb = int(executorAllocatedMemoryGb * 1024 / 10)
lalBatchSize = 3  # There will be 2 steps per cluster. So actual batch size will be 6

cluster_tags = {
    "Team": slack_groups.ADPB.team.jira_team,
}

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
job_start_date = datetime(2024, 8, 22, 7, 0)
job_schedule_interval = timedelta(hours=24)
job_slack_channel = "#scrum-adpb-alerts"
targeting_data_count_subdag_task_id = "1p_xd_LAL"
job_environment = TtdEnvFactory.get_from_system()

sibv2_user_lal = TtdDag(
    dag_id="adpb-sibv2-device-1p-xd-expansion-lal",
    start_date=job_start_date,  # Current LAL process runs at 7am UTC
    schedule_interval=job_schedule_interval,
    max_active_runs=1,
    slack_channel=job_slack_channel
)

pixel_selection_and_permission_step_options = [
    ("totalUniquesCount", "600000000000"),
    ("totalPixelsCount", "120000"),
    ("maxPixelSelectionLookBackDays", 14),
    (
        "pixelDataTypes",
        ",".join(map(str, [e.value for e in [DataTypeId.IPAddressRange, DataTypeId.ImportedAdvertiserData, DataTypeId.CrmData]]))
    ),
    ("additionalTotalUniquesCount", "450000000000"),
    ("additionalTotalPixelsCount", "90000"),
    ("additionalMaxPixelSelectionLookBackDays", 14),
    (
        "additionalPixelDataTypes", ",".join(
            map(
                str,
                [e.value for e in [DataTypeId.ClickRetargeting, DataTypeId.MidPoint, DataTypeId.Complete, DataTypeId.CampaignSeedData]]
            )
        )
    ),
    ("campaignConversionFirstReportingColumnTotalUniquesCount", "20000000000"),
    ("campaignConversionFirstReportingColumnTotalPixelsCount", "30000"),
    ("campaignConversionFirstReportingColumnMaxPixelSelectionLookBackDays", 7),
    ("seedTotalUniquesCount", "600000000000"),
    ("seedTotalPixelsCount", "120000"),
    ("seedPixelSelectionLookBackDays", 7),
    ("offlineTrackingTagTotalUniquesCount", "60000000000"),
    ("offlineTrackingTagTotalPixelsCount", "12000"),
    ("offlineTrackingTagPixelSelectionLookBackDays", 7),
    ("totalPixelWithMissingLalUniquesCount", "1000000000"),
    ("totalPixelWithMissingLalCount", "1000"),
    ("pixelWithMissingLalSelectionLookBackDays", 1),
    ("date", calculation_date),
]

pixel_selection_and_permission_cluster = EmrClusterTask(
    name="PixelSelectionCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=256
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

pixel_selection_and_permission_step = EmrJobTask(
    name="LAL-Pixel-Selection",
    class_name="jobs.lal.device.DeviceExpansionPixelSelectionAndPermissionComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=[("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=47"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000"),
                                       ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")],
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=pixel_selection_and_permission_step_options,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=pixel_selection_and_permission_cluster.cluster_specs,
)

lal_spark_options_list = [
    ("executor-memory", f"{executorAllocatedMemoryGb}G"),
    ("executor-cores", str(numCoresPerContainer)),
    ("num-executors", str(numContainers)),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
    ("conf", f"spark.driver.memory={executorAllocatedMemoryGb}G"),
    ("conf", f"spark.driver.cores={numCoresPerContainer}"),
    ("conf", f"spark.default.parallelism={numFleetCoresNeeded * 3}"),
    ("conf", f"spark.sql.shuffle.partitions={numFleetCoresNeeded * 3}"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
    ("conf", f"spark.driver.memoryOverhead={memoryOverheadMb}m"),
    ("conf", f"spark.executor.memoryOverhead={memoryOverheadMb}m"),
    ("conf", "spark.memory.fraction=0.8"),
    ("conf", "spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures=5"),
    ("conf", "spark.rdd.compress=true"),
    ("conf", "spark.shuffle.compress=true"),
    ("conf", "spark.shuffle.spill.compress=true"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.driver.maxResultSize=48G"),
    ("conf", "spark.kryoserializer.buffer.max=2047m"),
]


# check sib date
def get_sib_date(**kwargs):
    hook = AwsCloudStorage(conn_id='aws_default')
    check_recent_uniques_date = SibDatasources.sibv2_device_xd_data_uniques().check_recent_data_exist(hook, date.today(), sib_lookback_days)
    if not check_recent_uniques_date:
        raise ValueError(f'Could not find sibv2 in last {sib_lookback_days} days')

    check_recent_bitmap_date = SibDatasources.sibv2_device_seven_day_rollup_index_bitmap.check_recent_data_exist(
        hook, date.today(), sib_lookback_days
    )
    if not check_recent_bitmap_date:
        raise ValueError(f'Could not find sibv2 bitmap in last {sib_lookback_days} days')

    check_recent_date = min(check_recent_uniques_date.get(), check_recent_bitmap_date.get())

    # get the most recent date
    sib_date = check_recent_date.strftime('%Y-%m-%d')
    kwargs['task_instance'].xcom_push(key=sib_date_key, value=sib_date)
    test_date = kwargs['task_instance'].xcom_pull(
        dag_id=sibv2_user_lal.airflow_dag.dag_id, task_ids=check_sib_date_task_id, key=sib_date_key
    )
    logging.info(f'Found sibv2 date: {test_date}')


check_recent_sib_date = OpTask(
    op=PythonOperator(task_id=check_sib_date_task_id, python_callable=get_sib_date, dag=sibv2_user_lal.airflow_dag, provide_context=True)
)


def generate_targeting_data_count_subdag(lalBatchSize):

    def generate_count_clusters(batchId):
        steps_per_cluster = 3
        name_prefix = "Bitmap"
        batchSize = lalBatchSize
        lal_cluster = EmrClusterTask(
            name=f"LAL_{name_prefix}_{batchId + 1}_of_{batchSize}",
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
            ),
            cluster_tags=cluster_tags,
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    M5.m5_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
                    M7a.m7a_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=numContainersPlusDriver
            ),
            additional_application_configurations=copy.deepcopy(application_configuration),
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
        )

        for i in range(steps_per_cluster):
            local_batch_size = batchSize * steps_per_cluster
            local_batch_id = i * batchSize + batchId
            lal_count_step_options = [("date", calculation_date), ("batchSize", local_batch_size), ("batch", local_batch_id),
                                      ("xdVendorId", "10"), ("numPartitions", numFleetCoresNeeded * 6),
                                      (
                                          "sibDate", "{{ task_instance.xcom_pull(dag_id='" + sibv2_user_lal.airflow_dag.dag_id +
                                          "', task_ids='" + check_sib_date_task_id + "', key='" + sib_date_key + "') }}"
                                      )]

            lal_counts_step = EmrJobTask(
                name=f"LAL_{name_prefix}_counts_{local_batch_id + 1}_of_{local_batch_size}",
                class_name="jobs.lal.device.RoaringBitmapBasedDeviceExpansionTargetingDataCountJob",
                executable_path=jar_path,
                additional_args_option_pairs_list=lal_spark_options_list,
                eldorado_config_option_pairs_list=lal_count_step_options,
                timeout_timedelta=timedelta(hours=7),
                cluster_specs=lal_cluster.cluster_specs,
            )
            lal_cluster.add_sequential_body_task(lal_counts_step)
        return lal_cluster

    lal_clusters = [generate_count_clusters(batchId) for batchId in range(lalBatchSize)]

    return lal_clusters


lal_clusters = generate_targeting_data_count_subdag(lalBatchSize)

lal_relevance_ratio_step_options = [("minimumThirdPartyDataUserOverlap", 1), ("minimumRelevanceRatioThreshold", 1.25), ("alpha", 0.05),
                                    ("lowValueOverlapThreshold", 0.00001), ("highValeOverlapThreshold", 0.0001),
                                    ("minimumUniquesUsersThreshold", 6100), ("maxResultsPerPixel", 500),
                                    ("fillCompleteResultLowLALPercentage", 0.2), ("date", calculation_date),
                                    (
                                        "sibDate", "{{ task_instance.xcom_pull(dag_id='" + sibv2_user_lal.airflow_dag.dag_id +
                                        "', task_ids='" + check_sib_date_task_id + "', key='" + sib_date_key + "') }}"
                                    )]

user_lal_ratio_cluster = EmrClusterTask(
    name="RatioCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=2048
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

lal_relevance_ratio_spark_options_list = [("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                          ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=383"),
                                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                          ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                          ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                          ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                          ("conf", "spark.sql.shuffle.partitions=6000"),
                                          ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]
lal_relevance_ratio_step = EmrJobTask(
    name="LAL-Relevance-Ratio",
    class_name="jobs.lal.device.DeviceExpansionRelevanceRatioComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=lal_relevance_ratio_spark_options_list,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=lal_relevance_ratio_step_options,
    timeout_timedelta=timedelta(hours=3),
    cluster_specs=user_lal_ratio_cluster.cluster_specs,
)

lal_confident_relevance_ratio_step = EmrJobTask(
    name="LAL-Confident-Relevance-Ratio",
    class_name="jobs.lal.combined.SeedOverlapRelevanceScoreComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=lal_relevance_ratio_spark_options_list,
    eldorado_config_option_pairs_list=lal_relevance_ratio_step_options,
    timeout_timedelta=timedelta(hours=3),
    cluster_specs=user_lal_ratio_cluster.cluster_specs,
)

pixel_selection_and_permission_cluster.add_sequential_body_task(pixel_selection_and_permission_step)
user_lal_ratio_cluster.add_sequential_body_task(lal_relevance_ratio_step)
user_lal_ratio_cluster.add_sequential_body_task(lal_confident_relevance_ratio_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status = OpTask(op=FinalDagStatusCheckOperator(dag=sibv2_user_lal.airflow_dag))

# Pixel Selection to count
sibv2_user_lal >> pixel_selection_and_permission_cluster >> check_recent_sib_date

# Relevance ratio computation takes place after counts have successfully been computed
# Pushing results to aerospike takes place after all results have been computed (Relevance Ratio step finishes)
for lal_cluster in lal_clusters:
    check_recent_sib_date >> lal_cluster >> user_lal_ratio_cluster

user_lal_ratio_cluster >> final_dag_status

dag = sibv2_user_lal.airflow_dag
