"""
1p device LAL Calculation and publish results to Insight Aerospike

Job Details:
    - Runs every 6 hours
    - LAL contains 3 stages: pixel selection, overlap count, relevance ratio computer
    - After LAL results are computed, next phrase is validating results and publishing to Insight Aerospike
"""
import copy
import logging
from datetime import datetime, timedelta, date

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from datasources.sources.sib_datasources import SibDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

# sib date
sib_lookback_days = 5
sib_date_key = "sib_date_key"
check_sib_date_task_id = "get-most-recent-sib-date"

calculation_date = "{{ ds }}"
execution_date_time = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"
# Config values
executionIntervalDays = 1
numContainers = 70
numCoresPerContainer = 95
numContainersPlusDriver = numContainers + 1
leastNumCoresPerNode = numCoresPerContainer + 1
numFleetCoresNeeded = numContainersPlusDriver * leastNumCoresPerNode
allocatedMemory = "600G"
batchSize = 1

cluster_tags = {
    "Team": ADPB.team.jira_team,
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
job_start_date = datetime(2024, 8, 21, 7, 0)
job_schedule_interval = timedelta(hours=12)
job_slack_channel = "#scrum-adpb-alerts"
targeting_data_count_subdag_task_id = "1p_LAL"
job_environment = TtdEnvFactory.get_from_system()
dag_name = "adpb-sibv2-device-1p-lal"
sibv2_user_lal = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,  # Current LAL process runs at 7am UTC
    schedule_interval=job_schedule_interval,
    max_active_runs=1,
    slack_channel=job_slack_channel
)
lal_dag = sibv2_user_lal.airflow_dag

pixel_selection_and_permission_step_options = [("seedTotalUniquesCount", "3000000000"), ("seedTotalPixelsCount", "600"),
                                               ("seedPixelSelectionLookBackDays", 7),
                                               ("campaignConversionFirstReportingColumnTotalUniquesCount", "25000000000"),
                                               ("campaignConversionFirstReportingColumnTotalPixelsCount", "5000"),
                                               ("campaignConversionFirstReportingColumnMaxPixelSelectionLookBackDays", 14),
                                               ("date", calculation_date), ("dateTime", execution_date_time)]

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
    class_name="jobs.firstpartylal.device.FirstPartyDevicePixelSelectionAndPermissionComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=[("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=47"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000")],
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=pixel_selection_and_permission_step_options,
    timeout_timedelta=timedelta(hours=2)
)
pixel_selection_and_permission_cluster.add_sequential_body_task(pixel_selection_and_permission_step)

lal_spark_options_list = [("executor-memory", allocatedMemory), ("executor-cores", str(numCoresPerContainer)),
                          ("num-executors", str(numContainers)), ("conf", "spark.driver.memory=" + allocatedMemory),
                          ("conf", "spark.driver.cores=" + str(numCoresPerContainer)), ("conf", "spark.dynamicAllocation.enabled=false"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC")]


# check sib date
def get_sib_date(**kwargs):
    hook = AwsCloudStorage(conn_id='aws_default')
    check_recent_date = SibDatasources.sibv2_device_data_uniques.check_recent_data_exist(hook, date.today(), sib_lookback_days)
    if check_recent_date:
        # get the most recent date
        sib_date = check_recent_date.get().strftime('%Y-%m-%d')
        kwargs['task_instance'].xcom_push(key=sib_date_key, value=sib_date)
        test_date = kwargs['task_instance'].xcom_pull(dag_id=dag_name, task_ids=check_sib_date_task_id, key=sib_date_key)
        logging.info(f'Found sibv2 date: {test_date}')

    else:
        raise ValueError(f'Could not find sibv2 in last {sib_lookback_days} days')


check_recent_sib_date_step = OpTask(
    op=PythonOperator(task_id=check_sib_date_task_id, python_callable=get_sib_date, dag=sibv2_user_lal.airflow_dag, provide_context=True)
)


def generate_count_clusters(batchId):
    lal_cluster = EmrClusterTask(
        name=f"LAL_{batchId + 1}_of_{batchSize}",
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
        ),
        cluster_tags=cluster_tags,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R5a.r5a_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(96),
                R5.r5_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(96),
                R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
            ],
            on_demand_weighted_capacity=numFleetCoresNeeded
        ),
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
    )

    lal_count_step_options = [("ttd.ds.FirstPartyTargetingDataPermissionsDataSet.isInChain", "true"),
                              ("ttd.ds.FirstPartySelectedPixelsDataSet.isInChain", "true"), ("date", calculation_date),
                              ("dateTime", execution_date_time), ("availCoresPerNode", str(numCoresPerContainer)),
                              ("numNodes", str(numContainers)), ("batchSize", batchSize), ("batchId", batchId),
                              (
                                  "sibDate", "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='" + check_sib_date_task_id +
                                  "', key='" + sib_date_key + "') }}"
                              )]

    lal_counts_step = EmrJobTask(
        name=f"LAL_counts_{batchId + 1}_of_{batchSize}",
        class_name="jobs.firstpartylal.device.DeviceFirstPartyTargetingDataCountJob",
        executable_path=jar_path,
        additional_args_option_pairs_list=lal_spark_options_list,
        eldorado_config_option_pairs_list=lal_count_step_options,
        timeout_timedelta=timedelta(hours=7)
    )
    lal_cluster.add_sequential_body_task(lal_counts_step)
    return lal_cluster


lal_clusters = [generate_count_clusters(batchId) for batchId in range(batchSize)]

# Dummy step used to link the steps together for Airflow visualisation purposes
count_start = OpTask(op=DummyOperator(task_id="targeting_data_count_start", dag=lal_dag))

count_end = OpTask(op=DummyOperator(task_id="targeting_data_count_end", dag=lal_dag))

for user_lal_cluster in lal_clusters:
    count_start >> user_lal_cluster >> count_end

lal_relevance_ratio_step_options = [("ttd.ds.FirstPartyTargetingDataPermissionsDataSet.isInChain", "true"),
                                    ("ttd.ds.FirstPartySelectedPixelsDataSet.isInChain", "true"),
                                    ("ttd.ds.FirstPartyCountsDataSet.isInChain", "true"), ("date", calculation_date),
                                    ("dateTime", execution_date_time),
                                    (
                                        "sibDate", "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='" +
                                        check_sib_date_task_id + "', key='" + sib_date_key + "') }}"
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
        on_demand_weighted_capacity=512
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

lal_relevance_ratio_step = EmrJobTask(
    name="LAL-Relevance-Ratio",
    class_name="jobs.firstpartylal.device.DeviceFirstPartyRelevanceRatioComputer",
    executable_path=jar_path,
    additional_args_option_pairs_list=[("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=95"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000")],
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=lal_relevance_ratio_step_options,
    timeout_timedelta=timedelta(hours=2)
)
user_lal_ratio_cluster.add_sequential_body_task(lal_relevance_ratio_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=lal_dag))

# Pixel Selection to count
sibv2_user_lal >> pixel_selection_and_permission_cluster
pixel_selection_and_permission_cluster >> check_recent_sib_date_step >> count_start

# Relevance ratio computation takes place after counts have successfully been computed
count_end >> user_lal_ratio_cluster
user_lal_ratio_cluster >> final_dag_status_step
