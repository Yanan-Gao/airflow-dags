import copy
import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from dags.adpb.spark_jobs.shared.counts_hmh_shared import get_xdgraph_date
from datasources.datasources import Datasources

from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.cluster_params import calc_cluster_params
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from ttd.eldorado.databricks.workflow import DatabricksWorkflow, DatabricksRegion
from ttd.openlineage import OpenlineageConfig, OpenlineageTransport


# Config values
class CrossDeviceVendorIds:
    """
    Cross Device Vendor IDs
    https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?pageId=122292736
    """
    DEVICE_VENDOR_ID = "0"
    PERSON_VENDOR_ID_ADBRAIN = "1"
    HOUSEHOLD_VENDOR_ID_ADBRAIN = "8",
    HOUSEHOLD_VENDOR_ID_MIP = "9",
    PERSON_VENDOR_ID_IAV2 = "10"
    HOUSEHOLD_VENDOR_ID_IAV2 = "11"
    PERSON_VENDOR_ID_IAV2_LEGACY = "610"
    HOUSEHOLD_VENDOR_ID_IAV2_LEGACY = "611"


jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

job_start_date = datetime(2024, 8, 21, 1, 0, 0)
job_schedule_interval = timedelta(days=1)

pipeline_name = "adpb-sibv2-group-data-pipeline"  # IMPORTANT: add "dev-" prefix when testing
sibv2_group_data_daily_agg_job_name = "adpb-sibv2-group-data-daily-agg"  # IMPORTANT: add "dev-" prefix when testing
sibv2_group_data_rollup_and_uniques_job_name = "adpb-sibv2-group-data-rollup-and-uniques"
legacy_graph_sibv2_group_data_rollup_and_uniques_job_name = "adpb-sibv2-group-data-rollup-and-uniques-legacy-graph"
sibv2_device_data_rollup_and_uniques_job_name = "adpb-sibv2-device-data-rollup-and-uniques"
sibv2_device_bitmap_job_name = "adpb-sibv2-device-bitmap"

sibDateToProcess = "{{ ds }}"
job_environment = TtdEnvFactory.get_from_system()
env_path = "prod" if job_environment == TtdEnvFactory.prod else "test"

databricks_spark_version = "15.4.x-scala2.12"

# XD variables
xd_graph_lookback_days = 12
xd_graph_date_key_iav2 = 'xd_graph_date_iav2'
xdVendorIds = [
    CrossDeviceVendorIds.DEVICE_VENDOR_ID, CrossDeviceVendorIds.PERSON_VENDOR_ID_IAV2, CrossDeviceVendorIds.HOUSEHOLD_VENDOR_ID_IAV2,
    CrossDeviceVendorIds.PERSON_VENDOR_ID_IAV2_LEGACY, CrossDeviceVendorIds.HOUSEHOLD_VENDOR_ID_IAV2_LEGACY
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.aimd.enabled": "true",
        "fs.s3.aimd.maxAttempts": "150000",
        "fs.s3.multipart.part.attempts": "600000",
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "150000",
        "fs.s3.sleepTimeSeconds": "10"
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
}, {
    "Classification": "spark-defaults",
    "Properties": {
        "spark.driver.maxResultSize": "30G",
        "spark.network.timeout": "360",
        "spark.sql.files.ignoreCorruptFiles": "true",
    }
}]


def generate_spark_option_list(instances_count, instance_type):
    cluster_params = calc_cluster_params(
        instances=instances_count,
        vcores=instance_type.cores,
        memory=instance_type.memory,
        parallelism_factor=2,
        max_cores_executor=instance_type.cores
    )
    spark_options_list = [("executor-memory", f'{cluster_params.executor_memory_with_unit}'),
                          ("executor-cores", f'{cluster_params.executor_cores}'),
                          ("conf", f"num-executors={cluster_params.executor_instances}"),
                          ("conf", f"spark.executor.memoryOverhead={cluster_params.executor_memory_overhead_with_unit}"),
                          ("conf", f"spark.driver.memoryOverhead={cluster_params.executor_memory_overhead_with_unit}"),
                          ("conf", f"spark.driver.memory={cluster_params.executor_memory_with_unit}"),
                          ("conf", f"spark.default.parallelism={cluster_params.parallelism}"),
                          ("conf", f"spark.sql.shuffle.partitions={cluster_params.parallelism}"), ("conf", "spark.speculation=false"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                          ("conf", f"spark.driver.maxResultSize={cluster_params.executor_memory_with_unit}"),
                          ("conf", "spark.network.timeout=12000s"),
                          ("conf", f"yarn.nodemanager.resource.memory-mb={cluster_params.node_memory * 1000}"),
                          ("conf", f"yarn.nodemanager.resource.cpu-vcores={cluster_params.vcores}"),
                          ("conf", "spark.executor.heartbeatInterval=1000s"), ("conf", "spark.sql.files.ignoreCorruptFiles=true")]
    return spark_options_list


cluster_tags = {
    'Team': ADPB.team.jira_team,
}

# DAG
dag_pipeline = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=0,
    max_active_runs=4,
    retry_delay=timedelta(hours=4),
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True,
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
)

dag = dag_pipeline.airflow_dag

# Pipeline dependencies
# Sanity check if the input sibv2log available
check_sibv2_log_data = OpTask(
    op=DatasetRecencyOperator(
        dag=dag,
        datasets_input=[Datasources.sib.sibv2_log],
        cloud_provider=CloudProviders.aws,
        xcom_push=True,
        task_id="check_sibv2_log_data",
    ),
)

check_sibv2_group_daily_data = OpTask(
    op=DatasetRecencyOperator(
        dag=dag,
        datasets_input=[
            Datasources.sib.sibv2_daily(CrossDeviceVendorIds.PERSON_VENDOR_ID_IAV2),
            Datasources.sib.sibv2_daily(CrossDeviceVendorIds.HOUSEHOLD_VENDOR_ID_IAV2),
        ],
        lookback_input=6,
        cloud_provider=CloudProviders.aws,
        xcom_push=True,
        task_id="check_sibv2_group_daily_data_for_the_last_7_days",
    ),
)

check_sibv2_group_daily_data_legacy_graph = OpTask(
    op=DatasetRecencyOperator(
        dag=dag,
        datasets_input=[
            Datasources.sib.sibv2_daily(CrossDeviceVendorIds.PERSON_VENDOR_ID_IAV2_LEGACY),
            Datasources.sib.sibv2_daily(CrossDeviceVendorIds.HOUSEHOLD_VENDOR_ID_IAV2_LEGACY),
        ],
        lookback_input=6,
        cloud_provider=CloudProviders.aws,
        xcom_push=True,
        task_id="check_sibv2_group_daily_data_for_the_last_7_days_legacy_graph",
    ),
)

check_sibv2_device_daily_data_for_rollup = OpTask(
    op=DatasetRecencyOperator(
        dag=dag,
        datasets_input=[Datasources.sib.sibv2_daily(CrossDeviceVendorIds.DEVICE_VENDOR_ID)],
        lookback_input=6,
        cloud_provider=CloudProviders.aws,
        xcom_push=True,
        task_id="check_sibv2_device_daily_data_for_the_last_7_days",
    ),
)

check_xd_graph = OpTask(
    op=PythonOperator(
        task_id="get-most-recent-xdgraph-date",
        python_callable=get_xdgraph_date,
        op_kwargs={'cloud_provider': 'aws'},
        dag=dag,
        templates_dict={'start_date': '{{ ds }}'},
        provide_context=True
    )
)


def add_success_file(job_date_str: str, bucket: str, path: str):
    aws_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    date_str = job_date_str.replace("-", "")
    aws_storage.put_object(bucket_name=bucket, key=f"{path}/date={date_str}/_SUCCESS", body="", replace=True)
    logging.info(f"Success file path: {path}/date={date_str}/")
    return "Success markers written"


####################################################################################################################
# SeenInBidding_V2_Group_Data_Daily_Agg
####################################################################################################################

sibv2_group_data_daily_agg_spark_options_list = generate_spark_option_list(240 * 3, R7g.r7g_4xlarge())

# Fleet Cluster
fleet_cluster = EmrClusterTask(
    name=sibv2_group_data_daily_agg_job_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R7g.r7g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
        ],
        on_demand_weighted_capacity=240 * 3
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

# Step
sibv2_group_data_daily_agg_options = [("sibDateToProcess", sibDateToProcess), ("xdVendorIds", ",".join(xdVendorIds))]

sibv2_group_data_daily_agg_step = EmrJobTask(
    cluster_specs=fleet_cluster.cluster_specs,
    name="SeenInBidding_V2_Group_Data_Daily_Agg_Step",
    class_name="jobs.agg_etl.DailySeenInBiddingV2DataAgg",
    timeout_timedelta=timedelta(hours=4),
    additional_args_option_pairs_list=sibv2_group_data_daily_agg_spark_options_list,
    eldorado_config_option_pairs_list=sibv2_group_data_daily_agg_options,
    executable_path=jar_path,
    retries=0,
)

sibv2_group_data_daily_agg_success = OpTask(
    op=PythonOperator(
        task_id="sib_person_group_success",
        python_callable=add_success_file,
        op_kwargs=dict(
            job_date_str=sibDateToProcess,
            bucket="ttd-identity",
            path=f"datapipeline/{env_path}/seeninbiddingpersongroup/v=2/xdvendorid=10"
        ),
        retries=3,
        provide_context=True,
        dag=dag_pipeline.airflow_dag,
    )
)

fleet_cluster.add_sequential_body_task(sibv2_group_data_daily_agg_step)

################################################
# SeenInBidding_V2_Group_Data_Seven_Day_Roll_Up
################################################

rollup_cluster = EmrClusterTask(
    name=sibv2_group_data_rollup_and_uniques_job_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(768).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
        ],
        on_demand_weighted_capacity=360
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

# Step roll up
sibv2_group_data_seven_day_rollup_options = [("rollUpToDate", sibDateToProcess)]

sibv2_group_data_seven_day_rollup_agg_vendorId_11_step = EmrJobTask(
    cluster_specs=rollup_cluster.cluster_specs,
    name="SeenInBiddingV2GroupDataSevenDayRollUpAggVendorId11_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2GroupDataSevenDayRollUpAgg",
    timeout_timedelta=timedelta(hours=3),
    eldorado_config_option_pairs_list=sibv2_group_data_seven_day_rollup_options +
    [("xdVendorId", CrossDeviceVendorIds.HOUSEHOLD_VENDOR_ID_IAV2)],
    executable_path=jar_path
)

sibv2_group_data_seven_day_rollup_agg_vendorId_10_step = EmrJobTask(
    cluster_specs=rollup_cluster.cluster_specs,
    name="SeenInBiddingV2GroupDataSevenDayRollUpAggVendorId10_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2GroupDataSevenDayRollUpAgg",
    timeout_timedelta=timedelta(hours=3),
    eldorado_config_option_pairs_list=sibv2_group_data_seven_day_rollup_options +
    [("xdVendorId", CrossDeviceVendorIds.PERSON_VENDOR_ID_IAV2)],
    executable_path=jar_path
)

rollup_cluster.add_sequential_body_task(sibv2_group_data_seven_day_rollup_agg_vendorId_11_step)
rollup_cluster.add_sequential_body_task(sibv2_group_data_seven_day_rollup_agg_vendorId_10_step)

legacy_graph_rollup_cluster = EmrClusterTask(
    name=legacy_graph_sibv2_group_data_rollup_and_uniques_job_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
        ],
        on_demand_weighted_capacity=360
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

# Legacy IAV2 Household graph loading
sibv2_group_data_seven_day_rollup_agg_vendorId_611_step = EmrJobTask(
    cluster_specs=legacy_graph_rollup_cluster.cluster_specs,
    name="SeenInBiddingV2GroupDataSevenDayRollUpAggVendorId611_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2GroupDataSevenDayRollUpAgg",
    timeout_timedelta=timedelta(hours=3),
    eldorado_config_option_pairs_list=sibv2_group_data_seven_day_rollup_options +
    [("xdVendorId", CrossDeviceVendorIds.HOUSEHOLD_VENDOR_ID_IAV2_LEGACY)],
    executable_path=jar_path
)

# Legacy IAV2 Person graph loading
sibv2_group_data_seven_day_rollup_agg_vendorId_610_step = EmrJobTask(
    cluster_specs=legacy_graph_rollup_cluster.cluster_specs,
    name="SeenInBiddingV2GroupDataSevenDayRollUpAggVendorId610_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2GroupDataSevenDayRollUpAgg",
    timeout_timedelta=timedelta(hours=3),
    eldorado_config_option_pairs_list=sibv2_group_data_seven_day_rollup_options +
    [("xdVendorId", CrossDeviceVendorIds.PERSON_VENDOR_ID_IAV2_LEGACY)],
    executable_path=jar_path
)

legacy_graph_rollup_cluster.add_sequential_body_task(sibv2_group_data_seven_day_rollup_agg_vendorId_611_step)
legacy_graph_rollup_cluster.add_sequential_body_task(sibv2_group_data_seven_day_rollup_agg_vendorId_610_step)
coldstorage_aerospike_host = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", port=4333, limit=1)}}'

####################################################################################################################
# SeenInBidding_V2_Device_Data_Seven_Day_Roll_Up and SeenInBiddingV2_Device_Uniques_And_OverlapsJob
####################################################################################################################

device_rollup_cluster = EmrClusterTask(
    name=sibv2_device_data_rollup_and_uniques_job_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
        ],
        on_demand_weighted_capacity=360
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

device_rollup_databricks_cluster = DatabricksWorkflow(
    job_name=sibv2_device_data_rollup_and_uniques_job_name + "_databricks",
    cluster_name="SeenInBiddingV2_Device_Data_Seven_Day_Roll_Up_Agg",
    cluster_tags={
        "Process": "Sibv2-Rollup",
        "Team": "ADPB"
    },
    databricks_spark_version=databricks_spark_version,
    worker_node_type="r6gd.8xlarge",
    worker_node_count=85,
    use_photon=True,
    eldorado_cluster_options_list=[
        ("rollUpToDate", sibDateToProcess),
    ],
    spark_configs={
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.speculation": "false",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.sql.shuffle.partitions": 20000,
        "spark.sql.files.ignoreCorruptFiles": "false",
        "fs.s3a.acl.default": "BucketOwnerFullControl",
    },
    region=DatabricksRegion.use(),
    tasks=[
        SparkDatabricksTask(
            class_name="jobs.agg_etl.SeenInBiddingV2DeviceDataSevenDayRollUpAgg",
            executable_path=jar_path,
            job_name=sibv2_device_data_rollup_and_uniques_job_name,
            openlineage_config=OpenlineageConfig(transport=OpenlineageTransport.ROBUST)
        )
    ],
    enable_elastic_disk=True,
)

sibv2_device_sevenday_rollup_agg_success = OpTask(
    op=PythonOperator(
        task_id="sib_device_sevenday_rollup_success",
        python_callable=add_success_file,
        op_kwargs=dict(
            job_date_str=sibDateToProcess, bucket="ttd-identity", path=f"datapipeline/{env_path}/seeninbiddingdevicesevendayrollup/v=2"
        ),
        retries=3,
        provide_context=True,
        dag=dag_pipeline.airflow_dag,
    )
)

# Step generate offline tracking tag expansion
sibv2_device_offline_tracker_expansion_options = [("processingDate", sibDateToProcess), ("generatePrometheusMetrics", 'true'),
                                                  ("conversionLookbackDays", 30), ('ttd.azure.enable', "false")]

sibv2_device_offline_tracker_expansion_step = EmrJobTask(
    cluster_specs=device_rollup_cluster.cluster_specs,
    name="OfflineTrackerExpansion_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2OfflineTrackerExpansionJob",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=sibv2_device_offline_tracker_expansion_options,
    executable_path=jar_path
)

# Step generate uniques and overlaps
sibv2_device_data_uniques_and_overlaps_options = [("rolledUpDate", sibDateToProcess), ("isOfflineTrackingTagExpansionIncluded", "true")]
sibv2_device_data_uniques_and_overlaps_step = EmrJobTask(
    cluster_specs=device_rollup_cluster.cluster_specs,
    name="SeenInBiddingV2_Device_Uniques_And_Overlaps_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2DeviceUniquesAndOverlapsJob",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=sibv2_device_data_uniques_and_overlaps_options,
    executable_path=jar_path
)

# Step generate device graph expansion
sibv2_device_1p_graph_expansion_options = [("processingDate", sibDateToProcess), ("generatePrometheusMetrics", 'true'),
                                           (
                                               "graphDate", "{{ task_instance.xcom_pull(dag_id='" + pipeline_name +
                                               "', task_ids='get-most-recent-xdgraph-date', key='" + xd_graph_date_key_iav2 + "') }}"
                                           ), ('ttd.azure.enable', "false")]

sibv2_device_graph_expansion_step = EmrJobTask(
    cluster_specs=device_rollup_cluster.cluster_specs,
    name="FirstPartyXDExpansionJob_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2FirstPartyXDExpansionJob",
    timeout_timedelta=timedelta(hours=3),
    eldorado_config_option_pairs_list=sibv2_device_1p_graph_expansion_options,
    executable_path=jar_path
)

# Step generate expanded uniques and overlaps
sibv2_device_expansion_data_uniques_and_overlaps_options = [("rolledUpDate", sibDateToProcess),
                                                            ("isOfflineTrackingTagExpansionIncluded", "true")]

sibv2_device_expansion_data_uniques_and_overlaps_step = EmrJobTask(
    cluster_specs=device_rollup_cluster.cluster_specs,
    name="SeenInBiddingV2_Device_Expansion_Uniques_And_Overlaps_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2DeviceExpansionUniquesAndOverlapsJob",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=sibv2_device_expansion_data_uniques_and_overlaps_options,
    executable_path=jar_path
)

device_rollup_cluster.add_sequential_body_task(sibv2_device_offline_tracker_expansion_step)
device_rollup_cluster.add_sequential_body_task(sibv2_device_data_uniques_and_overlaps_step)
device_rollup_cluster.add_sequential_body_task(sibv2_device_graph_expansion_step)
device_rollup_cluster.add_sequential_body_task(sibv2_device_expansion_data_uniques_and_overlaps_step)

####################################################################################################################
# SeenInBidding_V2_Device_BitMap_Job
####################################################################################################################

sibv2_device_bitmap_job_cluster = EmrClusterTask(
    name=sibv2_device_bitmap_job_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
        ],
        on_demand_weighted_capacity=360
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

# Step generate bitmap user index
sibv2_device_index_map_options = [("rolledUpDate", sibDateToProcess)]

sibv2_device_index_map_step = EmrJobTask(
    cluster_specs=sibv2_device_bitmap_job_cluster.cluster_specs,
    name="SeenInBiddingV2_Device_Index_Map_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2DeviceIndexMapGenerator",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=sibv2_device_index_map_options,
    executable_path=jar_path
)

sibv2_device_bitmap_job_cluster.add_sequential_body_task(sibv2_device_index_map_step)

# Step generate bitmaps
sibv2_device_bitmap_options = [("rolledUpDate", sibDateToProcess)]

sibv2_device_bitmap_step = EmrJobTask(
    cluster_specs=sibv2_device_bitmap_job_cluster.cluster_specs,
    name="SeenInBiddingV2_Device_Bitmap_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2DeviceBitmapGenerator",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=sibv2_device_bitmap_options,
    executable_path=jar_path
)

sibv2_device_bitmap_job_cluster.add_sequential_body_task(sibv2_device_bitmap_step)

# Step generate universe bitmaps
sibv2_device_universe_bitmap_options = [("rolledUpDate", sibDateToProcess)]

sibv2_device_universe_bitmap_step = EmrJobTask(
    cluster_specs=sibv2_device_bitmap_job_cluster.cluster_specs,
    name="SeenInBiddingV2_Device_Universe_Bitmap_Step",
    class_name="jobs.agg_etl.SeenInBiddingV2DeviceUniverseBitmapGenerator",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=sibv2_device_universe_bitmap_options,
    executable_path=jar_path
)

sibv2_device_bitmap_job_cluster.add_sequential_body_task(sibv2_device_universe_bitmap_step)

dag_pipeline >> check_sibv2_log_data >> fleet_cluster >> sibv2_group_data_daily_agg_success >> check_xd_graph

check_xd_graph >> check_sibv2_group_daily_data >> rollup_cluster

check_xd_graph >> check_sibv2_device_daily_data_for_rollup >> device_rollup_databricks_cluster >> device_rollup_cluster >> sibv2_device_sevenday_rollup_agg_success >> sibv2_device_bitmap_job_cluster

fleet_cluster >> check_sibv2_group_daily_data_legacy_graph >> legacy_graph_rollup_cluster
