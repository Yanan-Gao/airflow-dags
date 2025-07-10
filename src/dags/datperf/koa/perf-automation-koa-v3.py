import copy
import logging
from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator

from dags.datperf.datasets import platformreport_dataset, campaignspendtargets_dataset, verticakoabudget_dataset
from dags.datperf.utils.spark_config_utils import get_spark_args
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r6a import R6a
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import LogFileBatchProcessCallable
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

# execution_date_Ymd = execution_date_format_fn('%Y%m%d')

execution_date_Y_m_d = "{{ (data_interval_start + macros.timedelta(days=1)).strftime(\"%Y-%m-%d\") }}"
execution_start_Y_m_d = "{{ data_interval_start.strftime(\"%Y%m%d\") }}"
execution_date_Ymd = "{{ (data_interval_start + macros.timedelta(days=1)).strftime(\"%Y%m%d\") }}"

execution_date_H = "{{ data_interval_start.strftime(\"%H\") }}"
execution_datehour = "{{ (data_interval_start + macros.timedelta(days=1)).strftime(\"%Y-%m-%dT%H:00:00\") }}"
execution_date = "{{ (data_interval_start + macros.timedelta(days=1)).strftime(\"%Y-%m-%dT00:00:00\") }}"
execution_start_dt = "{{ data_interval_start.to_datetime_string() }}"
# check for hour 23 to give us the highest chance we are operating on complete data
bidding_data_dt = "{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "30",
        "fs.s3.sleepTimeSeconds": "15"
    }
}, {
    'Classification': 'hdfs-site',
    'Properties': {
        'dfs.replication': '2'
    }
}]

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

cluster_tags = {
    'Team': DATPERF.team.jira_team,
}

executionIntervalInDays = 1

# set to your custom jar file if testing.
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar"

# Define the DAG using TtdDag
koav3_dag = TtdDag(
    dag_id="perf-automation-koa-v3",
    start_date=datetime(2025, 1, 22, 2, 0),
    schedule_interval=timedelta(days=executionIntervalInDays),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/SCJKBw',
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = koav3_dag.airflow_dag


###############################################################################
# Helper
###############################################################################
def copy_file(source_key, source_bucket, dest_key, dest_bucket):
    try:
        s3 = AwsCloudStorage(conn_id='aws_default')
        logging.info(f"Attempting to copy file from s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")
        s3.copy_file(source_key, source_bucket, dest_key, dest_bucket)
        logging.info("File copy completed successfully")
    except Exception as e:
        logging.error(f"Failed to copy file: {str(e)}")
        raise


def get_success_file_object(**kwargs):
    s3 = AwsCloudStorage(conn_id='aws_default')
    # Match the date path with S3 copy previous steps
    execution_date_ymd_path = kwargs['datetime']
    date_str = datetime.strptime(execution_date_ymd_path, '%Y%m%d').strftime('%Y-%m-%dT00:00:00')
    bucket_name = kwargs['bucket_name']
    log_type_id = kwargs['log_type_id']
    # Success signal key which will write along with log file record
    success_signal_key = f'date={execution_date_ymd_path}/log_type_id={log_type_id}/auto_opt_upstream_success'
    now_utc = datetime.now(timezone.utc)
    logfiletask_endtime = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
    # Construct key used in object list
    for s3_prefix in kwargs['s3_prefixes']:
        full_path_prefix = f'{s3_prefix}date={execution_date_ymd_path}/_SUCCESS'
        retrieved_keys = s3.list_keys(prefix=full_path_prefix, bucket_name=bucket_name)
        logging.info(f'retrieved key is: {retrieved_keys}')
        if retrieved_keys is None or len(retrieved_keys) == 0:
            raise Exception(f'Expected non-zero number of files for VerticaLoad log type {log_type_id} at s3 path {full_path_prefix}')

    # If no exception thrown, append success key in LogFile table
    object_list = [(log_type_id, success_signal_key, date_str, 1, 0, 1, 1, 0, logfiletask_endtime)
                   ]  # CloudServiceId 1 == AWS，DataDomain 1 == TTD_RestOfWorld
    logging.info(f'Added {success_signal_key} for log type {log_type_id} in the log file task')
    return object_list


###############################################################################
# S3 Data Sensors
###############################################################################
budget_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='budget_data_available',
    datasets=[verticakoabudget_dataset],
    # looks for success file for this hour
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

platform_report_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='platform_report_data_available',
    datasets=[platformreport_dataset.with_check_type("day")],
    # looks for success file for this hour
    ds_date=bidding_data_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

bidrequest_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='bidrequest_data_available',
    datasets=[RtbDatalakeDatasource.rtb_bidrequest_v5],
    # looks for success file for this hour
    ds_date=bidding_data_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

bidfeedback_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='bidfeedback_data_available',
    datasets=[RtbDatalakeDatasource.rtb_bidfeedback_v5],
    # looks for success file for this hour
    ds_date=bidding_data_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

campaignspendtargets_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='campaignspendtargets_data_available',
    datasets=[campaignspendtargets_dataset],
    # looks for success file for this hour
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

###############################################################################
# Shared cluster config
###############################################################################

shared_fleet_master_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)


def get_cluster_config(capacity):
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R6a.r6a_16xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R6a.r6a_32xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(2)
        ],
        on_demand_weighted_capacity=capacity
    )

    cluster_params = R6a.r6a_16xlarge().calc_cluster_params(
        instances=capacity, min_executor_memory=202, max_cores_executor=32, memory_tolerance=0.95
    )

    spark_args = get_spark_args(cluster_params)

    return core_fleet_instance_type_configs, spark_args


###############################################################################
# KPI Jobs
###############################################################################

kpi_job_options = [('ttd.kpicalculation.datetime', execution_date), ('ttd.kpicalculation.rtbreportlookback', '14'),
                   ('ttd.kpicalculation.rtbbidfeedbacklookback', '4')]

kpi_ar_job_options = [('usegracenote', 'true'), ('campaignsToTestLtvWithRate', '1')]

kpi_cluster_config, kpi_spark_args = get_cluster_config(60)

kpi_cluster_additional_reach = EmrClusterTask(
    name="KPI_AR",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=kpi_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

kpi_step_ar = EmrJobTask(
    name="KPI_AdditionalReach",
    class_name="com.thetradedesk.jobs.koav3.kpiprediction.KpiPredictionForAdditionalReachModel",
    additional_args_option_pairs_list=copy.deepcopy(kpi_spark_args),
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + kpi_job_options + kpi_ar_job_options,
    timeout_timedelta=timedelta(hours=5),
    executable_path=jar_path,
)

kpi_cluster_additional_reach.add_sequential_body_task(kpi_step_ar)

kpi_cluster_other = EmrClusterTask(
    name="KPI_OTHER",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=kpi_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

kpi_step_other = EmrJobTask(
    name="KPI_OTHER",
    class_name="com.thetradedesk.jobs.koav3.kpiprediction.KpiPredictionForOtherGoalsModel",
    additional_args_option_pairs_list=copy.deepcopy(kpi_spark_args),
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + kpi_job_options,
    timeout_timedelta=timedelta(hours=3),
    executable_path=jar_path,
)

kpi_cluster_other.add_sequential_body_task(kpi_step_other)

kpi_cluster_cpa = EmrClusterTask(
    name="KPI_CPA",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=kpi_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

kpi_step_cpa = EmrJobTask(
    name="KPI_CPA",
    class_name="com.thetradedesk.jobs.koav3.kpiprediction.KpiPredictionForCpaModel",
    additional_args_option_pairs_list=copy.deepcopy(kpi_spark_args),
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + kpi_job_options,
    timeout_timedelta=timedelta(hours=5),
    executable_path=jar_path,
)

kpi_cluster_cpa.add_sequential_body_task(kpi_step_cpa)

kpi_cluster_union = EmrClusterTask(
    name="KPI_Union",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=kpi_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

kpi_step_union = EmrJobTask(
    name="KPI_Union",
    class_name="com.thetradedesk.jobs.koav3.kpiprediction.UnionKpiPredictionResultsJob",
    additional_args_option_pairs_list=copy.deepcopy(kpi_spark_args),
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + kpi_job_options,
    timeout_timedelta=timedelta(hours=5),
    executable_path=jar_path,
)

kpi_cluster_union.add_sequential_body_task(kpi_step_union)

###############################################################################
# Platform report aggregation
###############################################################################

pr_cluster_config, pr_spark_args = get_cluster_config(90)

platform_report_agg_cluster = EmrClusterTask(
    name="PlatformReportAgg",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=pr_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

platform_report_agg_step = EmrJobTask(
    name="KPI_PlatformReportAgg",
    class_name="com.thetradedesk.jobs.koav3.kpiprediction.KpiPredictionPlatformAggregationJob",
    additional_args_option_pairs_list=copy.deepcopy(pr_spark_args),
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + kpi_job_options,
    timeout_timedelta=timedelta(hours=5),
    executable_path=jar_path,
)

platform_report_agg_cluster.add_sequential_body_task(platform_report_agg_step)

###############################################################################
# Potential Bids
###############################################################################

pb_spark_options_list = [("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25")]

pb_job_options = [('endDateTime', execution_date), ('lookbackWindowHours', '24'), ('maxSplit', '1073741824'),
                  ('useBaseBidAdjustments', 'true')]

pb_cluster_config, pb_spark_args = get_cluster_config(90)

potential_bids_cluster = EmrClusterTask(
    name="PotentialBids",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=pb_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

android_ios_map_step = EmrJobTask(
    name="AndroidIosAppMapper",
    class_name="com.thetradedesk.jobs.koav3.potentialbids.AndroidIosAppMapper",
    additional_args_option_pairs_list=copy.deepcopy(pb_spark_args) + pb_spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + [('date', execution_date_Y_m_d)],
    timeout_timedelta=timedelta(hours=2),
    executable_path=jar_path,
)

potential_bids_cluster.add_sequential_body_task(android_ios_map_step)

potential_bids_hash_step = EmrJobTask(
    name="HashJob",
    class_name="com.thetradedesk.jobs.koav3.potentialbids.GrainHashMapJob",
    additional_args_option_pairs_list=copy.deepcopy(pb_spark_args) + pb_spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + pb_job_options,
    timeout_timedelta=timedelta(hours=3),
    executable_path=jar_path,
)

potential_bids_cluster.add_sequential_body_task(potential_bids_hash_step)

potential_bids_count_step = EmrJobTask(
    name="PotentialBidCountsJob",
    class_name="com.thetradedesk.jobs.koav3.potentialbids.ImpressionProfileBidCountsJob",
    additional_args_option_pairs_list=copy.deepcopy(pb_spark_args) + pb_spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + pb_job_options,
    timeout_timedelta=timedelta(hours=3),
    executable_path=jar_path,
)

potential_bids_cluster.add_sequential_body_task(potential_bids_count_step)

###############################################################################
# FPM
###############################################################################

fpm_spark_options_list = [
    ("conf", "spark.sql.shuffle.partitions=6000"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.memory.storageFraction=0.25"),
    # FPM is having issues with heartbeats from workers. Workers are in fact doing work, but hearbeats are getting
    # lost and the driver thinks the workers are hung and kills them. Increasing the network timeout increases the chance
    # that one of the heartbeats (they are sent every 10 seconds) will be received by the driver and thus the worker won't
    # get killed. I don't love it, because I don't know what the root cause of the issue is. But it works.
    ("conf", "spark.network.timeout=2400s"),
    ("conf", "spark.executor.heartbeatInterval=120000")  # 2 minutes
]

fpm_job_options = [('date', execution_date_Y_m_d), ('ttd.fip.numPartitions', '6500'), ('ttd.fip.maxNumItems', '4'),
                   ('ttd.fip.maxNumItemsetTypes', '10'), ('ttd.fip.maxNumImpressionProfileLines', '1000'),
                   ('ttd.fip.targetSpendCoverageRateBasedAdGroups', '3.0'), ('ttd.fip.targetSpendCoverageCostBasedAdGroups', '4.0'),
                   ('ttd.fip.minSupport', '0.05'), ('ttd.fip.itemsetMinSupport', '0.01'), ('ttd.fip.storeintermdatasets', 'true'),
                   ('ttd.fip.adGroupsToTestAkhWith', ''), ('ttd.fip.adGroupsToTestAkhWithRate', '0.05')]

fpm_cluster_config, fpm_spark_args = get_cluster_config(120)

fpm_cluster = EmrClusterTask(
    name="FPM",
    master_fleet_instance_type_configs=shared_fleet_master_configs,
    core_fleet_instance_type_configs=fpm_cluster_config,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

fpm_step = EmrJobTask(
    name="FPM",
    class_name="com.thetradedesk.jobs.koav3.fpm.FrequentImpressionProfileModel",
    additional_args_option_pairs_list=copy.deepcopy(fpm_spark_args) + fpm_spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(java_settings_list) + fpm_job_options,
    timeout_timedelta=timedelta(hours=8),
    executable_path=jar_path,
)

fpm_cluster.add_sequential_body_task(fpm_step)

###############################################################################
# WINRATE + Optimization loop
###############################################################################
emr_subnet = EmrSubnets.Public.useast_emr_1d()

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_24xlarge().with_ebs_size_gb(2048).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=9,
)

wropt_cluster = EmrClusterTask(
    name="WinRateOptimizationModels",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_tags={"Team": DATPERF.team.jira_team},
    ec2_subnet_ids=[emr_subnet],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

wr_spark_options_list = [("executor-memory", "32G"), ("executor-cores", "4"),
                         ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.maxResultSize=20G"),
                         ("conf", "spark.driver.memory=100G"), ("conf", "spark.sql.shuffle.partitions=6000"),
                         ("conf", "spark.sql.mapKeyDedupPolicy=LAST_WIN")]

wr_job_options = [('endDate', execution_date_Y_m_d)]

wr_step = EmrJobTask(
    name="WinRateOptimizationModels",
    class_name="com.thetradedesk.jobs.koav3.winrate.PlatformWinRateModel",
    additional_args_option_pairs_list=wr_spark_options_list,
    eldorado_config_option_pairs_list=wr_job_options,
    timeout_timedelta=timedelta(hours=8),
    executable_path=jar_path
)

wropt_cluster.add_sequential_body_task(wr_step)

optloop_spark_options_list = [
    ("executor-memory", "32G"),
    ("executor-cores", "4"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.maxResultSize=50G"),
    ("conf", "spark.driver.memory=100G"),
    ("conf", "spark.sql.shuffle.partitions=6000"),
]

optloop_job_options = [
    ('date', execution_date_Y_m_d),
    ("numPartitions", "480"),
    ("minBidFactor", "0.05"),
    ("maxBidFactorStarving", "10.0"),
    ("maxBidFactorNonStarving", "4.0"),
    ("extendToIos", "true"),
    ("isBaseBidOptEnabled", "true"),
    ("includeNoOptimizationReasons", "true"),
    ("adGroupsToTestAkhWithRate", "0.05"),
    ("minBaseBidMultiplier", "0.25"),
]

optloop_step = EmrJobTask(
    name="autooptPlatformOptimizationModel",
    class_name="com.thetradedesk.jobs.koav3.optimizationloop.PlatformOptimizationModel",
    additional_args_option_pairs_list=optloop_spark_options_list,
    eldorado_config_option_pairs_list=optloop_job_options,
    timeout_timedelta=timedelta(hours=8),
    executable_path=jar_path
)

wropt_cluster.add_sequential_body_task(optloop_step)

###############################################################################
# COPY SUCCESS FILE - add copy step to copy success file into partition folder
###############################################################################
env = TtdEnvFactory.get_from_system().execution_env
# 1. OptLoopMetrics
bucket = 'ttd-identity'
optloopmetrics_source_key = f'datapipeline/{env}/models/autoopt/debug/pg/optloopmetrics/v=2/_SUCCESS'
optloopmetrics_dest_key = f'datapipeline/{env}/models/autoopt/debug/pg/optloopmetrics/v=2/date=' + execution_start_Y_m_d + '/_SUCCESS'
copy_optloopmetrics_step = OpTask(
    op=PythonOperator(
        task_id='copy_optloopmetrics_success_file',
        python_callable=copy_file,
        dag=dag,
        op_kwargs={
            'source_key': optloopmetrics_source_key,
            'source_bucket': bucket,
            'dest_key': optloopmetrics_dest_key,
            'dest_bucket': bucket
        }
    )
)

# 2. AdgroupPerfImprovement
bucket = 'ttd-identity'
agperfimprovement_source_key = f'datapipeline/{env}/models/autoopt/pg/adgroupperformanceimprovement/v=2/_SUCCESS'
agperfimprovement_dest_key = f'datapipeline/{env}/models/autoopt/pg/adgroupperformanceimprovement/v=2/date=' + execution_start_Y_m_d + '/_SUCCESS'
copy_agperfimprovement_step = OpTask(
    op=PythonOperator(
        task_id='copy_agperfimprovement_success_file',
        python_callable=copy_file,
        dag=dag,
        op_kwargs={
            'source_key': agperfimprovement_source_key,
            'source_bucket': bucket,
            'dest_key': agperfimprovement_dest_key,
            'dest_bucket': bucket
        }
    )
)

###############################################################################
# DATAMOVER JOB
###############################################################################
log_workflow_connection_id = "lwdb"
log_workflow_db = 'LogWorkflow'
# Log type id for dbo.fn_Enum_LogType_ImportAutoOptPG() defined in Log Workflow service
log_type_id = 209
bucket_name = 'ttd-identity'

add_log_file_task = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=LogFileBatchProcessCallable,
        provide_context=True,
        op_kwargs={
            'database':
            log_workflow_db,
            'mssql_conn_id':
            log_workflow_connection_id,
            'get_object_list':
            get_success_file_object,
            'datetime':
            execution_start_Y_m_d,
            's3_prefixes': [
                'datapipeline/prod/models/autoopt/pg/adgroupperformanceimprovement/v=2/',
                'datapipeline/prod/models/autoopt/debug/pg/optloopmetrics/v=2/'
            ],
            'bucket_name':
            bucket_name,
            'log_type_id':
            log_type_id,
        },
        task_id="add_log_file_task"
    )
)

###############################################################################
# Dependencies
###############################################################################

# Final status check to ensure that all tasks have completed successfully
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# clusters

koav3_dag >> kpi_cluster_additional_reach
koav3_dag >> kpi_cluster_other
koav3_dag >> kpi_cluster_cpa
koav3_dag >> kpi_cluster_union
koav3_dag >> platform_report_agg_cluster
koav3_dag >> potential_bids_cluster
koav3_dag >> fpm_cluster
koav3_dag >> wropt_cluster

# data dependencies
[budget_sensor, bidrequest_sensor, bidfeedback_sensor] >> potential_bids_cluster.first_airflow_op()
platform_report_sensor >> platform_report_agg_cluster.first_airflow_op()

# platform aggregation after hashes
potential_bids_hash_step >> platform_report_agg_cluster

# some kpi steps after report agg, but all also depend on bid counts
platform_report_agg_step >> kpi_cluster_cpa
potential_bids_count_step >> kpi_cluster_cpa
platform_report_agg_step >> kpi_cluster_other
potential_bids_count_step >> kpi_cluster_other
potential_bids_count_step >> kpi_cluster_additional_reach

# Don't start the union step until we have all the data from the previous three steps running simultaneously.
kpi_step_cpa >> kpi_cluster_union
kpi_step_ar >> kpi_cluster_union
kpi_step_other >> kpi_cluster_union

# fpm after kpi
kpi_step_union >> fpm_cluster

# step dependencies, make sure there is data before cluster creation
campaignspendtargets_sensor >> wropt_cluster.first_airflow_op()

# wr after fpm
fpm_step >> wropt_cluster

# add copy steps after optloop_step
optloop_step >> copy_optloopmetrics_step
optloop_step >> copy_agperfimprovement_step

# add check previous copy steps and log a record in data mover to trigger vertica tasks
copy_optloopmetrics_step >> add_log_file_task
copy_agperfimprovement_step >> add_log_file_task

# this should be the last cluster
add_log_file_task >> final_dag_check
