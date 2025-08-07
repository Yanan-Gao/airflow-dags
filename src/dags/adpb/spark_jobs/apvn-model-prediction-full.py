from ttd.ec2.cluster_params import calc_cluster_params
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from dags.adpb.spark_jobs.shared.adpb_helper import open_lwdb_gate_daily
import copy

run_time = "{{ data_interval_end.strftime(\"%Y-%m-%d\") }}"
conversion_lookback_days = 7
bid_feedback_lookback_days = 14
recordsPerFile = 3000

job_start_date = datetime(2025, 4, 15, 9, 0, 0)
job_schedule_interval = timedelta(hours=24)
job_slack_channel = "#scrum-adpb-alerts"

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "150",
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
}]

cluster_tags = {
    'Team': ADPB.team.jira_team,
}

apvn_model_prediction_dag = TtdDag(
    dag_id="adpb-apvn-model-prediction-full",
    start_date=job_start_date,
    run_only_latest=False,
    schedule_interval=timedelta(hours=24),
    max_active_runs=1,
    slack_channel=job_slack_channel,
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True,
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/LDtnEQ"
)

dag = apvn_model_prediction_dag.airflow_dag


def generate_spark_option_list(instances_count, instance_type, gc="ParallelGC"):
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
                          ("conf", f"spark.executor.extraJavaOptions=-server -XX:+Use{gc}"),
                          ("conf", f"spark.driver.maxResultSize={cluster_params.executor_memory_with_unit}"),
                          ("conf", "spark.network.timeout=12000s"),
                          ("conf", f"yarn.nodemanager.resource.memory-mb={cluster_params.node_memory * 1000}"),
                          ("conf", f"yarn.nodemanager.resource.cpu-vcores={cluster_params.vcores}"),
                          ("conf", "spark.executor.heartbeatInterval=1000s"), ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                          ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]
    return spark_options_list


model_generation_spark_options_list = generate_spark_option_list(42, R7g.r7g_12xlarge())

model_generation_fleet_cluster = EmrClusterTask(
    name="APvN_Model_Generation",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_12xlarge().with_ebs_size_gb(600).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_16xlarge().with_ebs_size_gb(900).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=84
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    retries=0
)

bid_feedback_trimmer_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APvN_Bid_Feedback_Trimmer",
    class_name="model.apv3.BidFeedbackTrimmer",
    timeout_timedelta=timedelta(hours=1),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time)],
    executable_path=jar_path
)

training_user_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APv3_Training_Users_Generation",
    class_name="model.apv3.GenerateTrainingUsers",
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                       ("bidFeedbackLookbackDays", bid_feedback_lookback_days), ("onlyGenerateAPAdGroups", "false")],
    executable_path=jar_path
)

seed_training_user_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APv4_Seed_Training_Users_Generation",
    class_name="model.apv3.GenerateSeedTrainingUsers",
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                       ("bidFeedbackLookbackDays", bid_feedback_lookback_days), ("onlyGenerateAPAdGroups", "false")],
    executable_path=jar_path
)

click_tracker_training_user_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APV5_Click_Tracker_Training_Users_Generation",
    class_name="model.apv3.GenerateClickTrackerTrainingUsers",
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                       ("bidFeedbackLookbackDays", bid_feedback_lookback_days), ("onlyGenerateAPAdGroups", "false")],
    executable_path=jar_path
)

training_data_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APvN_Training_Data_Generation",
    class_name="model.apv3.GenerateTrainingData",
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                       ("bidFeedbackLookbackDays", bid_feedback_lookback_days)],
    executable_path=jar_path
)

model_training_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APvN_Model_Training",
    class_name="model.apv3.ModelTraining",
    timeout_timedelta=timedelta(hours=4),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                       ("bidFeedbackLookbackDays", bid_feedback_lookback_days)],
    executable_path=jar_path
)

model_aggregation_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APvN_Model_Aggregation",
    class_name="model.apv3.TransformedModelAggregation",
    timeout_timedelta=timedelta(hours=1),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time)],
    executable_path=jar_path
)

model_threshold_step = EmrJobTask(
    cluster_specs=model_generation_fleet_cluster.cluster_specs,
    name="APvN_Model_Threshold",
    class_name="model.apv3.ModelThreshold",
    timeout_timedelta=timedelta(hours=5),
    additional_args_option_pairs_list=copy.deepcopy(model_generation_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                       ("bidFeedbackLookbackDays", bid_feedback_lookback_days)],
    executable_path=jar_path
)

batchSize = 7


def generate_user_scoring_clusters(batchId):
    model_prediction_fleet_cluster = EmrClusterTask(
        name=f"APvN_Model_Prediction_{batchId + 1}_of_{batchSize}",
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        cluster_tags=cluster_tags,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R7g.r7g_12xlarge().with_ebs_size_gb(600).with_max_ondemand_price().with_fleet_weighted_capacity(2),
                R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
                R7g.r7g_16xlarge().with_ebs_size_gb(900).with_max_ondemand_price().with_fleet_weighted_capacity(3),
                R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3)
            ],
            on_demand_weighted_capacity=162
        ),
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        retries=0
    )

    model_prediction_spark_options_list = generate_spark_option_list(81, R7g.r7g_12xlarge(), "G1GC")

    user_scoring_step = EmrJobTask(
        cluster_specs=model_prediction_fleet_cluster.cluster_specs,
        name="APvN_User_Scoring",
        class_name="model.apv3.UserScoring",
        timeout_timedelta=timedelta(hours=9),
        additional_args_option_pairs_list=model_prediction_spark_options_list,
        eldorado_config_option_pairs_list=[("runTime", run_time), ("conversionLookbackDays", conversion_lookback_days),
                                           ("bidFeedbackLookbackDays", bid_feedback_lookback_days), ("batchSize", batchSize),
                                           ("batchId", batchId)],
        executable_path=jar_path
    )
    model_prediction_fleet_cluster.add_sequential_body_task(user_scoring_step)

    return model_prediction_fleet_cluster


user_scoring_clusters = [generate_user_scoring_clusters(batchId) for batchId in range(batchSize)]

model_prediction_start = OpTask(op=DummyOperator(task_id="model_prediction_start", trigger_rule=TriggerRule.ALL_SUCCESS), )
model_prediction_end = OpTask(op=DummyOperator(task_id="model_prediction_end", trigger_rule=TriggerRule.ALL_SUCCESS), )

export_cluster = EmrClusterTask(
    name="APvN_User_Export",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(400).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(800).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(1200).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7g.r7g_16xlarge().with_ebs_size_gb(1600).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=120
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    retries=0
)
export_spark_options_list = [("executor-memory", "92G"), ("executor-cores", "16"),
                             ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                             ("conf", "dfs.client.use.datanode.hostname=true"), ("conf", "spark.driver.memory=50G"),
                             ("conf", "spark.sql.shuffle.partitions=2700"), ("conf", "spark.driver.maxResultSize=50G"),
                             ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]
export_uid2_spark_options_list = [("executor-memory", "44G"), ("executor-cores", "16"),
                                  ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                  ("conf", "dfs.client.use.datanode.hostname=true"), ("conf", "spark.driver.memory=44G"),
                                  ("conf", "spark.sql.shuffle.partitions=2700"), ("conf", "spark.driver.maxResultSize=44G"),
                                  ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

user_export_step = EmrJobTask(
    cluster_specs=export_cluster.cluster_specs,
    name="APvN_User_Export",
    class_name="model.apv3.ExportResultToDataImport",
    timeout_timedelta=timedelta(hours=5),
    additional_args_option_pairs_list=copy.deepcopy(export_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("recordsPerFile", recordsPerFile),
                                       ("combinedDataImportBucket", "thetradedesk-useast-data-import")],
    executable_path=jar_path
)

adgroup_budget_metadata = EmrJobTask(
    cluster_specs=export_cluster.cluster_specs,
    name="AdGroup_Budget_Metadata",
    class_name="model.apv3.BudgetUniquesMetadata",
    timeout_timedelta=timedelta(hours=1),
    additional_args_option_pairs_list=copy.deepcopy(export_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time)],
    executable_path=jar_path
)

job_datetime_format: str = "%Y-%m-%dT%H:00:00"
gating_type_id = 2000337  # dbo.fn_Enum_GatingType_ImportAdGroupBudgetMetadata()


def _get_time_slot(dt: datetime):
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


budget_metadata_sql_import_open_gate = OpTask(
    op=PythonOperator(
        task_id='open_lwdb_gate',
        python_callable=open_lwdb_gate_daily,
        provide_context=True,
        op_kwargs=dict(
            gating_type=gating_type_id,
            job_date_str='{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}',
        ),
        dag=apvn_model_prediction_dag.airflow_dag,
    )
)

audience_budget_uniques = EmrJobTask(
    cluster_specs=export_cluster.cluster_specs,
    name="APV3_Audience_Budget_Uniques",
    class_name="model.apv3.BudgetUniquesCalculation",
    timeout_timedelta=timedelta(hours=1),
    additional_args_option_pairs_list=copy.deepcopy(export_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("targetAdGroupAPv3UsageRatio", 0.8)],
    executable_path=jar_path
)

non_apv3_relevance_threshold_calculation = EmrJobTask(
    cluster_specs=export_cluster.cluster_specs,
    name="Non_APV3_Relevance_Threshold_Calculation",
    class_name="model.apv3.NonApv3AdGroupsRelevanceThresholdCalculation",
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=copy.deepcopy(export_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("LookbackDays", 14), ("CampaignReportingPixelUniquesMinimum", 1),
                                       ("ThirdPartyDataSegmentUniquesMinimum", 1000), ("TargetingDataGroupRelevanceRatioThreshold", 0.5),
                                       ("lalPath", "models/lal/all_model_unfiltered_results/v=3/xdvendorid=10"),
                                       ("customSegmentTiersToCheck", "3,4,5,6,7,8,9,10"), ("defaultRelevanceScore", 1)],
    executable_path=jar_path
)

uid2_export_cluster = EmrClusterTask(
    name="APvN_User_Uid2_Export",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7g.r7g_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=45
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    retries=0
)

uid2_export_step = EmrJobTask(
    cluster_specs=uid2_export_cluster.cluster_specs,
    name="APvN_Uid2_Export",
    class_name="model.apv3.ExportUid2ToDataServer",
    timeout_timedelta=timedelta(hours=6),
    additional_args_option_pairs_list=copy.deepcopy(export_uid2_spark_options_list),
    eldorado_config_option_pairs_list=[("runTime", run_time), ("numParallelDataServerRequests", 700), ("numRecordsPerRequest", 250)],
    executable_path=jar_path
)

model_generation_fleet_cluster.add_sequential_body_task(bid_feedback_trimmer_step)
model_generation_fleet_cluster.add_sequential_body_task(training_user_step)
model_generation_fleet_cluster.add_sequential_body_task(seed_training_user_step)
model_generation_fleet_cluster.add_sequential_body_task(click_tracker_training_user_step)
model_generation_fleet_cluster.add_sequential_body_task(training_data_step)
model_generation_fleet_cluster.add_sequential_body_task(model_training_step)
model_generation_fleet_cluster.add_sequential_body_task(model_aggregation_step)
model_generation_fleet_cluster.add_sequential_body_task(model_threshold_step)

export_cluster.add_sequential_body_task(user_export_step)
export_cluster.add_sequential_body_task(non_apv3_relevance_threshold_calculation)
export_cluster.add_sequential_body_task(adgroup_budget_metadata)
export_cluster.add_sequential_body_task(audience_budget_uniques)

uid2_export_cluster.add_sequential_body_task(uid2_export_step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=apvn_model_prediction_dag.airflow_dag))

for user_scoring_cluster in user_scoring_clusters:
    model_prediction_start >> user_scoring_cluster >> model_prediction_end

apvn_model_prediction_dag >> model_generation_fleet_cluster >> model_prediction_start >> model_prediction_end >> export_cluster >> budget_metadata_sql_import_open_gate >> check
user_export_step >> uid2_export_cluster >> check
