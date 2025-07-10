import copy
from datetime import datetime, timedelta
from enum import Enum

from dags.adpb.spark_jobs.shared.adpb_helper import LalAerospikeConstant
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack import slack_groups
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from dags.adpb.datasets.datasets import get_rsm_id_relevance_score, get_rsm_seed_ids, get_rsm_seed_population_scores
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

###########################################
#   job settings
###########################################

job_name = "adpb-rsm-lal"
job_schedule_interval = timedelta(days=1)
job_ec2_subnet_id = ["subnet-0e82171b285634d41"]
el_dorado_jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
spark_3_emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3  # Ensure spark3 and the correct scala version is used for the spark steps
job_start_date = datetime(2025, 3, 12, 17, 0)

target_date = "{{ ds }}"
target_date_no_dash = "{{ ds_nodash }}"

numContainers = 71
numCoresPerContainer = 95
numContainersPlusDriver = numContainers + 1
leastNumCoresPerNode = numCoresPerContainer + 1
numFleetCoresNeeded = numContainersPlusDriver * leastNumCoresPerNode
executorAllocatedMemory = "500G"
driverAllocatedMemory = "600G"

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

cluster_tags = {
    "Team": slack_groups.ADPB.team.jira_team,
    "Creator": "duy.nguyen",
}

# sib date
sib_lookback_days = 5
sib_date_key = "sib_date_key"
check_sib_date_task_id = "get-most-recent-sib-date"

thirdPartyBatchSize = 3
seedBatchSize = 1
idCap = 100000

# RSM Id Path
rsmInputEnv = "prod"
rsmInputPath = f"s3://thetradedesk-mlplatform-us-east-1/data/{rsmInputEnv}/audience/scores/tdid2seedid/v=1/date="
rsmSeedIdPath = f"s3://thetradedesk-mlplatform-us-east-1/data/{rsmInputEnv}/audience/scores/seedids/v=2/date="
seedPopulationScoreInputPath = f"s3://thetradedesk-mlplatform-us-east-1/data/{rsmInputEnv}/audience/scores/seedpopulationscore/v=1/date="

# Segment Path
raw_scores_3p_path = f"models/rsm_lal/all_model_unfiltered_results/v=1/idCap={idCap}/"
raw_scores_seed_path = f"models/rsm_lal/seed_all_model_unfiltered_results/v=1/idCap={idCap}/"
smooth_scores_3p_path = f"models/rsm_lal/all_model_unfiltered_smoothed_results/v=1/idCap={idCap}/"
smooth_scores_seed_path = f"models/rsm_lal/seed_all_model_unfiltered_smoothed_results/v=1/idCap={idCap}/"
boost_scores_3p_path = f"models/rsm_lal/third_party_seed_boosted_segment/v=1/idCap={idCap}/"
overridden_scores_3p_path = f"models/rsm_lal/all_model_unfiltered_overridden_results/v=1/idCap={idCap}/"

# Publish to aerospike config
push_rsm_score_step_name = "push-rsm-score-to-aerospike"
rsm_batch_name = "rsm"
minimum_sibv2_uniques_users = 6100
model_results_path_prefix = "s3://ttd-identity/datapipeline/prod/models/lal/daily_model_results_for_aerospike/v=1/date="
max_map_size = 250000


class InputFilterType(Enum):
    UnivAndNonUniv = 0
    Univ = 1
    NonUniv = 2
    Seed = 4


###########################################
#   DAG setup
###########################################
rsm_lal_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=1,
    max_active_runs=3,
    retry_delay=timedelta(minutes=2),
    slack_tags=ADPB.team.sub_team,
    slack_channel="#scrum-adpb-alerts",
    run_only_latest=False
)

dag = rsm_lal_dag.airflow_dag

rsm_seed_selection_and_permission_cluster = EmrClusterTask(
    name="RSMLal-SeedSelectionAndPermissionCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
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
    cluster_tags=cluster_tags,
    enable_prometheus_monitoring=True,
    emr_release_label=spark_3_emr_release_label,
    additional_application_configurations=application_configuration,
    environment=TtdEnvFactory.prod
)

seed_selection_and_permission_step = EmrJobTask(
    cluster_specs=rsm_seed_selection_and_permission_cluster.cluster_specs,
    name="RSMLal-SeedSelectionAndPermission",
    class_name="jobs.lal.rsm.SeedSelectionAndPermissionComputer",
    executable_path=el_dorado_jar_path,
    eldorado_config_option_pairs_list=[
        ("date", target_date),
    ],
    timeout_timedelta=timedelta(hours=1)
)

id_coverage_monitor_step = EmrJobTask(
    cluster_specs=rsm_seed_selection_and_permission_cluster.cluster_specs,
    name="RSMLal-IdUniverseCoverageMonitorJob",
    class_name="jobs.lal.rsm.IdUniverseCoverageMonitorJob",
    executable_path=el_dorado_jar_path,
    eldorado_config_option_pairs_list=[("targetDate", target_date), ("sibDate", target_date),
                                       ("rsmInputPath", f"{rsmInputPath}{target_date_no_dash}")],
    timeout_timedelta=timedelta(hours=1)
)

rsm_seed_selection_and_permission_cluster.add_sequential_body_task(seed_selection_and_permission_step)
rsm_seed_selection_and_permission_cluster.add_sequential_body_task(id_coverage_monitor_step)

lal_spark_options_list = [("executor-memory", executorAllocatedMemory), ("executor-cores", str(numCoresPerContainer)),
                          ("num-executors", str(numContainers)), ("conf", "spark.driver.memory=" + driverAllocatedMemory),
                          ("conf", "spark.driver.cores=" + str(numCoresPerContainer)), ("conf", "spark.dynamicAllocation.enabled=false"),
                          ("conf", "spark.memory.offHeap.size=200G"), ("conf", "spark.memory.offHeap.enabled=true"),
                          ("conf", "spark.executor.memoryOverhead=59G"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                          ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]


def generate_targeting_data_count_subdag(thirdPartyBatchSize, seedBatchSize):

    def generate_count_clusters(batchId, isThirdParty):
        if isThirdParty:
            steps_per_cluster = 2
            name_prefix = "ThirdParty"
            inputFilterType = InputFilterType.UnivAndNonUniv.value
            outputPath = raw_scores_3p_path
            batchSize = thirdPartyBatchSize
            instance_types = [
                R5.r5_24xlarge().with_ebs_size_gb(6144).with_max_ondemand_price().with_fleet_weighted_capacity(96),
                R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
            ]
        else:
            steps_per_cluster = 1
            name_prefix = "Seed"
            inputFilterType = InputFilterType.Seed.value
            outputPath = raw_scores_seed_path
            batchSize = seedBatchSize
            instance_types = [
                R5.r5_24xlarge().with_ebs_size_gb(6144).with_max_ondemand_price().with_fleet_weighted_capacity(96),
                R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
            ]
        lal_cluster = EmrClusterTask(
            name=f"LAL_{name_prefix}_{batchId + 1}_of_{batchSize}",
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
            ),
            cluster_tags=cluster_tags,
            core_fleet_instance_type_configs=
            EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=numFleetCoresNeeded),
            additional_application_configurations=copy.deepcopy(application_configuration),
            enable_prometheus_monitoring=True,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
            environment=TtdEnvFactory.prod
        )

        for i in range(steps_per_cluster):
            local_batch_size = batchSize * steps_per_cluster
            local_batch_id = i * batchSize + batchId
            lal_count_step_options = [("targetDate", target_date), ("availCoresPerNode", str(numCoresPerContainer)),
                                      ("numNodes", str(numContainers)), ("numberOfBatches", local_batch_size), ("batchId", local_batch_id),
                                      ("inputFilterType", inputFilterType), ("sibDate", target_date), ("outputPath", outputPath),
                                      ("rsmInputPath", f"{rsmInputPath}{target_date_no_dash}"),
                                      ("rsmSeedIdPath", f"{rsmSeedIdPath}{target_date_no_dash}"), ("idCap", idCap)]

            lal_counts_step = EmrJobTask(
                name=f"LAL_{name_prefix}_segment_{local_batch_id + 1}_of_{local_batch_size}",
                class_name="jobs.lal.rsm.TargetingDataRelevanceComputerJob",
                executable_path=el_dorado_jar_path,
                additional_args_option_pairs_list=lal_spark_options_list,
                eldorado_config_option_pairs_list=lal_count_step_options,
                timeout_timedelta=timedelta(hours=3),
                cluster_specs=lal_cluster.cluster_specs,
            )
            lal_cluster.add_sequential_body_task(lal_counts_step)
        return lal_cluster

    lal_clusters = [generate_count_clusters(batchId, False) for batchId in range(seedBatchSize)]
    for batchId in range(thirdPartyBatchSize):
        lal_clusters.append(generate_count_clusters(batchId, True))

    return lal_clusters


lal_clusters = generate_targeting_data_count_subdag(thirdPartyBatchSize, seedBatchSize)
###########################################
# Smoothing
###########################################
user_lal_ratio_cluster = EmrClusterTask(
    name="SmoothRatioCluster",
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
        on_demand_weighted_capacity=1024
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    environment=TtdEnvFactory.prod,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

lal_relevance_ratio_spark_options_list = [("executor-memory", "35G"), ("conf", "spark.executor.cores=5"),
                                          ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=191"),
                                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                          ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"),
                                          ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                          ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                          ("conf", "spark.sql.shuffle.partitions=6000"),
                                          ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

lal_relevance_ratio_step_options = [("targetDate", target_date),
                                    ("seedPopulationScoreInputPath", f"{seedPopulationScoreInputPath}{target_date_no_dash}/"),
                                    ("smootheningMaxSeedSize", 12500), ("relevanceSmootheningSeedSizeSuppressingFactor", 800)]

lal_relevance_ratio_step = EmrJobTask(
    name="ThirdPartyData-Smooth",
    class_name="jobs.lal.rsm.TargetingDataRelevanceSmoothingJob",
    executable_path=el_dorado_jar_path,
    additional_args_option_pairs_list=lal_relevance_ratio_spark_options_list,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=lal_relevance_ratio_step_options + [("inputPath", raw_scores_3p_path),
                                                                          ("outputPath", smooth_scores_3p_path)],
    timeout_timedelta=timedelta(hours=3),
    cluster_specs=user_lal_ratio_cluster.cluster_specs,
)

lal_boost_step = EmrJobTask(
    name="ThirdPartyData-Boost",
    class_name="jobs.lal.rsm.ThirdPartySeedTargetingDataRelevanceBoostingJob",
    executable_path=el_dorado_jar_path,
    additional_args_option_pairs_list=lal_relevance_ratio_spark_options_list,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=lal_relevance_ratio_step_options + [("outputPath", boost_scores_3p_path),
                                                                          ("inputPath", smooth_scores_3p_path),
                                                                          ("outputOverriddenResultPath", overridden_scores_3p_path)],
    timeout_timedelta=timedelta(hours=3),
    cluster_specs=user_lal_ratio_cluster.cluster_specs,
)

seed_lal_relevance_ratio_step = EmrJobTask(
    name="Seed-Smooth",
    class_name="jobs.lal.rsm.TargetingDataRelevanceSmoothingJob",
    executable_path=el_dorado_jar_path,
    additional_args_option_pairs_list=lal_relevance_ratio_spark_options_list,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=lal_relevance_ratio_step_options + [("inputPath", raw_scores_seed_path),
                                                                          ("outputPath", smooth_scores_seed_path)],
    timeout_timedelta=timedelta(hours=3),
    cluster_specs=user_lal_ratio_cluster.cluster_specs,
)

user_lal_ratio_cluster.add_sequential_body_task(lal_relevance_ratio_step)
user_lal_ratio_cluster.add_sequential_body_task(lal_boost_step)
user_lal_ratio_cluster.add_sequential_body_task(seed_lal_relevance_ratio_step)

# check if rsm id relevance score ready, skip otherwise
check_rsm_id_relevance_score_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[get_rsm_id_relevance_score(split, env=rsmInputEnv) for split in range(0, 10)] + [get_rsm_seed_ids(env=rsmInputEnv)],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
        task_id='check_rsm_id_relevance_score',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 8,  # wait up to 8 hours. So it won't time out before T +1 day +1 hour
    )
)

# check if rsm seed population ready, skip otherwise
check_rsm_seed_population_relevance_score_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[get_rsm_seed_population_scores(env=rsmInputEnv)],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
        task_id='check_rsm_population_relevance_score',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 5,  # wait up to 5 hours
    )
)
##############################################################################
# Config for DailyRefreshLalModelsInAerospike
##############################################################################

spark_config_options = [("executor-memory", "35G"), ("conf", "spark.executor.cores=5"), ("conf", "spark.dynamicAllocation.enabled=false"),
                        ("conf", "spark.executor.instances=179"), ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                        ("conf", "spark.driver.memory=35G"), ("conf", "spark.driver.cores=5"), ("conf", "spark.driver.maxResultSize=6G"),
                        ("conf", "spark.network.timeout=1200s"), ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                        ("conf", "spark.sql.shuffle.partitions=6000"), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

rsm_lal_generate_daily_refresh_aerospike_cluster = EmrClusterTask(
    name="GenerateDailyRefreshRSMLalModelsInAerospike",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": slack_groups.ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=960
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

rsm_lal_additional_generate_daily_refresh_aerospike_step_options = [("date", target_date), ("inputRSMPath", overridden_scores_3p_path),
                                                                    ("rsmBatchName", rsm_batch_name), ("maxMapSizeForTopRSM", max_map_size),
                                                                    ("minUniquesForTopRSMSegments", minimum_sibv2_uniques_users),
                                                                    ("isTestMode", "false")]

rsm_lal_generate_daily_refresh_aerospike_step = EmrJobTask(
    name="RSM-LAL-Generate-Daily-Refresh-LalModels-InAerospike",
    class_name="jobs.lal.DailyRefreshRsmLalAerospike",
    executable_path=el_dorado_jar_path,
    additional_args_option_pairs_list=spark_config_options,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=rsm_lal_additional_generate_daily_refresh_aerospike_step_options,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rsm_lal_generate_daily_refresh_aerospike_cluster.cluster_specs,
)

rsm_lal_3p_validation_step_options = [("date", target_date), ("numberOfTopCandidates", 50), ("minimumSimilarityCoverageThreshold", 0.9),
                                      ("dailyBatchName", rsm_batch_name), ("lookBackDays", 7)]
rsm_lal_3p_validation_aerospike_step = EmrJobTask(
    name="RSM-LAL-3P-Validation-InAerospike",
    class_name="jobs.lal.rsm.ValidateThirdPartyLal",
    executable_path=el_dorado_jar_path,
    additional_args_option_pairs_list=spark_config_options,
    # Could look at whether another set of spark options may be more optimal
    eldorado_config_option_pairs_list=rsm_lal_3p_validation_step_options,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rsm_lal_generate_daily_refresh_aerospike_cluster.cluster_specs,
)

rsm_lal_generate_daily_refresh_aerospike_cluster.add_sequential_body_task(rsm_lal_generate_daily_refresh_aerospike_step)
rsm_lal_generate_daily_refresh_aerospike_cluster.add_sequential_body_task(rsm_lal_3p_validation_aerospike_step)

##############################################################################
# Config for RSM Push to Aerospike cluster
##############################################################################
rsm_lal_aerospike_push_cluster = EmrClusterTask(
    name=job_name + "push_to_aerospike",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5a.m5a_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        'Team': 'ADPB',
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5a.r5a_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=20,
    ),
    ec2_subnet_ids=[LalAerospikeConstant.job_ec2_subnet_id],
    emr_managed_master_security_group=LalAerospikeConstant.job_emr_managed_master_security_group,
    emr_managed_slave_security_group=LalAerospikeConstant.job_emr_managed_slave_security_group,
    service_access_security_group=LalAerospikeConstant.job_service_access_security_group,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

lal_push_to_aerospike_spark_options = [("executor-memory", "95G"), ("executor-cores", "15"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=95G"), ("conf", "spark.driver.maxResultSize=95G"),
                                       ("conf", "spark.sql.shuffle.partitions=3000"),
                                       ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

prod_push_rsm_score_to_aerospike_spark_step = EmrJobTask(
    name=push_rsm_score_step_name,
    class_name="jobs.lal.PublishToAerospikeLal",
    executable_path=el_dorado_jar_path,
    additional_args_option_pairs_list=lal_push_to_aerospike_spark_options,
    eldorado_config_option_pairs_list=[("modelResultsPath", model_results_path_prefix + target_date_no_dash + "/batch=" + rsm_batch_name),
                                       ("aerospikeAddress", LalAerospikeConstant.aerospike_address),
                                       ("aerospikeNamespace", LalAerospikeConstant.aerospike_namespace), ("ttl", LalAerospikeConstant.ttl),
                                       ("aerospikeSet", LalAerospikeConstant.aerospike_set), ("maxMapSize", max_map_size),
                                       ("isTestRun", "false")],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rsm_lal_aerospike_push_cluster.cluster_specs,
)

rsm_lal_aerospike_push_cluster.add_sequential_body_task(prod_push_rsm_score_to_aerospike_spark_step)

check_rsm_3p_validation_results_data = OpTask(
    op=DatasetRecencyOperator(
        dag=rsm_lal_dag.airflow_dag,
        datasets_input=[Datasources.lal.rsm_lal_3p_validation_dataset],
        cloud_provider=CloudProviders.aws,
        run_delta=timedelta(days=0),  # check yesterday log is available
        task_id='check_rsm_3p_validation_results_data'
    )
)
###########################################
#   job flow
###########################################

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=rsm_lal_dag.airflow_dag))

rsm_lal_dag >> check_rsm_id_relevance_score_sensor_task >> rsm_seed_selection_and_permission_cluster
for lal_cluster in lal_clusters:
    rsm_seed_selection_and_permission_cluster >> lal_cluster >> check_rsm_seed_population_relevance_score_sensor_task

check_rsm_seed_population_relevance_score_sensor_task >> user_lal_ratio_cluster
user_lal_ratio_cluster >> rsm_lal_generate_daily_refresh_aerospike_cluster >> check_rsm_3p_validation_results_data
check_rsm_3p_validation_results_data >> rsm_lal_aerospike_push_cluster >> final_dag_check
