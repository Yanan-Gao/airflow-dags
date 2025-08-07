import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

application_configuration = [
    {
        "Classification": "emrfs-site",
        "Properties": {
            "fs.s3.maxConnections": "1000"
        }
    },
    {
        "Classification": "core-site",
        "Properties": {
            "fs.s3a.connection.maximum": "1000",
            "fs.s3a.threads.max": "50"
        },
    },
    {
        "Classification": "capacity-scheduler",
        "Properties": {
            "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
        },
    },
    {
        "Classification": "yarn-site",
        "Properties": {
            "yarn.nodemanager.pmem-check-enabled": "false",
            "yarn.nodemanager.vmem-check-enabled": "false",
        },
    },
]

spark_options_list = [
    ("executor-memory", "188G"),
    ("executor-cores", "30"),
    ("num-executors", "180"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=50G"),
    ("conf", "spark.driver.cores=10"),
    ("conf", "spark.sql.shuffle.partitions=10740"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
]

non_conversion_campaign_spark_options_list = [
    ("executor-memory", "400G"),
    ("executor-cores", "64"),
    ("num-executors", "60"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=50G"),
    ("conf", "spark.driver.cores=10"),
    ("conf", "spark.driver.maxResultSize=16G"),
    ("conf", "spark.sql.shuffle.partitions=8000"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
]

no_audience_campaign_spark_options_list = [
    ("executor-memory", "188G"),
    ("executor-cores", "30"),
    ("num-executors", "48"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=50G"),
    ("conf", "spark.driver.cores=10"),
    ("conf", "spark.sql.shuffle.partitions=5000"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
]

merge_spark_options_list = [
    ("executor-memory", "188G"),
    ("executor-cores", "30"),
    ("num-executors", "32"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=50G"),
    ("conf", "spark.driver.cores=10"),
    ("conf", "spark.sql.shuffle.partitions=5000"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
]

fdp_dag = TtdDag(
    dag_id="adpb-fractional-data-pricing",
    start_date=datetime(2024, 7, 18, 6, 0, 0),
    schedule_interval=timedelta(hours=4),
    run_only_latest=True,  # Just execute the latest run. It's fine to skip old runs
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True,
)
dag = fdp_dag.airflow_dag

cluster = EmrClusterTask(
    name="FdpCampaignFragmentRelevanceScoresGeneration",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
        ],
        on_demand_weighted_capacity=180,
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

pfs_conversion_campaign_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="FdpCampaignFragmentRelevanceScoresGeneration",
    class_name="jobs.fractionaldatapricing.CampaignSegmentRelevanceScoreGeneratorJob",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[
        ("datetime", '{{ data_interval_start.strftime("%Y-%m-%dT%H:00:00") }}'),
        (
            "output_path",
            "models/fractionaldatapricing/lal_based_campaign_segment_relevance_scores/v=1",
        ),
        ("input_lal_path", "models/lal/all_model_unfiltered_results/v=3/xdvendorid=10"),
        ("rollOutRelevanceScoreMode", "false"),
        ("overrideLalAdvertiserVectorId", "56"),
        ("overrideRelevanceType", "3"),
        (
            "override_lal_path",
            "models/user_embeddings_lal/all_model_unfiltered_results/v=1/dailyMaximumFrequencyThreshold=48/lookBackDays=7",
        ),
    ],
    executable_path=jar_path,
)

non_conversion_campaign_cluster = EmrClusterTask(
    name="FdpNonConversionCampaignFragmentRelevanceScoresGeneration",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=60,
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

pfs_non_conversion_campaign_step = EmrJobTask(
    cluster_specs=non_conversion_campaign_cluster.cluster_specs,
    name="FdpNonConversionCampaignFragmentRelevanceScoresGeneration",
    class_name="jobs.fractionaldatapricing.NonConversionCampaignSegmentRelevanceScoreGeneratorJob",
    additional_args_option_pairs_list=non_conversion_campaign_spark_options_list,
    eldorado_config_option_pairs_list=[
        ("datetime", '{{ data_interval_start.strftime("%Y-%m-%dT%H:00:00") }}'),
        ("sib_sample_ratio", 0.01),
        ("campaign_sample_ratio", 0.125),
    ],
    executable_path=jar_path,
)

merge_cluster = EmrClusterTask(
    name="FdpCampaignFragmentRelevanceScoresMerge",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
        ],
        on_demand_weighted_capacity=32,
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

pfs_merge_step = EmrJobTask(
    cluster_specs=merge_cluster.cluster_specs,
    name="FdpCampaignFragmentRelevanceScoresMerge",
    class_name="jobs.fractionaldatapricing.CampaignSegmentRelevanceScoreMergerJob",
    additional_args_option_pairs_list=merge_spark_options_list,
    eldorado_config_option_pairs_list=[
        ("datetime", '{{ data_interval_start.strftime("%Y-%m-%dT%H:00:00") }}'), ("rollout_additional_score_mode", "true"),
        ("additional_campaign_score_path", "models/fractionaldatapricing/lal_based_campaign_segment_relevance_scores/v=4")
    ],
    executable_path=jar_path,
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=fdp_dag.airflow_dag))

cluster.add_sequential_body_task(pfs_conversion_campaign_step)
non_conversion_campaign_cluster.add_sequential_body_task(pfs_non_conversion_campaign_step)
merge_cluster.add_sequential_body_task(pfs_merge_step)

fdp_dag >> cluster >> merge_cluster
fdp_dag >> non_conversion_campaign_cluster >> merge_cluster
merge_cluster >> check
