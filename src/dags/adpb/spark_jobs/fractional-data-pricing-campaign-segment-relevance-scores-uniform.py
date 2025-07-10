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

spark_options_list = [("executor-memory", "188G"), ("executor-cores", "30"), ("num-executors", "70"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=50G"),
                      ("conf", "spark.driver.cores=10"), ("conf", "spark.sql.shuffle.partitions=7000"),
                      ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

fdp_campaign_segment_relevance_scores_generation = TtdDag(
    dag_id="adpb-fdp-campaign-segment-relevance-scores-uniform",
    start_date=datetime(2025, 3, 12, 4, 0, 0),
    schedule_interval=timedelta(hours=24),
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True,
    run_only_latest=False
)

# RSM FP ###

rsm_cluster = EmrClusterTask(
    name="RSM_FdpCampaignFragmentRelevanceScoresGeneration",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
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
        on_demand_weighted_capacity=70
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)

pfs_rsm_conversion_campaign_step = EmrJobTask(
    cluster_specs=rsm_cluster.cluster_specs,
    name="FdpCampaignFragmentRelevanceScoresGenerationRSM",
    class_name="jobs.fractionaldatapricing.SeedCampaignSegmentRelevanceScoreGeneratorJob",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                       ("output_path", "models/fractionaldatapricing/lal_based_campaign_segment_relevance_scores/v=4"),
                                       ("input_lal_path", "models/rsm_lal/all_model_unfiltered_overridden_results/v=1/idCap=100000/"),
                                       ("prometheusClientApplicationNamePostfix", "_rsm"), ("input_relevance_type", "6")],
    executable_path=jar_path
)
rsm_cluster.add_sequential_body_task(pfs_rsm_conversion_campaign_step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=fdp_campaign_segment_relevance_scores_generation.airflow_dag))

fdp_campaign_segment_relevance_scores_generation >> rsm_cluster >> check

dag = fdp_campaign_segment_relevance_scores_generation.airflow_dag
