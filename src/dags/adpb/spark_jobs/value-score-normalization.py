import copy
from datetime import datetime, timedelta

from airflow.operators.python import ShortCircuitOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
import logging

# Parameters
dag_name = "adpb-value-score-normalization"
job_start_date = datetime(2024, 10, 28, 0, 0, 0)
job_schedule_interval = timedelta(hours=24)
owner = ADPB.team
generate_value_score_days_of_week = [0]  # Monday

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

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

default_spark_options_list = [("conf", "spark.dynamicAllocation.enabled=false"),
                              ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

generationDate = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
default_eldorado_options_list = [("generationDate", generationDate)]

# DAG definition
value_score_normalization = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    enable_slack_alert=True
)

# CurrentDataRateCardsByAdvertiserGenerator
current_data_rate_cards_by_advertiser_generator_cluster = EmrClusterTask(
    name="CurrentDataRateCardsByAdvertiserGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": owner.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(2)],
        on_demand_weighted_capacity=2
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

current_data_rate_cards_by_advertiser_generator_step = EmrJobTask(
    cluster_specs=current_data_rate_cards_by_advertiser_generator_cluster.cluster_specs,
    name="CurrentDataRateCardsByAdvertiserGeneratorStep",
    class_name="jobs.valuescorenormalization.CurrentDataRateCardsByAdvertiserGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
    eldorado_config_option_pairs_list=copy.deepcopy(default_eldorado_options_list),
    configure_cluster_automatically=True,
    executable_path=jar_path
)
current_data_rate_cards_by_advertiser_generator_cluster.add_sequential_body_task(current_data_rate_cards_by_advertiser_generator_step)

# ThirdPartyAverageDataCostGenerator Cluster
third_party_average_data_cost_generator_cluster = EmrClusterTask(
    name="ThirdPartyAverageDataCostGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": owner.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_16xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(4)],
        on_demand_weighted_capacity=4
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

# PlatformAverageMediaCostByChannelGenerator Step
platform_average_media_cost_by_channel_generator_step = EmrJobTask(
    cluster_specs=third_party_average_data_cost_generator_cluster.cluster_specs,
    name="PlatformAverageMediaCostByChannelGeneratorStep",
    class_name="jobs.valuescorenormalization.PlatformAverageMediaCostByChannelGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
    eldorado_config_option_pairs_list=copy.deepcopy(default_eldorado_options_list),
    configure_cluster_automatically=True,
    executable_path=jar_path
)
third_party_average_data_cost_generator_cluster.add_sequential_body_task(platform_average_media_cost_by_channel_generator_step)

# RatedThirdPartyDataGenerator Step

rated_third_party_data_generator_step = EmrJobTask(
    cluster_specs=third_party_average_data_cost_generator_cluster.cluster_specs,
    name="RatedThirdPartyDataGeneratorStep",
    class_name="jobs.valuescorenormalization.RatedThirdPartyDataGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
    eldorado_config_option_pairs_list=copy.deepcopy(default_eldorado_options_list),
    configure_cluster_automatically=True,
    executable_path=jar_path
)
third_party_average_data_cost_generator_cluster.add_sequential_body_task(rated_third_party_data_generator_step)

# ThirdPartyAverageDataCostGenerator Step
third_party_average_data_cost_generator_step = EmrJobTask(
    cluster_specs=third_party_average_data_cost_generator_cluster.cluster_specs,
    name="ThirdPartyAverageDataCostGeneratorStep",
    class_name="jobs.valuescorenormalization.ThirdPartyAverageDataCostGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
    eldorado_config_option_pairs_list=copy.deepcopy(default_eldorado_options_list),
    executable_path=jar_path
)
third_party_average_data_cost_generator_cluster.add_sequential_body_task(third_party_average_data_cost_generator_step)

# Value Score Generator Cluster

raw_log_value_score_generator_cluster = EmrClusterTask(
    name="RawLogValueScoreGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": owner.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=60
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

raw_log_value_score_generator_overlap_step = EmrJobTask(
    cluster_specs=raw_log_value_score_generator_cluster.cluster_specs,
    name="CombinedValueScoreGeneratorJob_Overlap",
    class_name="jobs.valuescorenormalization.CombinedValueScoreGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
    eldorado_config_option_pairs_list=default_eldorado_options_list + [("relevanceScoreVersion", "overlap")],
    configure_cluster_automatically=True,
    executable_path=jar_path
)

raw_log_value_score_generator_rsm_step = EmrJobTask(
    cluster_specs=raw_log_value_score_generator_cluster.cluster_specs,
    name="ValueScoreGeneratorJob_Rsm",
    class_name="jobs.valuescorenormalization.CombinedValueScoreGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
    eldorado_config_option_pairs_list=default_eldorado_options_list + [("relevanceScoreVersion", "rsm")],
    configure_cluster_automatically=True,
    executable_path=jar_path
)

raw_log_value_score_generator_cluster.add_sequential_body_task(raw_log_value_score_generator_overlap_step)
raw_log_value_score_generator_cluster.add_sequential_body_task(raw_log_value_score_generator_rsm_step)

# Value Score Bounds Generator Cluster

raw_log_value_score_bounds_generator_cluster = EmrClusterTask(
    name="RawLogValueScoreBoundsGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": owner.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=25
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

for relevanceScoreVersion, filteringDataSet in [("overlap", "none"), ("rsm", "none")]:
    step = EmrJobTask(
        cluster_specs=raw_log_value_score_bounds_generator_cluster.cluster_specs,
        name=f"ValueScoreBoundsGeneratorStep_{relevanceScoreVersion}_{filteringDataSet}",
        class_name="jobs.valuescorenormalization.RawLogValueScoreBoundByChannelGeneratorJob",
        additional_args_option_pairs_list=copy.deepcopy(default_spark_options_list),
        eldorado_config_option_pairs_list=default_eldorado_options_list + [
            ("lowerPercentile", 0.001),
            ("upperPercentile", 0.999),
            ("relevanceScoreVersion", relevanceScoreVersion),
            ("filteringDataSet", filteringDataSet),
        ],
        configure_cluster_automatically=True,
        executable_path=jar_path
    )

    raw_log_value_score_bounds_generator_cluster.add_sequential_body_task(step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=value_score_normalization.airflow_dag))


# check if it is value score generation day, 1 day after new ue version publish, no need to run daily.
def check_is_value_score_generation_day(run_date_str):
    run_date_weekday = datetime.strptime(run_date_str, "%Y-%m-%d").weekday()
    if run_date_weekday not in generate_value_score_days_of_week:
        logging.info(f"Run date {run_date_str} is not value score generation day, skip")
        return False
    else:
        logging.info(f"Run date {run_date_str} is value score generation day, continue")
        return True


check_is_value_score_generation_day_op_task = OpTask(
    op=ShortCircuitOperator(
        task_id="is_value_score_generation_day",
        python_callable=check_is_value_score_generation_day,
        op_kwargs={"run_date_str": "{{ ds }}"},
        dag=value_score_normalization.airflow_dag,
        trigger_rule="none_failed",
    )
)

value_score_normalization >> current_data_rate_cards_by_advertiser_generator_cluster >> check_is_value_score_generation_day_op_task
value_score_normalization >> third_party_average_data_cost_generator_cluster >> check_is_value_score_generation_day_op_task
check_is_value_score_generation_day_op_task >> raw_log_value_score_generator_cluster >> raw_log_value_score_bounds_generator_cluster >> check
dag = value_score_normalization.airflow_dag
