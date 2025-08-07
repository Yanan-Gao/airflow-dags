from datetime import datetime, timedelta

import datasources.sources.frequency_map_datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.tasks.op import OpTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB

###########################################
#   job settings
###########################################
from ttd.ttdenv import TtdEnvFactory

job_name = "adpb-user-embedding-lal"
job_schedule_interval = timedelta(days=1)
job_ec2_subnet_id = ["subnet-0e82171b285634d41"]
el_dorado_jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
forecast_etl_jar_path = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
spark_3_emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3  # Ensure spark3 and the correct scala version is used for the spark steps
job_start_date = datetime(2024, 6, 23, 17, 0)

target_date = "{{ ds }}"
target_date_no_dash = "{{ ds_nodash }}"

standard_cluster_tags = {
    "Team": ADPB.team.jira_team,
}

application_configuration = [{
    "Classification": "spark-defaults",
    "Properties": {
        "spark.driver.maxResultSize": "30G",
    }
}]

generation_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6g.r6g_12xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

generation_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
        R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
        R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64)
    ],
    on_demand_weighted_capacity=60 * 96,
)

###########################################
#   DAG setup
###########################################
user_embedding_lal_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_tags=ADPB.team.sub_team,
    slack_channel="#scrum-adpb-alerts",
)

dag = user_embedding_lal_dag.airflow_dag

user_embedding_lal_cluster = EmrClusterTask(
    name="user-embedding-lal-cluster",
    master_fleet_instance_type_configs=generation_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=generation_core_fleet_instance_type_configs,
    cluster_tags={**standard_cluster_tags},
    enable_prometheus_monitoring=True,
    emr_release_label=spark_3_emr_release_label,
    additional_application_configurations=application_configuration,
    environment=TtdEnvFactory.prod
)

seed_selection_and_permission_step = EmrJobTask(
    cluster_specs=user_embedding_lal_cluster.cluster_specs,
    name="UserEmbeddingLal-SeedSelectionAndPermission",
    class_name="jobs.lal.ue.SeedSelectionAndPermissionComputer",
    executable_path=el_dorado_jar_path,
    eldorado_config_option_pairs_list=[
        ("date", target_date),
    ],
    timeout_timedelta=timedelta(hours=1)
)

user_embedding_relevance_score_calculation_step = EmrJobTask(
    cluster_specs=user_embedding_lal_cluster.cluster_specs,
    name="UserEmbeddingLal-RelevanceScoreComputer",
    class_name="com.thetradedesk.etlforecastjobs.universalforecasting.relevance.lal.TargetingDataRelevanceComputerJob",
    executable_path=forecast_etl_jar_path,
    eldorado_config_option_pairs_list=[("targetDate", target_date), ("numberOfQuantiles", 101), ("dailyMaximumFrequencyThreshold", 24 * 2),
                                       ("lookBackDays", 7)],
    timeout_timedelta=timedelta(hours=8)
)

# check if the segment kll sketches is ready, skip otherwise
check_targeting_data_kll_sketch_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[datasources.sources.frequency_map_datasources.FrequencyMapDataSources.targetingDataEmbeddingKLLSketch],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
        task_id='check_targeting_data_kll_sketch',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 5,  # wait up to 5 hours
    )
)
user_embedding_1p_lal_cluster = EmrClusterTask(
    name="user-embedding-1p-lal-cluster",
    master_fleet_instance_type_configs=generation_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=generation_core_fleet_instance_type_configs,
    cluster_tags={**standard_cluster_tags},
    enable_prometheus_monitoring=True,
    emr_release_label=spark_3_emr_release_label,
    additional_application_configurations=application_configuration,
    environment=TtdEnvFactory.prod
)

user_embedding_1p_relevance_score_calculation_step = EmrJobTask(
    cluster_specs=user_embedding_1p_lal_cluster.cluster_specs,
    name="UserEmbeddingLal-RelevanceScoreComputer",
    class_name="com.thetradedesk.etlforecastjobs.universalforecasting.relevance.lal.FirstPartyTargetingDataRelevanceComputerJob",
    executable_path=forecast_etl_jar_path,
    eldorado_config_option_pairs_list=[("targetDate", target_date), ("numberOfQuantiles", 101), ("dailyMaximumFrequencyThreshold", 24 * 2),
                                       ("lookBackDays", 7)],
    timeout_timedelta=timedelta(hours=4)
)

###########################################
#   job flow
###########################################
user_embedding_lal_cluster.add_sequential_body_task(seed_selection_and_permission_step)
user_embedding_lal_cluster.add_sequential_body_task(user_embedding_relevance_score_calculation_step)
user_embedding_1p_lal_cluster.add_sequential_body_task(user_embedding_1p_relevance_score_calculation_step)
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=user_embedding_lal_dag.airflow_dag))

user_embedding_lal_dag >> check_targeting_data_kll_sketch_sensor_task >> user_embedding_lal_cluster >> final_dag_check
seed_selection_and_permission_step >> user_embedding_1p_lal_cluster >> final_dag_check
