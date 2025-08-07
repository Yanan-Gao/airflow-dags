from datetime import datetime
from airflow import DAG

from dags.pfx.upstream_forecasts.ttd_generate_reach_curve_pyspark import base_job_name, ReachCurveScheduling
from dags.tv.constants import FORECAST_JAR_PATH
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.slack.slack_groups import PFX
from ttd.tasks.chain import ChainOfTasks

JAR_PATH = FORECAST_JAR_PATH

FORECAST_CONFIGURATION_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.UpstreamForecastConfigurationSetupJob"
LEGACY_RAILS_MAPPING_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.AvailsRailsMapperJob"
TIME_SEQUENCE_AGGREGATOR_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.TimeSequenceAggregatorJob"
AUDIENCE_MAPPER_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.AvailsAudienceMapperJob"
FREQUENCY_MAPPING_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.FrequencyMapWithWeightsAggregatorJob"

# conf inputs: target-date is both avails latest date and fmap gen date, demo-target-date is the latest demo date
# TODO: this remains manual after being deployed, meant to be called by upstream dag once well tested
target_date = "{{ dag_run.conf['target-date'] }}"
demo_target_date = "{{ dag_run.conf['demo-target-date'] }}"
avails_target_date = target_date
look_back_days = "{{ dag_run.conf['look-back-days'] }}"  # 28 days for prod runs
overall_sampling_factor = 900

# job_schedule_interval = "0 12 * * *"
job_start_date = datetime(2024, 10, 1)

dag = TtdDag(
    dag_id="ctv-legacy-generate-fmap",
    start_date=job_start_date,
    # schedule_interval=job_schedule_interval,
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "fplan", "fmap"],
    run_only_latest=True,
)

adag: DAG = dag.airflow_dag

cluster_name = "ctv-legacy-generate-fmap-cluster"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_12xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6gd.r6gd_16xlarge().with_fleet_weighted_capacity(4),
        R6gd.r6gd_8xlarge().with_fleet_weighted_capacity(2),
        R6gd.r6gd_4xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=250,
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

cluster_task = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.PFX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations],
)

additional_args_option_pairs_list = [("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]

forecast_config_job_task = EmrJobTask(
    name="forecast-configuration-setup",
    class_name=FORECAST_CONFIGURATION_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("targetDate", target_date),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

rails_mapping_job_task = EmrJobTask(
    name="rails-mapping",
    class_name=LEGACY_RAILS_MAPPING_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
        ("lookBackDays", look_back_days),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

time_sequence_aggregator_job_task = EmrJobTask(
    name="time-sequence-aggregator",
    class_name=TIME_SEQUENCE_AGGREGATOR_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

audience_mapping_job_task = EmrJobTask(
    name="audience-mapping",
    class_name=AUDIENCE_MAPPER_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
        ("demoTargetDate", demo_target_date),
        ("availsTargetDate", avails_target_date),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

frequency_map_generation_job_task = EmrJobTask(
    name="frequency-map-generation-with-weights",
    class_name=FREQUENCY_MAPPING_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

(
    forecast_config_job_task >> rails_mapping_job_task >> time_sequence_aggregator_job_task >> audience_mapping_job_task >>
    frequency_map_generation_job_task
)

cluster_task.add_parallel_body_task(
    ChainOfTasks("ctv-legacy-generate-fmap-tasks", [forecast_config_job_task, frequency_map_generation_job_task])
)

trigger_reach_curve_gen_v2 = ReachCurveScheduling.create_trigger_reach_curve_gen_dag_task(
    parent_dag=adag, trigger_dag_id=f"{base_job_name}-v2", trigger_input_date=target_date
)

dag >> cluster_task
cluster_task.last_airflow_op() >> trigger_reach_curve_gen_v2
