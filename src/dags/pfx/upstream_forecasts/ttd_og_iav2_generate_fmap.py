from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd

from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.slack.slack_groups import PFX
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from dags.tv.constants import FORECAST_JAR_PATH

JAR_PATH = FORECAST_JAR_PATH

FORECAST_CONFIGURATION_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.UpstreamForecastConfigurationSetupJob"
RAILS_MAPPING_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.avails.HHAvailsV2RailsMapper"
OLD_RAILS_MAPPING_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.AvailsRailsMapperJob"
TIME_SEQUENCE_AGGREGATOR_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.TimeSequenceAggregatorJob"
AUDIENCE_MAPPER_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.AvailsAudienceMapperJob"
FREQUENCY_MAPPING_CLASS = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation.FrequencyMapWithWeightsAggregatorJob"

# conf inputs: target-date is both avails latest date and fmap gen date, demo-target-date is the latest demo date
# TODO: this remains manual after being deployed, meant to be called by upstream dag once well tested
target_date = "{{ dag_run.conf['target-date'] }}"
demo_target_date = "{{ dag_run.conf['demo-target-date'] }}"
avails_target_date = target_date
look_back_days = "{{ dag_run.conf['look-back-days'] }}"  # 28 days for prod runs
use_avails_filter = "{{ dag_run.conf.get('use-avails-filter', 'false') }}"

overall_sampling_factor = 900
s3_root_path_og = "s3a://ttd-ctv/upstream-forecast/data-preparation-og"
demo_root_path_og = s3_root_path_og
demo_folder_path_og = "owdi-demos/weighted"

# job_schedule_interval = "0 12 * * *"
job_start_date = datetime(2025, 2, 1)

dag = TtdDag(
    dag_id="ttd-og-iav2-generate-fmap",
    start_date=job_start_date,
    # schedule_interval=job_schedule_interval,
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "fplan", "fmap"],
    run_only_latest=True,
)

adag: DAG = dag.airflow_dag

cluster_name = "ttd-og-iav2-generate-fmap-cluster"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_16xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4),
        R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
        R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
        R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
        R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4),
        R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(6)
    ],
    on_demand_weighted_capacity=500,
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
        ("ttd.env", "prod"),
        ("targetDate", target_date),
        ("s3RootPath", s3_root_path_og),
    ],
    cluster_specs=cluster_task.cluster_specs,
    configure_cluster_automatically=False,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
)

rails_mapping_job_task = EmrJobTask(
    name="rails-mapping",
    class_name=RAILS_MAPPING_CLASS,
    executable_path=JAR_PATH,
    eldorado_config_option_pairs_list=[
        ("ttd.env", "prod"),
        ("graphName", "openGraphIav2"),  # This will load open graph avails
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
        ("lookBackDays", look_back_days),
        ("s3RootPath", s3_root_path_og),
        ("useAvailsFilter", use_avails_filter),
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
        ("ttd.env", "prod"),
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
        ("s3RootPath", s3_root_path_og),
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
        ("ttd.env", "prod"),
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
        ("demoTargetDate", demo_target_date),
        ("availsTargetDate", avails_target_date),
        ("s3RootPath", s3_root_path_og),
        ("demoRootPath", demo_root_path_og),
        ("demoFolderPath", demo_folder_path_og),
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
        ("ttd.env", "prod"),
        ("targetDate", target_date),
        ("overallSamplingFactor", overall_sampling_factor),
        ("s3RootPath", s3_root_path_og),
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
    ChainOfTasks("ttd-iav2-generate-fmap-tasks", [forecast_config_job_task, frequency_map_generation_job_task])
)

trigger_extrapolation_task = OpTask(
    op=TriggerDagRunOperator(
        dag=adag,
        task_id='trigger-downstream-extrapolated-fmap-gen',
        trigger_dag_id="ttd-og-reach-curve-extrapolation-dag",
        wait_for_completion=True,
        poke_interval=300,  # 5 min
    )
)

dag >> cluster_task >> trigger_extrapolation_task
