from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator, WeekDay
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from dags.cmo.audience_accelerator.tasks.ctv_acr_enriched_provider_daily_graph_aggregation import \
    CtvAcrEnrichedProviderDailyGraphAggregation
from dags.cmo.audience_accelerator.tasks.ctv_acr_enriched_provider_graph import ACRTVIDHouseholdGraphGenerationJob
from dags.cmo.audience_accelerator.tasks.segment.backfill.ctv_acr_segments_backfill_identify import \
    CtvAcrSegmentsBackfillIdentify
from dags.cmo.audience_accelerator.tasks.segment.ctv_acr_provider_grain_value_mapping import \
    CtvAcrProviderGrainValueMapping
from dags.cmo.audience_accelerator.tasks.segment.ctv_acr_segment_tvid_mapping import ACRSegmentTVIDMapping
from dags.cmo.audience_accelerator.tasks.segment.ctv_acr_universal_daily_frequency_aggregation import \
    CtvAcrUniversalDailyFrequencyAggregation
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_daily_segment_generation import \
    ACRDailySegmentGeneration
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_exposure_segment_generation import \
    ACRExposureSegmentGeneration
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_segment_loader import ACRSegmentLoader
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_weekly_segment_generation import \
    ACRWeeklySegmentGeneration
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################
pipeline_name = "tivo-v2"
job_start_date = datetime(2024, 8, 19, 8, 0)
job_schedule_interval = timedelta(days=1)
provider = ACRProviders.Tivo
data_delay = 1
run_date_macro = f'{{{{ (logical_date - macros.timedelta(days={data_delay})).strftime(\"%Y-%m-%d\") }}}}'

cluster_tags = {
    'Team': CMO.team.jira_team,
}

config = AcrPipelineConfig(
    provider=provider,
    country=Country.US,
    enriched_dataset=Datasources.tivo.generated.tivo_enriched_v2,
    data_delay=data_delay,
    run_date=run_date_macro,
    use_crosswalk=True,
    provider_graph_aggregate_version=2
)

################################################################################################
# DAG
################################################################################################
dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["acr", config.provider],
    retries=1,
    retry_delay=timedelta(minutes=15),
    depends_on_past=True
)

adag: DAG = dag.airflow_dag


################################################################################################
# Pipeline Dependencies
################################################################################################
def check_tivo_data(wildcard_key: str):
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    if aws_storage.check_for_wildcard_key(bucket_name='thetradedesk-useast-data-import', wildcard_key=wildcard_key):
        print(f's3://thetradedesk-useast-data-import/{wildcard_key} exists')
        return True
    print(f's3://thetradedesk-useast-data-import/{wildcard_key} does not exists')
    return False


# The following need to use S3KeySensors because we require wildcard functionality
tivo_adlog_sensor = OpTask(
    op=PythonSensor(
        task_id='tivo_adlog_available',
        poke_interval=60 * 30,  # 30 minutes
        timeout=60 * 60 * 2,  # 2 hours = pipeline retry cadence
        python_callable=check_tivo_data,
        op_kwargs={
            'wildcard_key':
            'tivo/tv_viewership/adlog_papaya_1/data/incremental/file_name=' +
            '{{ (logical_date + macros.timedelta(days=-1)).strftime(\"%Y_%m_%d\") }}' + '_*/_SUCCESS',
        },
        dag=adag
    )
)

tivo_complete_adlog_sensor = OpTask(
    op=PythonSensor(
        task_id='tivo_complete_adlog_available',
        poke_interval=60 * 30,  # 30 minutes
        timeout=60 * 60 * 2,  # 2 hours = pipeline retry cadence
        python_callable=check_tivo_data,
        op_kwargs={
            'wildcard_key':
            'tivo/tv_viewership/adlog_papaya_1/data/complete/file_name=' +
            '{{ (logical_date + macros.timedelta(days=-14)).strftime(\"%Y_%m_%d\") }}' + '_*/_SUCCESS',
        },
        dag=adag
    )
)

tivo_experian_sensor = OpTask(
    op=PythonSensor(
        task_id='tivo_experian_mapping_available',
        poke_interval=60 * 30,  # 30 minutes
        timeout=60 * 60 * 2,  # 2 hours = pipeline retry cadence
        python_callable=check_tivo_data,
        op_kwargs={
            'wildcard_key':
            'tivo/tv_viewership/adlog_papaya_1/experian/incremental/file_name=' +
            '{{ (logical_date + macros.timedelta(days=-1)).strftime(\"%Y_%m_%d\") }}' + '_*',
        },
        dag=adag
    )
)

tivo_ip_sensor = OpTask(
    op=PythonSensor(
        task_id='tivo_ip_available',
        poke_interval=60 * 30,  # 30 minutes
        timeout=60 * 60 * 2,  # 2 hours = pipeline retry cadence
        python_callable=check_tivo_data,
        op_kwargs={
            'wildcard_key':
            'tivo/tv_viewership/papaya_1/ipaddress/file_name=' + '{{ (logical_date + macros.timedelta(days=-2)).strftime(\"%Y_%m_%d\") }}' +
            '_*',
        },
        dag=adag
    )
)
tivo_maid_sensor = OpTask(
    op=PythonSensor(
        task_id='tivo_maid_available',
        poke_interval=60 * 30,  # 30 minutes
        timeout=60 * 60 * 2,  # 2 hours = pipeline retry cadence
        python_callable=check_tivo_data,
        op_kwargs={
            'wildcard_key':
            'tivo/tv_viewership/papaya_1/maid/file_name=' + '{{ (logical_date + macros.timedelta(days=-2)).strftime(\"%Y_%m_%d\") }}' +
            '_*',
        },
        dag=adag
    )
)

################################################################################################
# Steps - Enrichment
################################################################################################
enrichment_incremental_start = OpTask(op=EmptyOperator(task_id='enrichment_incremental_start', trigger_rule=TriggerRule.NONE_FAILED))
enrichment_incremental = EmrClusterTask(
    name=f'ctv-acr-{provider}-enrichment',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={
        "Team": CMO.team.jira_team,
    },
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, instance_capacity=10),
    use_on_demand_on_timeout=False,
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
enrichment_incremental.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=enrichment_incremental.cluster_specs,
        name='TivoEnrichment-Incremental',
        class_name="jobs.ctv.linear.acr.tivo.TivoEnrichment",
        eldorado_config_option_pairs_list=[("date", '{{ (logical_date + macros.timedelta(days=-1)).strftime(\"%Y-%m-%d\") }}'),
                                           ("outputPath", Datasources.tivo.generated.tivo_enriched_v2.get_root_path()),
                                           ("adLogType", "incremental"), ("version", 2)],
        timeout_timedelta=timedelta(minutes=90),
        executable_path=config.jar,
        additional_args_option_pairs_list=config.get_step_additional_configurations()
    )
)

enrichment_complete = EmrClusterTask(
    name=f'ctv-acr-{provider}-enrichment-complete',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={
        "Team": CMO.team.jira_team,
    },
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.FourX, instance_capacity=10),
    use_on_demand_on_timeout=False,
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
enrichment_complete.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=enrichment_complete.cluster_specs,
        name='TivoEnrichment-Complete',
        class_name="jobs.ctv.linear.acr.tivo.TivoEnrichment",
        eldorado_config_option_pairs_list=[("date", '{{ (logical_date + macros.timedelta(days=-14)).strftime(\"%Y-%m-%d\") }}'),
                                           ("outputPath", Datasources.tivo.generated.tivo_enriched_v2.get_root_path()),
                                           ("adLogType", "complete"), ("version", 1)],
        timeout_timedelta=timedelta(minutes=90),
        executable_path=config.jar,
        additional_args_option_pairs_list=config.get_step_additional_configurations()
    )
)
# enrichment stats
enrichment_stats = EmrClusterTask(
    name=f'ctv-acr-{provider}-enrichment-stats',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags={
        "Team": CMO.team.jira_team,
    },
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, instance_capacity=2),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
enrichment_stats.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=enrichment_stats.cluster_specs,
        name='TivoEnrichmentStats',
        class_name="jobs.ctv.linear.acr.tivo.TivoEnrichmentStats",
        eldorado_config_option_pairs_list=[("date", run_date_macro),
                                           ("enrichedDataPath", Datasources.fwm.generated.fwm_enriched.get_root_path())],
        timeout_timedelta=timedelta(hours=1),
        executable_path=config.jar,
        additional_args_option_pairs_list=config.get_step_additional_configurations()
    )
)

################################################################################################
# Steps - ACR Segments
################################################################################################

check_acr_segments = OpTask(
    op=DatasetRecencyOperator(
        task_id="check_segment_parquet_sync_files",
        datasets_input=[Datasources.sql.acr_provider_segments, Datasources.sql.acr_provider_brand_segments],
        cloud_provider=CloudProviders.aws,
        dag=adag
    )
)

# provider grain value mapping
provider_grain_value_mapping = CtvAcrProviderGrainValueMapping.get_task(config=config)

daily_graph_aggregation = CtvAcrEnrichedProviderDailyGraphAggregation.get_task(config=config)
daily_graph = ACRTVIDHouseholdGraphGenerationJob.get_task(config=config)

# Find segments that should be back-filled
back_fill_start = OpTask(op=EmptyOperator(task_id='back_fill_start'))
back_fill = CtvAcrSegmentsBackfillIdentify.get_task(config=config)

# # Back-fill the segments
segment_tvid_mapping_backfill = ACRSegmentTVIDMapping.get_task(config=config, back_fill=True)

# set the trigger to none failed, allowing it to have an upstream that is skipped. This maybe should be an option
segment_tvid_mapping_dummy = OpTask(
    op=EmptyOperator(task_id="segment_tvid_mapping_dummy", trigger_rule=TriggerRule.NONE_FAILED, dag=adag),
)
segment_tvid_mapping = ACRSegmentTVIDMapping.get_task(config=config, back_fill=False)

frequency_aggregation_backfill = CtvAcrUniversalDailyFrequencyAggregation(config=config, back_fill=True)
daily_frequency_aggregation_backfill = frequency_aggregation_backfill.get_daily_aggregation()
ten_day_frequency_aggregation_backfill = frequency_aggregation_backfill.get_ten_day_aggregation()

frequency_aggregation = CtvAcrUniversalDailyFrequencyAggregation(config=config, back_fill=False)
daily_frequency_aggregation = frequency_aggregation.get_daily_aggregation()
ten_day_frequency_aggregation = frequency_aggregation.get_ten_day_aggregation()
ninety_day_frequency_aggregation = frequency_aggregation.get_ninety_day_aggregation()

tivo_specific_segment_mapper_java_option_list = [("tivoExperianS3Root", Datasources.tivo.external.tivo_experian_luids.get_root_path()),
                                                 ("useHybridCrosswalk", "true"), ("useDirectFallbackJoinMethod", "false"),
                                                 ("loadParentSegments", "true")]

# Weekly
weekly_gen_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=25, ebs_size=256, on_demand=True
)

weekly_segment_start = OpTask(op=EmptyOperator(task_id='weekly_segment_start'))
weekly_segment_generation = ACRWeeklySegmentGeneration.get_task(
    config=config,
    core_fleet_type=weekly_gen_fleet_type,
    provider_specific_java_options=tivo_specific_segment_mapper_java_option_list + [("tivoCrosswalkLookbackDays", 90)],
    step_configuration=[("conf", "spark.sql.shuffle.partitions=2400")],
    dag=adag
)

weekly_load_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=20, ebs_size=250, on_demand=True
)

weekly_segment_loader = ACRSegmentLoader.get_task(
    config=config,
    job_type=ACRSegmentLoader.WEEKLY,
    output_file_count=275000,
    core_fleet_type=weekly_load_fleet_type,
    step_configuration=[("conf", "spark.sql.shuffle.partitions=2400")]
)

# Daily
daily_gen_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=20, ebs_size=400, on_demand=True
)

daily_segment_generation = ACRDailySegmentGeneration.get_task(
    config=config,
    core_fleet_type=daily_gen_fleet_type,
    provider_specific_java_options=tivo_specific_segment_mapper_java_option_list + [("tivoCrosswalkLookbackDays", 1)],
    dag=adag
)

daily_load_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=18, ebs_size=256, on_demand=True
)

daily_segment_loader = ACRSegmentLoader.get_task(
    config=config, job_type=ACRSegmentLoader.DAILY, output_file_count=50000, core_fleet_type=daily_load_fleet_type
)

# Exposure
exposure_gen_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=15, ebs_size=256, on_demand=True
)

exposure_segment_generation = ACRExposureSegmentGeneration.get_task(
    config=config,
    core_fleet_type=exposure_gen_fleet_type,
    provider_specific_java_options=tivo_specific_segment_mapper_java_option_list + [("tivoCrosswalkLookbackDays", 90)],
    dag=adag
)

exposure_load_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=15, on_demand=True
)

exposure_segment_loader = ACRSegmentLoader.get_task(
    config=config, job_type=ACRSegmentLoader.EXPOSURE, output_file_count=8000, core_fleet_type=exposure_load_fleet_type
)

backfill_branch = OpTask(
    op=BranchDayOfWeekOperator(
        week_day=[WeekDay.MONDAY],
        task_id='backfill_branch',
        follow_task_ids_if_true=['back_fill_start', 'segment_tvid_mapping_dummy'],
        follow_task_ids_if_false=['segment_tvid_mapping_dummy'],
        use_task_logical_date=True,
        dag=adag
    )
)

should_run_weekly_segment = OpTask(
    op=BranchDayOfWeekOperator(
        week_day=[WeekDay.MONDAY],
        task_id='should_run_weekly_segment',
        follow_task_ids_if_true=['weekly_segment_start'],
        follow_task_ids_if_false=[],
        use_task_logical_date=True,
        dag=adag
    )
)

################################################################################################
#  Extra Job - Pre-Process Tivo Household IP/Maid mapping
################################################################################################
# preproc ip/maid
ip_maid_job = EmrClusterTask(
    name=f'{provider}-ip-maid-processing',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, instance_capacity=2),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
ip_maid_job.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=ip_maid_job.cluster_specs,
        name='TivoInputProcessor-IP-MAID',
        class_name="jobs.ctv.linear.acr.tivo.TivoInputProcessor",
        eldorado_config_option_pairs_list=[("date", '{{ (logical_date + macros.timedelta(days=-2)).strftime(\"%Y-%m-%d\") }}')],
        timeout_timedelta=timedelta(hours=1),
        executable_path=config.jar,
        additional_args_option_pairs_list=config.get_step_additional_configurations()
    )
)

should_run_ip_maid = OpTask(
    op=BranchDayOfWeekOperator(
        week_day=[WeekDay.TUESDAY],
        task_id='should_run_ip_maid',
        follow_task_ids_if_true=['tivo_ip_available', 'enrichment_incremental_start'],
        follow_task_ids_if_false=['enrichment_incremental_start'],
        use_task_logical_date=True,
        dag=adag
    )
)
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

################################################################################################
# Pipeline Structure
################################################################################################

dag >> tivo_adlog_sensor >> enrichment_incremental
dag >> tivo_experian_sensor >> enrichment_incremental
dag >> tivo_complete_adlog_sensor >> enrichment_complete
enrichment_incremental >> enrichment_complete
enrichment_complete >> enrichment_stats
enrichment_incremental >> enrichment_stats
enrichment_stats >> check_acr_segments

check_acr_segments >> daily_graph_aggregation >> daily_graph
check_acr_segments >> provider_grain_value_mapping >> final_dag_status_step
check_acr_segments >> backfill_branch

# backfill branching
backfill_branch >> back_fill_start >> back_fill >> segment_tvid_mapping_backfill >> daily_frequency_aggregation_backfill >> ten_day_frequency_aggregation_backfill >> segment_tvid_mapping_dummy
backfill_branch >> segment_tvid_mapping_dummy >> segment_tvid_mapping

# frequency aggregation
segment_tvid_mapping >> daily_frequency_aggregation >> ten_day_frequency_aggregation >> ninety_day_frequency_aggregation

# daily_segment_generation
daily_graph >> daily_segment_generation
segment_tvid_mapping >> daily_segment_generation
daily_segment_generation >> daily_segment_loader >> final_dag_status_step

# exposure_segment_generation
daily_graph >> exposure_segment_generation
ninety_day_frequency_aggregation >> exposure_segment_generation
exposure_segment_generation >> exposure_segment_loader >> final_dag_status_step

# weekly_segment_generation
daily_graph >> should_run_weekly_segment
ninety_day_frequency_aggregation >> should_run_weekly_segment
should_run_weekly_segment >> weekly_segment_start >> weekly_segment_generation >> weekly_segment_loader >> final_dag_status_step

# ip/maid
dag >> should_run_ip_maid >> tivo_ip_sensor >> tivo_maid_sensor >> ip_maid_job >> enrichment_incremental_start >> enrichment_incremental
dag >> should_run_ip_maid >> enrichment_incremental_start >> enrichment_incremental
