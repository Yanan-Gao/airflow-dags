import re
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.weekday import BranchDayOfWeekOperator, WeekDay
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
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################

pipeline_name = ACRProviders.Fwm
job_start_date = datetime(2024, 8, 19, 8, 0)
job_schedule_interval = timedelta(days=1)
provider = ACRProviders.Fwm

# Gen date is -1 and then target date is -14 from gen date
target_to_run_offset = 14

gen_date = '{{ (logical_date).strftime(\"%Y-%m-%d\") }}'
target_date = f'{{{{ (logical_date - macros.timedelta(days={target_to_run_offset})).strftime(\"%Y-%m-%d\") }}}}'

config = AcrPipelineConfig(
    provider=provider,
    country=Country.US,
    enriched_dataset=Datasources.fwm.generated.fwm_enriched,
    data_delay=target_to_run_offset,
    run_date=target_date,
    use_crosswalk=True,
    frequency_aggregation_version=5,
    segment_enriched_version=3,
    provider_graph_aggregate_version=3,
    gen_date=gen_date,  # gen_date is only used by the Audience pipeline. Passing here since None cannot be parsed in the Spark config.
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
# Steps - Check Target Dates
################################################################################################
def check_target_dates(**context):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()

    genDateMacro = (context['logical_date']).strftime("%Y-%m-%d")
    targetDateMacro = (context['logical_date'] + timedelta(days=-target_to_run_offset)).strftime("%Y-%m-%d")
    prefix = f"fwm/impression/v=2/gd={genDateMacro}/td={targetDateMacro}"

    if (cloud_storage.check_for_prefix(bucket_name="thetradedesk-useast-data-import", prefix=prefix)):
        print(f"------------ Found target file: {prefix}")
        return True

    print(f"------------ Didn't find the target file: {prefix}")
    return False


check_target_dates = OpTask(
    op=ShortCircuitOperator(task_id='check_target_dates', python_callable=check_target_dates, provide_context=True, dag=adag)
)


def verify_no_correction_files(**context):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()

    genDateMacro = (context['logical_date']).strftime("%Y-%m-%d")
    targetDateMacro = (context['logical_date'] + timedelta(days=-target_to_run_offset)).strftime("%Y-%m-%d")

    paths = cloud_storage.list_keys(bucket_name='thetradedesk-useast-data-import', prefix=f'fwm/impression/v=2/gd={genDateMacro}')

    numCorrectionFiles = 0
    for p in paths:
        found = re.search(rf'^((?!{re.escape(targetDateMacro)}).)*$', p)
        if (found is not None):
            numCorrectionFiles += 1
            print('NOTE: Correction file found at - ' + found[0])

    if (numCorrectionFiles > 0):
        raise AirflowFailException(
            'Found correction file(s). Please follow up and run the Spark job for the above dates delimted by "NOTE: Correction file found at"'
        )

    return 'Finished checking for correction files'


verify_no_correction_files = OpTask(
    op=PythonOperator(task_id='verify_no_correction_files', python_callable=verify_no_correction_files, provide_context=True, dag=adag)
)

################################################################################################
# Steps - Enrichment
################################################################################################

# enrichment
enrichment = EmrClusterTask(
    name=f'ctv-acr-{provider}-enrichment',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={
        "Team": CMO.team.jira_team,
    },
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.EightX, instance_capacity=12),
    use_on_demand_on_timeout=False,
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
enrichment.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=enrichment.cluster_specs,
        name='FwmEnrichment',
        class_name="jobs.ctv.linear.acr.fwm.FwmEnrichment",
        eldorado_config_option_pairs_list=[("genDate", gen_date), ("targetDate", target_date),
                                           ("outputPath", Datasources.fwm.generated.fwm_enriched.get_root_path())],
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
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=2),
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    retries=1,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
enrichment_stats.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=enrichment_stats.cluster_specs,
        name='FwmEnrichmentStats',
        class_name="jobs.ctv.linear.acr.fwm.FwmEnrichmentStats",
        eldorado_config_option_pairs_list=[("date", target_date),
                                           ("enrichedDataPath", Datasources.fwm.generated.fwm_enriched.get_root_path()),
                                           ("ttd.GraphHouseholdMatchingDataSet.isInChain", "true"),
                                           ("fwmUid2GraphPath", "s3://thetradedesk-useast-data-import/experian"), ("fwmUid2GraphVer", 1)],
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
        lookback_days=0,
        cloud_provider=CloudProviders.aws,
        dag=adag
    )
)

# Provider grain value mapping
provider_grain_value_mapping = CtvAcrProviderGrainValueMapping.get_task(config=config)

daily_graph_aggregation = CtvAcrEnrichedProviderDailyGraphAggregation.get_task(config=config)
daily_graph = ACRTVIDHouseholdGraphGenerationJob.get_task(config=config)

# Find segments that should be back-filled
back_fill_start = OpTask(op=EmptyOperator(task_id='back_fill_start'))
back_fill = CtvAcrSegmentsBackfillIdentify.get_task(config=config)

# Back-fill the segments
segment_tvid_mapping_backfill = ACRSegmentTVIDMapping.get_task(config=config, back_fill=True)

# set the trigger to none failed, allowing it to have an upstream that is skipped. This maybe should be an option
segment_tvid_mapping_dummy = OpTask(op=EmptyOperator(task_id="segment_tvid_mapping_dummy", dag=adag, trigger_rule=TriggerRule.NONE_FAILED))
segment_tvid_mapping = ACRSegmentTVIDMapping.get_task(config=config, back_fill=False)

frequency_aggregation_backfill = CtvAcrUniversalDailyFrequencyAggregation(config=config, back_fill=True)
daily_frequency_aggregation_backfill = frequency_aggregation_backfill.get_daily_aggregation()
ten_day_frequency_aggregation_backfill = frequency_aggregation_backfill.get_ten_day_aggregation()

frequency_aggregation = CtvAcrUniversalDailyFrequencyAggregation(config=config, back_fill=False)
daily_frequency_aggregation = frequency_aggregation.get_daily_aggregation()
ten_day_frequency_aggregation = frequency_aggregation.get_ten_day_aggregation()
ninety_day_frequency_aggregation = frequency_aggregation.get_ninety_day_aggregation()

weekly_core_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=25, on_demand=True
)

# useHybridCrosswalk is false for fwm
fwm_specific_segment_generation_java_option_list = [("loadParentSegments", "true"), ("ttd.FwmUid2CrosswalkDataSet.isInChain", "true"),
                                                    ("ttd.GraphHouseholdMatchingDataSet.isInChain", "true"),
                                                    ("crosswalkGraphType", "fwm-uid2"), ("crosswalkGraphVersion", "1")]

weekly_segment_start = OpTask(op=EmptyOperator(task_id='weekly_segment_start'))
weekly_segment_generation = ACRWeeklySegmentGeneration.get_task(
    config=config,
    core_fleet_type=weekly_core_fleet_type,
    provider_specific_java_options=fwm_specific_segment_generation_java_option_list,
    dag=adag
)

weekly_segment_loader = ACRSegmentLoader.get_task(
    config=config,
    job_type=ACRSegmentLoader.WEEKLY,
    output_file_count=250000,
    core_fleet_type=weekly_core_fleet_type,
    step_configuration=[("conf", "spark.sql.shuffle.partitions=2400")]
)

daily_core_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=25, on_demand=True
)

daily_segment_generation = ACRDailySegmentGeneration.get_task(
    config=config,
    core_fleet_type=daily_core_fleet_type,
    provider_specific_java_options=fwm_specific_segment_generation_java_option_list,
    dag=adag
)

daily_segment_loader = ACRSegmentLoader.get_task(
    config=config, job_type=ACRSegmentLoader.DAILY, output_file_count=42000, core_fleet_type=daily_core_fleet_type
)

exposure_core_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=15, on_demand=True
)

exposure_segment_generation = ACRExposureSegmentGeneration.get_task(
    config=config,
    core_fleet_type=exposure_core_fleet_type,
    provider_specific_java_options=fwm_specific_segment_generation_java_option_list,
    dag=adag
)

exposure_segment_loader = ACRSegmentLoader.get_task(
    config=config, job_type=ACRSegmentLoader.EXPOSURE, output_file_count=5000, core_fleet_type=exposure_core_fleet_type
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

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

################################################################################################
# Pipeline Structure
################################################################################################

dag >> check_target_dates >> verify_no_correction_files >> enrichment >> enrichment_stats >> check_acr_segments

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
