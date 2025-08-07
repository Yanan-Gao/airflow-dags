from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta

from dags.cmo.audience_accelerator.tasks.segment.ctv_acr_audience_tvid_mapping import ACRAudienceTVIDMapping
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_segment_loader import ACRSegmentLoader
from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_weekly_segment_generation import \
    ACRWeeklySegmentGeneration
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################
pipeline_name = "fwm-audiences"
job_start_date = datetime(2024, 8, 19, 8, 0)
job_schedule_interval = timedelta(days=1)
provider = ACRProviders.FwmAudiences
input_bucket = 'thetradedesk-useast-data-import'

gen_to_run_offset = 1

run_date = '{{ logical_date.strftime(\"%Y-%m-%d\") }}'
gen_date = f'{{{{ (logical_date + macros.timedelta(days={gen_to_run_offset})).strftime(\"%Y-%m-%d\") }}}}'

config = AcrPipelineConfig(
    provider=provider,
    country=Country.US,
    enriched_dataset=Datasources.fwm.external.fwm_raw,
    data_delay=2,  # Gen date is -1 and then target date is -1 from gen date
    run_date=run_date,
    use_crosswalk=True,
    raw_data_path='audience',
    provider_graph_aggregate_version=3
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
    retry_delay=timedelta(hours=2),
    depends_on_past=True
)
adag: DAG = dag.airflow_dag


################################################################################################
# Steps - Check Target Dates
################################################################################################
# We don't get audience segments everyday so we want to handle those situations
def check_target_dates(logical_date, **context):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()

    gen_date_macro = (logical_date + timedelta(days=gen_to_run_offset)).strftime("%Y-%m-%d")
    prefix = f'fwm/audience/gd={gen_date_macro}/td={logical_date.strftime("%Y-%m-%d")}'

    if (cloud_storage.check_for_prefix(bucket_name="thetradedesk-useast-data-import", prefix=prefix)):
        print(f"------------ Found target file: {prefix}")
        return True

    print(f"------------ Didn't find the target file: {prefix}")
    return False


################################################################################################
# Steps - ACR Segments
################################################################################################
check_acr_segments = OpTask(
    op=DatasetRecencyOperator(
        task_id="check_segment_parquet_sync_files",
        datasets_input=[Datasources.sql.acr_provider_segments],
        lookback_days=0,
        cloud_provider=CloudProviders.aws,
        dag=adag
    )
)

segment_tvid_mapping = ACRAudienceTVIDMapping.get_task(config=config)

weekly_core_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=6, ebs_size=256, on_demand=True
)

weekly_segment_generation = ACRWeeklySegmentGeneration.get_task(
    config=config,
    core_fleet_type=weekly_core_fleet_type,
    run_date=run_date,
    provider_specific_java_options=[("useHybridCrosswalk", "false"), ("loadParentSegments", "true"), ("crosswalkGraphType", "fwm-uid2"),
                                    ("crosswalkGraphVersion", "1")],
    dag=adag
)

weekly_segment_loader = ACRSegmentLoader.get_task(
    config=config, job_type=ACRSegmentLoader.WEEKLY, output_file_count=10000, core_fleet_type=weekly_core_fleet_type
)

check_target_dates = OpTask(
    op=ShortCircuitOperator(task_id='check_target_dates', python_callable=check_target_dates, provide_context=True, dag=adag)
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

################################################################################################
# Pipeline Structure
################################################################################################
dag >> check_target_dates >> check_acr_segments >> segment_tvid_mapping >> weekly_segment_generation >> weekly_segment_loader >> final_dag_status_step
