from airflow import DAG
from datetime import datetime, timedelta

from dags.cmo.audience_accelerator.tasks.segment.loader.ctv_acr_segment_loader import ACRSegmentLoader
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances
from datasources.datasources import Datasources
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import CTV

################################################################################################
# Configs -- Change things here for different providers
################################################################################################
pipeline_name = "tivo-test"
job_start_date = datetime(2024, 9, 11, 0, 0)
job_schedule_interval = timedelta(days=1)
provider = ACRProviders.Tivo
data_delay = 1
run_date_macro = f'{{{{ (logical_date - macros.timedelta(days={data_delay})).strftime(\"%Y-%m-%d\") }}}}'

cluster_tags = {
    'Team': CTV.team.jira_team,
}

config = AcrPipelineConfig(
    provider=provider,
    country=Country.US,
    enriched_dataset=Datasources.tivo.generated.tivo_enriched_v2,
    data_delay=data_delay,
    run_date=run_date_macro,
    use_crosswalk=True,
    jar="s3://ttd-build-artefacts/tv-data/mergerequests/rsk-CMO-59-api-segment-loader/jobs/jars/latest/ltv-data-jobs.jar"
)

################################################################################################
# DAG
################################################################################################
dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CTV.segments().alarm_channel,
    slack_tags=CTV.segments().sub_team,
    tags=[CTV.team.jira_team, "acr", config.provider],
    retries=1,
    retry_delay=timedelta(minutes=15),
    depends_on_past=True
)

adag: DAG = dag.airflow_dag

daily_load_fleet_type = getFleetInstances(
    EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.FourX, instance_capacity=4, ebs_size=100, on_demand=True
)

daily_segment_loader = ACRSegmentLoader.get_task(
    config=config,
    job_type=ACRSegmentLoader.DAILY,
    output_file_count=30000,
    core_fleet_type=daily_load_fleet_type,
    additional_java_options=[("enableMetrics", "false"), ("loadViaApi", "true")]
)

################################################################################################
# Pipeline Structure
################################################################################################

dag >> daily_segment_loader
