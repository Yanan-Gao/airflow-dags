from airflow import DAG
from datetime import datetime, timedelta

from dags.cmo.audience_accelerator.tasks.segment.samba.ctv_acr_samba_enrichment import CtvAcrSambaEnrichment
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################
pipeline_name = "sambaau"
job_start_date = datetime(2024, 8, 19, 8, 0)
job_schedule_interval = timedelta(days=1)
date_macro = '{{ (logical_date - macros.timedelta(days=5)).strftime(\"%Y-%m-%d\") }}'

config = AcrPipelineConfig(
    provider=ACRProviders.Samba,
    country=Country.AU,
    enriched_dataset=Datasources.samba.samba_enriched_v3(Country.AU),
    data_delay=5,  # The latency of Samba AU data delivery
    run_date=date_macro,
    frequency_aggregation_version=4,
    segment_enriched_version=2
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
check_incoming_data = OpTask(
    op=DatasetCheckSensor(
        datasets=[Datasources.samba.samba_au_impression_feed, Datasources.samba.samba_au_tradedesk_id_ip_mapping_v2],
        ds_date='{{ (logical_date - macros.timedelta(days=5)).to_datetime_string() }}',
        lookback=0,
        task_id='check_incoming_data',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 5,  # wait up to 5 hours
        dag=adag
    )
)
check_acr_segments = OpTask(
    op=DatasetRecencyOperator(
        task_id="check_segment_parquet_sync_files",
        datasets_input=[Datasources.sql.acr_provider_segments, Datasources.sql.acr_provider_brand_segments],
        lookback_days=0,
        cloud_provider=CloudProviders.aws,
        dag=adag
    )
)

################################################################################################
# Steps
################################################################################################
# tv activity and enrichment
samba_au_enrichment = CtvAcrSambaEnrichment(config=config, ignore_low_feed_match_rate=True).get_task()

################################################################################################
# Pipeline Structure
################################################################################################

dag >> check_acr_segments >> check_incoming_data >> samba_au_enrichment
