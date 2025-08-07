from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from dags.idnt.identity_helpers import DagHelpers
from dags.cmo.audience_accelerator.tasks.segment.samba.ctv_acr_samba_enrichment import CtvAcrSambaEnrichment
from dags.cmo.utils.acr_pipeline_config import AcrPipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.shareshift.shareshift_tasks import ShareShiftTasks
from datasources.datasources import Datasources
from datasources.sources.samba_datasources import SambaDatasources
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################

pipeline_name = "ctv-shareshift-samba-ca"
job_start_date = datetime(2024, 8, 21, 15, 15)
job_schedule_interval = timedelta(days=1)
country = Country.CA
provider = ACRProviders.Samba
data_delay = 1  # The latency of Samba CA data delivery

pipeline_config = AcrPipelineConfig(
    provider=provider,
    country=country,
    enriched_dataset=Datasources.samba.samba_enriched_v3(country),
    data_delay=data_delay,
    run_date=f'{{{{ (logical_date + macros.timedelta(days=-{data_delay})).strftime(\"%Y-%m-%d\") }}}}',
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
    tags=[pipeline_config.provider, "shareshift"],
    retries=1,
    retry_delay=timedelta(hours=1)
)
adag: DAG = dag.airflow_dag

################################################################################################
# Pipeline Dependencies
################################################################################################

wait_for_samba_feed = DagHelpers.check_datasets(
    [SambaDatasources.samba_ca_impression_feed],
    ds_date=f'{{{{ (logical_date + macros.timedelta(days=-{data_delay})).strftime(\"%Y-%m-%d %H:%M:%S\") }}}}',
    poke_minutes=60,
    timeout_hours=12
)

wait_for_samba_mapping = DagHelpers.check_datasets(
    [SambaDatasources.samba_ca_tradedesk_id_ip_mapping],
    ds_date=f'{{{{ (logical_date + macros.timedelta(days=-{data_delay})).strftime(\"%Y-%m-%d %H:%M:%S\") }}}}',
    poke_minutes=60,
    timeout_hours=12
)

################################################################################################
# Steps
################################################################################################
# tv activity and enrichment
samba_ca_enrichment_task = CtvAcrSambaEnrichment(
    config=pipeline_config, ignore_low_feed_match_rate=True
).get_task(additional_spark_config_list=[("conf", "spark.sql.legacy.timeParserPolicy=Legacy")])

#####################################################
# Shareshift steps (open graph)
#####################################################
shareshift = ShareShiftTasks(
    dag,
    pipeline_config,
    provider,
    country,
    start_date='{{ (logical_date + macros.timedelta(days=-28)).replace(day=1).strftime("$format$") }}',
    end_date='{{ logical_date.replace(day=1).strftime("$format$") }}',
    ds_version=2
)

imps_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
imps_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.FourX, instance_capacity=5)
imps_additional_eldorado_config = [
    ("enrichedPath", Datasources.samba.samba_enriched_v3(Country.CA).get_root_path()),
    ("useTopXNetworksAsCategory", 100)  # for now we take all networks, Samba CA has < 70 networks
]
imps_networks_task = shareshift.get_impressions_task(imps_master_cluster, imps_worker_cluster, imps_additional_eldorado_config)

weights_job_task = shareshift.get_weights_job_task()

corrections_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX)
corrections_worker_cluster = getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=3)
corrections_task = shareshift.get_corrections_task(corrections_master_cluster, corrections_worker_cluster)

optout_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
optout_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=1)
optout_task = shareshift.get_optout_task(optout_master_cluster, optout_worker_cluster)

shareshift_tasks_start = OpTask(op=EmptyOperator(task_id='shareshift_tasks_start'))
run_ss_branch = shareshift.get_run_shareshift_branch_task(2, 'shareshift_tasks_start')

################################################################################################
# Pipeline Structure
################################################################################################

dag >> wait_for_samba_feed >> samba_ca_enrichment_task >> run_ss_branch
dag >> wait_for_samba_mapping >> samba_ca_enrichment_task
run_ss_branch >> shareshift_tasks_start >> imps_networks_task >> weights_job_task >> corrections_task >> optout_task
