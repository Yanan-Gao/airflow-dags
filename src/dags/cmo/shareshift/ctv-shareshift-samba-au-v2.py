"""
Monthly job to process Samba impression, demographics data, and generate weights and corrections
for ShareShift ltv prediction
"""
from datetime import datetime, timedelta

from airflow import DAG
from dags.cmo.utils.pipeline_config import PipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, getFleetInstances, getMasterFleetInstances
from dags.cmo.shareshift.shareshift_tasks import ShareShiftTasks
from datasources.datasources import Datasources
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import CMO

pipeline_name = "ctv-shareshift-samba-au-v2"
job_start_date = datetime(2024, 7, 3, 0, 0)
job_schedule_interval = "0 15 3 * *"

pipeline_config = PipelineConfig()
"""
 Configurable values
"""
country = Country.AU
provider = ACRProviders.Samba

dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["samba", "shareshift"],
    retries=1,
    retry_delay=timedelta(hours=1)
)
adag: DAG = dag.airflow_dag

################################################################################################
# Steps
################################################################################################

#####################################################
# Shareshift steps (open graph)
#####################################################
shareshift = ShareShiftTasks(
    dag,
    pipeline_config,
    provider,
    country,
    start_date='{{ logical_date.replace(day=1).strftime("$format$") }}',
    end_date='{{ (logical_date + macros.timedelta(days=31)).replace(day=1).strftime("$format$") }}',
    ds_version=2
)

imps_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
imps_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.FourX, instance_capacity=5)
imps_additional_eldorado_config = [("enrichedPath", Datasources.samba.samba_enriched_v3(Country.AU).get_root_path())]
imps_networks_sub_dag_task = shareshift.get_impressions_task(imps_master_cluster, imps_worker_cluster, imps_additional_eldorado_config)

weights_job_task = shareshift.get_weights_job_task()

corrections_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX)
corrections_worker_cluster = getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=3)
corrections_sub_dag_task = shareshift.get_corrections_task(corrections_master_cluster, corrections_worker_cluster)

optout_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
optout_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=1)
optout_sub_dag_task = shareshift.get_optout_task(optout_master_cluster, optout_worker_cluster)

dag >> imps_networks_sub_dag_task >> weights_job_task >> corrections_sub_dag_task >> optout_sub_dag_task
