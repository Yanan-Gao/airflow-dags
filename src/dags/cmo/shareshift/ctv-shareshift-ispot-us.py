from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from dags.cmo.utils.pipeline_config import PipelineConfig
from dags.cmo.utils.constants import ACRProviders, Country
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.shareshift.shareshift_tasks import ShareShiftTasks
from datasources.datasources import Datasources
from datasources.sources.ispot_datasources import ISpotDatasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

job_start_date = datetime(2024, 8, 21, 0, 0)
job_schedule_interval = "15 3 * * *"

ttd_dag = TtdDag(
    dag_id="ctv-shareshift-ispot-us",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["ispot", "shareshift"],
    retries=1
)

dag: DAG = ttd_dag.airflow_dag

provider = ACRProviders.ISpot
country = Country.US

pipeline_config = PipelineConfig()

################################################################################################
# Data Transfer from iSpot bucket
################################################################################################
data_transfer_task = DatasetTransferTask(
    name='transfer-ispot-linear-data',
    dataset=ISpotDatasources.linear_source_dataset,
    dst_dataset=ISpotDatasources.linear_target_dataset,
    src_cloud_provider=CloudProviders.aws,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=ISpotDatasources.linear_target_dataset.get_partitioning_args(
        ds_date='{{ (logical_date + macros.timedelta(days=-4)).strftime(\"%Y-%m-%d\") }}'
    )
)

####################################################################################################################
# Steps
####################################################################################################################
# tv activity and enrichment
enrich_cluster_task = EmrClusterTask(
    name="ispot_enrichment_cluster",
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags={"Team": CMO.team.jira_team},
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=8),
    emr_release_label=pipeline_config.emr_release_label,
    additional_application_configurations=pipeline_config.get_cluster_additional_configurations(),
    enable_prometheus_monitoring=True
)

job_task = EmrJobTask(
    name="ispot_enrichment_job",
    class_name="jobs.ctv.linear.acr.ispot.ISpotEnrichment",
    eldorado_config_option_pairs_list=[("emr.version", "emr-6.7.0"),
                                       ("date", '{{ (logical_date + macros.timedelta(days=-4)).strftime(\"%Y-%m-%d\") }}'),
                                       ("country", country), ("provider", provider),
                                       ("outputPath", ISpotDatasources.ispot_enriched_dataset.get_root_path()),
                                       ("tvActivityPath", Datasources.segment(provider).get_tv_activity_dataset(country).get_root_path())],
    additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations(),
    timeout_timedelta=timedelta(minutes=60),
    executable_path=pipeline_config.jar,
    cluster_specs=enrich_cluster_task.cluster_specs
)
enrich_cluster_task.add_parallel_body_task(job_task)

######################################################
# OpenGraph migrated
######################################################
shareshift = ShareShiftTasks(
    ttd_dag,
    pipeline_config,
    provider,
    country,
    start_date='{{ (logical_date + macros.timedelta(days=-28)).replace(day=1).strftime("$format$") }}',
    end_date='{{ logical_date.replace(day=1).strftime("$format$") }}',
    ds_version=2
)
shareshift_tasks_start = OpTask(op=EmptyOperator(task_id='shareshift_tasks_start'))

imps_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
imps_additional_eldorado_config = [("useTopXNetworksAsCategory", 100)]
imps_worker_cluster = getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=10)
imps_task = shareshift.get_impressions_task(imps_master_cluster, imps_worker_cluster, imps_additional_eldorado_config)

weights_task = shareshift.get_weights_job_task(limit_ephemeral_storage='2G', request_memory='16G', limit_memory='20G')

sampling_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX)
sampling_worker_cluster = getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX, instance_capacity=2)
sampling_additional_eldorado_config = [("hhSampleEnabled", "true"), ("hhSamplePercent", 0.2)]
sampling_task = shareshift.get_sampling_task(sampling_master_cluster, sampling_worker_cluster, sampling_additional_eldorado_config)

corrections_master_cluster = getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX)
corrections_worker_cluster = getFleetInstances(
    EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwelveX, instance_capacity=6
)  # runtime doesn't improve with increasing instances
corrections_additional_eldorado_config = [("hhSampleEnabled", "true"), ("hhSamplePercent", 0.2)]
corrections_task = shareshift.get_corrections_task(
    corrections_master_cluster, corrections_worker_cluster, corrections_additional_eldorado_config
)

# Run on the 4th day of the month (enrich data lag is 4 days)
run_ss_branch = shareshift.get_run_shareshift_branch_task(4, 'shareshift_tasks_start')

ttd_dag >> data_transfer_task >> enrich_cluster_task >> run_ss_branch
run_ss_branch >> shareshift_tasks_start >> imps_task >> weights_task >> sampling_task >> corrections_task
