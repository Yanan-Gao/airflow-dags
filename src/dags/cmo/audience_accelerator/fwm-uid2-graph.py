from airflow import DAG
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta

from dags.cmo.utils.constants import ACRProviders
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.pipeline_config import PipelineConfig
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

################################################################################################
# Configs -- Change things here for different providers
################################################################################################

pipeline_name = "fwm-uid2-crosswalk"
job_start_date = datetime(2025, 6, 23, 8, 0)
job_schedule_interval = timedelta(days=1)
provider = ACRProviders.Fwm

# Gen date is -1 and then target date is -14 from gen date
target_to_run_offset = 14

gen_date = '{{ (logical_date).strftime(\"%Y-%m-%d\") }}'
target_date = f'{{{{ (logical_date - macros.timedelta(days={target_to_run_offset})).strftime(\"%Y-%m-%d\") }}}}'

config = PipelineConfig()

################################################################################################
# DAG
################################################################################################
dag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["acr", provider],
    retries=0,
    retry_delay=timedelta(minutes=15),
    depends_on_past=True,
    run_only_latest=False
)

adag: DAG = dag.airflow_dag


################################################################################################
# Steps - Check Target Dates
################################################################################################
def check_target_dates(**context):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()

    genDateMacro = (context['logical_date']).strftime("%Y-%m-%d")
    targetDateMacro = (context['logical_date'] + timedelta(days=-target_to_run_offset)).strftime("%Y-%m-%d")
    prefix = f"fwm/crosswalk-uid/gd={genDateMacro}/td={targetDateMacro}"

    if (cloud_storage.check_for_prefix(bucket_name="thetradedesk-useast-data-import", prefix=prefix)):
        print(f"------------ Found target file: {prefix}")
        return True

    print(f"------------ Didn't find the target file: {prefix}")
    return False


check_target_dates = OpTask(
    op=ShortCircuitOperator(task_id='check_target_dates', python_callable=check_target_dates, provide_context=True, dag=adag)
)

################################################################################################
# Steps
################################################################################################

uid2_graph_input = EmrClusterTask(
    name='fwm-uid2-graph-input',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={
        "Team": CMO.team.jira_team,
    },
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, instance_capacity=2),
    use_on_demand_on_timeout=False,
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations()
)
uid2_graph_input.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=uid2_graph_input.cluster_specs,
        name='FwmUidGraphInput',
        class_name="jobs.ctv.linear.acr.fwm.FwmUid2GraphInputProcessor",
        eldorado_config_option_pairs_list=[("genDate", gen_date), ("targetDate", target_date),
                                           ("outputPath", "s3://ttd-linear-tv-data/fwm")],
        timeout_timedelta=timedelta(minutes=90),
        executable_path=config.jar,
        additional_args_option_pairs_list=config.get_step_additional_configurations()
    )
)

uid2_graph_generator = EmrClusterTask(
    name='fwm-uid2-ttd-joined-graph',
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.TwoX),
    cluster_tags={
        "Team": CMO.team.jira_team,
    },
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=12),
    use_on_demand_on_timeout=False,
    enable_prometheus_monitoring=True,
    emr_release_label=config.emr_release_label,
    additional_application_configurations=config.get_cluster_additional_configurations(),
    retries=0
)
uid2_graph_generator.add_parallel_body_task(
    EmrJobTask(
        cluster_specs=uid2_graph_generator.cluster_specs,
        name='FwmUid2TtdGraphGeneration',
        class_name="jobs.ctv.linear.acr.fwm.FwmUid2GraphGeneration",
        eldorado_config_option_pairs_list=[("date", target_date), ("fwmGraphRoot", "s3://ttd-linear-tv-data/fwm"),
                                           ("ttd.FwmUid2CrosswalkDataSet.isInChain", "true")],
        timeout_timedelta=timedelta(minutes=90),
        executable_path=config.jar,
        additional_args_option_pairs_list=config.get_step_additional_configurations()
    )
)

################################################################################################
# Pipeline Structure
################################################################################################

dag >> check_target_dates >> uid2_graph_input >> uid2_graph_generator
