from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator, WeekDay
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.pipeline_config import PipelineConfig
from datasources.sources.experian_datasources import ExperianDatasources
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

# Configs
name = "ctv-experian-graph-processor"
job_start_date = datetime(2024, 10, 1, 0, 0)
job_schedule_interval = timedelta(days=1)
pipeline_config = PipelineConfig()

cluster_tags = {
    'Team': CMO.team.jira_team,
}

################################################################################################
# DAG
################################################################################################
dag: TtdDag = TtdDag(
    dag_id=name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["experian"],
    retries=1,
    retry_delay=timedelta(hours=3)
)

adag: DAG = dag.airflow_dag


################################################################################################
# Pipeline Dependencies
################################################################################################
def check_data(bucket: str, key: str):
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    if aws_storage.check_for_key(bucket_name=bucket, key=key):
        print(f's3://{bucket}/{key} exists')
        return True
    print(f's3://{bucket}/{key} does not exists')
    return False


is_open_graph_ready = OpTask(
    op=PythonSensor(
        dag=adag,
        task_id="wait_for_opengraph",
        python_callable=check_data,
        op_kwargs={
            'bucket': 'thetradedesk-useast-data-import',
            'key': 'sxd-etl/universal/nextgen_household/{{ (logical_date - macros.timedelta(days=1)).strftime(\"%Y-%m-%d\") }}/_SUCCESS',
        },
        poke_interval=60 * 60 * 3,  # poke every 3 hours
        timeout=60 * 60 * 20,  # wait for 20 hours (the graph is often created at the end of the day)
        mode='reschedule',
        # allow not updating it when open graph refresh. Experian graph refresh is only 2 days after
        soft_fail=True
    )
)

experian_graph_sensor = OpTask(
    op=PythonSensor(
        dag=adag,
        task_id="wait_for_graph_data",
        python_callable=check_data,
        op_kwargs={
            'bucket': 'thetradedesk-useast-data-import',
            'key': 'experian/upload/graph/thetradedesk_luid_ids_full_{{ logical_date.strftime(\"%Y%m%d\") }}_000000.gz.master.trigger',
        },
        poke_interval=60 * 60 * 3,  # poke every 3 hours
        timeout=60 * 60 * 24,  # wait for a day
        mode='reschedule',
        depends_on_past=True
    )
)

experian_optout_sensor = OpTask(
    op=PythonSensor(
        dag=adag,
        task_id="wait_for_opt_out_data",
        python_callable=check_data,
        op_kwargs={
            'bucket': 'thetradedesk-useast-data-import',
            'key': 'experian/upload/graph/thetradedesk_luid_optout_tapad_{{ logical_date.strftime(\"%Y%m%d\") }}_000000.gz',
        },
        poke_interval=60 * 60 * 3,  # poke every 3 hours
        timeout=60 * 60 * 24,  # wait for a day
        mode='reschedule'
    )
)

is_day_to_run_open_graph = OpTask(
    op=BranchDayOfWeekOperator(
        week_day=[WeekDay.SUNDAY],
        task_id='should_run_new_open_graph',
        follow_task_ids_if_true=[is_open_graph_ready.task_id],
        follow_task_ids_if_false=[],
        use_task_logical_date=True,
        dag=adag,
    )
)

is_day_to_run_experian_graph = OpTask(
    op=BranchDayOfWeekOperator(
        week_day=[WeekDay.TUESDAY],
        task_id='should_run_new_experian_graph',
        follow_task_ids_if_true=[experian_graph_sensor.task_id],
        follow_task_ids_if_false=[],
        use_task_logical_date=True,
        dag=adag,
    )
)

# Steps
# Experian graph processing
graph_input_processor_step = EmrClusterTask(
    name="graph_input_processor",
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.FourX, instance_capacity=6),
    enable_prometheus_monitoring=True,
    emr_release_label=pipeline_config.emr_release_label,
    additional_application_configurations=pipeline_config.get_cluster_additional_configurations()
)

graph_input_processor_step.add_parallel_body_task(
    EmrJobTask(
        name=graph_input_processor_step.name + "_step",
        class_name="jobs.ctv.experian.ExperianGraphInputProcessor",
        timeout_timedelta=timedelta(hours=2),
        eldorado_config_option_pairs_list=[("date", "{{ logical_date.strftime(\"%Y-%m-%d\") }}"), ("partitions", 500)],
        executable_path=pipeline_config.jar,
        additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations()
    )
)

# adbrain (open graph) x experian mapping job
experian_adbrain_open_graph_generation = EmrClusterTask(
    name="experian_adbrain_open_graph_generation",
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwelveX, instance_capacity=12),
    enable_prometheus_monitoring=True,
    emr_release_label=pipeline_config.emr_release_label,
    additional_application_configurations=pipeline_config.get_cluster_additional_configurations()
)

experian_adbrain_open_graph_generation.add_parallel_body_task(
    EmrJobTask(
        name='ExperianAdBrainGraphGeneration',
        class_name="jobs.ctv.experian.ExperianAdBrainGraphGeneration",
        eldorado_config_option_pairs_list=[("date", '{{ logical_date.strftime(\"%Y-%m-%d\") }}'),
                                           ("experianRootPath", ExperianDatasources.experian_path),
                                           ("experianProcessedPath", ExperianDatasources.experian_processed_path)],
        timeout_timedelta=timedelta(minutes=180),
        executable_path=pipeline_config.jar,
        additional_args_option_pairs_list=[("conf", "spark.sql.shuffle.partitions=1800")] +
        pipeline_config.get_step_additional_configurations()
    )
)

# Opt out processing
optout_input_processor = EmrClusterTask(
    name="optout_input_processor",
    master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=getFleetInstances(EmrInstanceClasses.MemoryOptimized, EmrInstanceSizes.TwoX, instance_capacity=1),
    enable_prometheus_monitoring=True,
    emr_release_label=pipeline_config.emr_release_label,
    additional_application_configurations=pipeline_config.get_cluster_additional_configurations()
)

optout_input_processor.add_parallel_body_task(
    EmrJobTask(
        name=optout_input_processor.name + "_step",
        class_name="jobs.ctv.experian.ExperianOptOutInputProcessor",
        timeout_timedelta=timedelta(hours=2),
        eldorado_config_option_pairs_list=[("date", "{{ logical_date.strftime(\"%Y-%m-%d\") }}")],
        executable_path=pipeline_config.jar,
        additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations()
    )
)

should_run_experian_open_graph = OpTask(
    op=EmptyOperator(task_id='should_run_experian_open_graph', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
)

# Structure
dag >> is_day_to_run_experian_graph >> experian_graph_sensor >> graph_input_processor_step >> should_run_experian_open_graph >> experian_adbrain_open_graph_generation
dag >> is_day_to_run_open_graph >> is_open_graph_ready >> should_run_experian_open_graph >> experian_adbrain_open_graph_generation

dag >> experian_optout_sensor >> optout_input_processor
