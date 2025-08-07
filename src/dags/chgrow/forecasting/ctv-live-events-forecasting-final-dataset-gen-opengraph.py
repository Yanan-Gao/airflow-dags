# from airflow import DAG
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import CHGROW

job_name = 'ctv-live-events-forecasting-final-dataset-generation-opengraph'
cluster_name = 'ctv-live-events-forecasting-final-dataset-generation-opengraph'
job_jar = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
job_class = 'com.thetradedesk.etlforecastjobs.preprocessing.liveevents.LiveEventsForecastingDataOpenGraphGeneration'
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
job_start_date = datetime(2025, 1, 11, 16, 0)
job_schedule_interval = timedelta(days=1)
job_slack_channel = CHGROW.channels_growth().alarm_channel
active_running_jobs = 1

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6g_16xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_4xlarge().with_fleet_weighted_capacity(8),
        R6g.r6g_8xlarge().with_fleet_weighted_capacity(16),
        R6g.r6g_12xlarge().with_fleet_weighted_capacity(24),
        R6g.r6g_16xlarge().with_fleet_weighted_capacity(32)
    ],
    on_demand_weighted_capacity=1664,
)

live_events_final_dataset_generation_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=CHGROW.channels_growth().sub_team,
    tags=["live events", "ctv", CHGROW.channels_growth().jira_team],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

live_events_final_dataset_generation_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags={"Team": CHGROW.channels_growth().jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True
)

step = EmrJobTask(
    name="join-datasets-and-write",
    class_name=job_class,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}")],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4),
    cluster_specs=live_events_final_dataset_generation_cluster.cluster_specs,
    configure_cluster_automatically=False,
)

live_events_final_dataset_generation_cluster.add_parallel_body_task(step)

dag: DAG = live_events_final_dataset_generation_dag.airflow_dag


def raise_exception_on_task_failure():
    raise Exception("One task has failed. Entire DAG run will be reported as a failure")


# Python operator that triggers raise_exception_on_task_failure if any step failed
final_dag_status_step = OpTask(
    op=PythonOperator(
        task_id="final_dag_status",
        provide_context=True,
        python_callable=raise_exception_on_task_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
        dag=dag
    )
)

check_hhs_avails_exist = OpTask(
    op=DatasetCheckSensor(
        datasets=[Datasources.avails.household_sampled_identity_deal_agg_hourly("high", "openGraphAdBrain").with_check_type("day")],
        ds_date="{{ logical_date.to_datetime_string() }}",
        poke_interval=60 * 10,
        generate_task_id=True,
        dag=dag
    )
)

live_events_final_dataset_generation_dag >> check_hhs_avails_exist
check_hhs_avails_exist >> live_events_final_dataset_generation_cluster
live_events_final_dataset_generation_cluster >> final_dag_status_step
