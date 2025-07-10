from datetime import datetime

from airflow.utils.trigger_rule import TriggerRule

from datasources.sources.ctv_datasources import CtvDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask

job_schedule_interval = "0 10 * * *"
job_start_date = datetime(2025, 4, 30, 6)

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2_1

dag = TtdDag(
    dag_id="ctv-forecasting-tool-daily-avail-counts-v3",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_groups.PFX.team.alarm_channel,
    slack_tags=slack_groups.PFX.dev_ctv_forecasting_tool().sub_team,
    tags=[slack_groups.PFX.team.name],
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_xlarge().with_fleet_weighted_capacity(1),
        R5.r5_2xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_4xlarge().with_fleet_weighted_capacity(2),
        R5d.r5d_8xlarge().with_fleet_weighted_capacity(4),
    ],
    on_demand_weighted_capacity=8
)

cluster_task = EmrClusterTask(
    name="ctv-forecasting-tool-daily-avail-counts-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.PFX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True
)

job_task = EmrJobTask(
    name="ctv-forecasting-tool-daily-avail-counts-task",
    class_name="com.thetradedesk.etlforecastjobs.ctvforecastingtool.cpmwinrateestimation.DailyAvailCountsJobV3",
    executable_path="s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}")],
    cluster_specs=cluster_task.cluster_specs,
)

cluster_task.add_parallel_body_task(job_task)

input_avails_dependency = OpTask(
    op=DatasetCheckSensor(
        task_id="filtered_aggregated_avails_dataset_input_check",
        datasets=[CtvDatasources.daily_agg_filtered_avails_v3],
        ds_date="{{ logical_date.strftime('%Y-%m-%d 00:00:00') }}",
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 4,  # wait 4 hours
    )
)

# Ensure that the dataset has been generated
generated_dataset_check = OpTask(
    op=DatasetCheckSensor(
        task_id="daily_avails_counts_generated_check",
        datasets=[CtvDatasources.daily_avails_counts_v3],
        ds_date="{{ logical_date.strftime('%Y-%m-%d 00:00:00') }}",
        raise_exception=True  # Raise an error straight away if the dataset hasn't been generated
    )
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag, name="final_dag_status", trigger_rule=TriggerRule.ONE_FAILED))

dag >> input_avails_dependency >> cluster_task >> generated_dataset_check >> final_dag_status_step
