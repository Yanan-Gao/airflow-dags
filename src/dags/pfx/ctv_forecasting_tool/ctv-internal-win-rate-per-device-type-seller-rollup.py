from airflow import DAG
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from dags.forecast.sketches.randomly_sampled_avails.constants import ETL_BASED_FORECASTS_JAR_PATH
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

job_name = 'ctv-forecasting-tool-internal-win-rate-device-type-seller-rollup'
cluster_name = 'ctv_forecasting_tool_internal_win_rate_device_type_rollup_cluster'
job_jar = ETL_BASED_FORECASTS_JAR_PATH
job_class = 'com.thetradedesk.etlforecastjobs.preprocessing.internalwinrate.InternalWinRatePerDeviceTypeSellerRollupDaily'
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2
ctv_tag = 'ctv_forecasting_tool'

job_start_date = datetime(2024, 10, 29, 3, 00)
job_schedule_interval = timedelta(days=1)
job_slack_channel = PFX.team.alarm_channel
active_running_jobs = 1

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6gd_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6gd_2xlarge().with_fleet_weighted_capacity(16),
        M6g.m6gd_4xlarge().with_fleet_weighted_capacity(32),
    ],
    on_demand_weighted_capacity=48
)

internal_win_rate_device_type_rollup_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=PFX.dev_ctv_forecasting_tool().sub_team,
    tags=["internal_win_rate_device_type", ctv_tag],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

internal_win_rate_device_type_rollup_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags={
        "Team": PFX.team.jira_team,
        "SubTeam": ctv_tag
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True
)

step = EmrJobTask(
    name=job_name,
    class_name=job_class,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("rollupLength", 28)],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4)
)

internal_win_rate_device_type_rollup_cluster.add_parallel_body_task(step)

adag: DAG = internal_win_rate_device_type_rollup_dag.airflow_dag

# Python operator that triggers raise_exception_on_task_failure if any step failed
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(
    name="final_dag_status",
    dag=adag,
    trigger_rule=TriggerRule.ONE_FAILED,
))

internal_win_rate_device_type_rollup_dag >> internal_win_rate_device_type_rollup_cluster
internal_win_rate_device_type_rollup_cluster >> final_dag_status_step
