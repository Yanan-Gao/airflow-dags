from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

# Config values
job_name = 'ctv-combined-sampled-avails'
cluster_name = 'ctv-combined-sampled-avails-etl'
job_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-tv-assembly.jar'
job_class = 'jobs.ctv.avails.CombinedSampledAvailsETL'

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2_1
ctv_tag = 'ctv_forecasting_tool'

job_start_date = datetime(2024, 10, 16, 10, 00)
job_schedule_interval = timedelta(hours=1)
job_slack_channel = PFX.team.alarm_channel
active_running_jobs = 10

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

executor_cores = 2048

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5d.r5d_8xlarge().with_fleet_weighted_capacity(32),
        R5d.r5d_12xlarge().with_fleet_weighted_capacity(48),
        R5d.r5d_16xlarge().with_fleet_weighted_capacity(64),
        R5d.r5d_24xlarge().with_fleet_weighted_capacity(96),
    ],
    on_demand_weighted_capacity=executor_cores
)

avails_etl = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=PFX.dev_ctv_forecasting_tool().sub_team,
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=2,
    retry_delay=timedelta(minutes=30)
)

dag = avails_etl.airflow_dag

cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={
        "Team": PFX.team.jira_team,
        "SubTeam": ctv_tag
    },
    enable_prometheus_monitoring=True,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True
)

avails_processing_step = EmrJobTask(
    name=job_name,
    class_name=job_class,
    eldorado_config_option_pairs_list=[("runTime", "{{ logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"), ("protoNumPartitions", "10000")],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4),
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag, name="final_dag_status", trigger_rule=TriggerRule.ONE_FAILED))

cluster.add_parallel_body_task(avails_processing_step)

avails_etl >> cluster >> final_dag_status_step
