# from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.tasks.op import OpTask
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import CHGROW
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.ttdenv import TtdEnvFactory

job_name = 'do-not-air-preferences-mapping'
cluster_name = 'do-not-air-preferences-mapping-cluster'
job_jar = 's3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/channels/chgrow/channelsgrowthspark-assembly.jar'
job_class = 'com.thetradedesk.channels.chgrow.channelsgrowthspark.pipelines.donotairpreferences.GenerateDoNotAirMapping'
log_uri = 's3://ttd-identity/datapipeline/logs/airflow'
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

job_start_date = datetime(2025, 1, 20, 6, 0)
job_schedule_interval = timedelta(days=1)
job_slack_channel = CHGROW.channels_growth().alarm_channel
active_running_jobs = 1

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6g_4xlarge().with_ebs_size_gb(200).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6gd_4xlarge().with_fleet_weighted_capacity(16),
        M6g.m6gd_8xlarge().with_fleet_weighted_capacity(32),
        M6g.m6gd_12xlarge().with_fleet_weighted_capacity(48),
        M6g.m6gd_16xlarge().with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=96
)

do_not_air_preferences_mapping_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=CHGROW.channels_growth().sub_team,
    tags=["content library", "do not air", CHGROW.channels_growth().jira_team],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

do_not_air_preferences_mapping_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags={"Team": CHGROW.channels_growth().jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True
)

step = EmrJobTask(
    name=job_name,
    class_name=job_class,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}")],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=2)
)
do_not_air_preferences_mapping_cluster.add_parallel_body_task(step)

do_not_air_preferences_mapping_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': 'lwdb' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'sandbox-lwdb',
            'sproc_arguments': {
                'gatingType': 2000537,  # dbo.fn_enum_GatingType_ImportDoNotAirPreferenceMapping()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': '{{ logical_date.strftime("%Y-%m-%d") }}'
            }
        },
        task_id="do_not_air_preferences_mapping_import_open_gate",
        retries=3,
        retry_delay=timedelta(minutes=60),
        retry_exponential_backoff=True
    )
)

dag = do_not_air_preferences_mapping_dag.airflow_dag

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

do_not_air_preferences_mapping_dag >> do_not_air_preferences_mapping_cluster
do_not_air_preferences_mapping_cluster >> do_not_air_preferences_mapping_import_open_gate
do_not_air_preferences_mapping_import_open_gate >> final_dag_check
