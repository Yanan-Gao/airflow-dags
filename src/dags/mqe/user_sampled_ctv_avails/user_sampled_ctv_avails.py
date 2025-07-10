from airflow.operators.python import PythonOperator

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from datetime import datetime, timedelta

from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

RUNTIME_DATE = '{{ (data_interval_end).strftime("%Y-%m-%dT%H:00:00") }}'
JOB_DATE = '{{ (data_interval_start).strftime("%Y-%m-%d") }}'
JAR_PATH = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar"
dag_name = 'user-sampled-ctv-avails'
logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
gating_type_id = 2000517  # dbo.fn_Enum_GatingType_ImportUserSampledCTVAvails()
TaskBatchGrain_Hourly = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()
execution_date_time = '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'
job_environment = TtdEnvFactory.get_from_system()
####################################################################################################################
# DAG
####################################################################################################################

user_sampled_ctv_avails_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 11, 29, 5, 0),
    schedule_interval='0 */1 * * *',
    retries=0,
    max_active_runs=1,
    tags=['MQE'],
    slack_tags=slack_groups.mqe.name,
    slack_channel=slack_groups.mqe.alarm_channel,
    enable_slack_alert=True
)

dag = user_sampled_ctv_avails_dag.airflow_dag

###########################################
# Check Dependencies
###########################################

check_avails_upstream_completed = OpTask(
    op=DatasetCheckSensor(
        datasets=[Datasources.avails.avails_7_day.with_check_type(check_type="hour")],
        ds_date='{{data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}',
        poke_interval=int(timedelta(minutes=5).total_seconds()),
        timeout=int(timedelta(hours=12).total_seconds()),
        task_id='avails_dataset_check',
        cloud_provider=CloudProviders.aws,
    )
)

####################################################################################################################
# clusters
####################################################################################################################

# EMR version to run
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(100).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_8xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(32),
        M5.m5_12xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(48),
        M5.m5_16xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(64),
        M5.m5_24xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(96)
    ],
    on_demand_weighted_capacity=384
)

spark_options_list = [
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
    ("conf", "spark.yarn.maxAppAttempts=2"),
]

cluster = EmrClusterTask(
    name="user-sampled-ctv-avails-job",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": slack_groups.mqe.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    emr_release_label=emr_release_label
)

user_sampled_ctv_avails_task = EmrJobTask(
    name="userSampledCTVAvailsTask",
    class_name="jobs.usersampledctvavails.UserSampledCTVAvails",
    executable_path=JAR_PATH,
    additional_args_option_pairs_list=spark_options_list,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[("runTime", RUNTIME_DATE), ("date", JOB_DATE),
                                       ('datetime', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                       ("env", job_environment.execution_env)]
)

cluster.add_parallel_body_task(user_sampled_ctv_avails_task)

vertica_import_open_gate = PythonOperator(
    python_callable=ExternalGateOpen,
    provide_context=True,
    op_kwargs={
        'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
        'sproc_arguments': {
            'gatingType': gating_type_id,
            'grain': TaskBatchGrain_Hourly,
            'dateTimeToOpen': execution_date_time
        }
    },
    task_id="vertica_import_open_gate",
    retries=3,
    retry_delay=timedelta(minutes=5)
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
user_sampled_ctv_avails_dag >> check_avails_upstream_completed >> cluster
# open the LWF gate after model data is ready
user_sampled_ctv_avails_task.last_airflow_op() >> vertica_import_open_gate
vertica_import_open_gate >> final_dag_status_step
