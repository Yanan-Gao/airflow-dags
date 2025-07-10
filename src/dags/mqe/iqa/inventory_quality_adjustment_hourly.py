from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

from ttd.slack import slack_groups
from ttd.ttdenv import TtdEnvFactory

from datetime import timedelta, datetime

# s3://ttd-identity/datapipeline/prod/bidrequestadgroupsampledinternalauctionresultslog/v=1/date=20240718/hour=0/
bidrequest_ial_data = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    data_name="bidrequestadgroupsampledinternalauctionresultslog",
    hour_format="hour={hour}",
    version=1,
    env_aware=True,
    success_file=None
)

upstream_datasets_list = [bidrequest_ial_data.with_check_type("hour")]

job_environment = TtdEnvFactory.get_from_system()

dag = TtdDag(
    dag_id="inventory-quality-adjustment-hourly",
    start_date=datetime(2025, 3, 19),
    schedule_interval=timedelta(hours=1),
    retries=1,
    max_active_runs=2,
    retry_delay=timedelta(minutes=10),
    tags=["MQE"],
    slack_tags=slack_groups.mqe.name,
    slack_channel=slack_groups.mqe.alarm_channel,
    enable_slack_alert=True
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_2xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        M5.m5_4xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=6
)
spark_options_list = [
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
    ("conf", "spark.yarn.maxAppAttempts=2"),
]
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
aws_region = "us-east-1"

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

cluster_task = EmrClusterTask(
    name="inventory-quality-adjustment-hourly-job",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.mqe.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    environment=job_environment,
    emr_release_label=emr_release_label,
    additional_application_configurations=[additional_application_configurations],
    region_name=aws_region,
)

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
gating_type_id = 2000572  # dbo.fn_Enum_GatingType_ImportInventoryQualityAdjustmentData()
TaskBatchGrain_Hourly = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar"
job_class_name = "jobs.iqa.InventoryQualityAdjustmentTransformer"
processing_time = '{{ (logical_date).strftime(\"%Y-%m-%dT%H:00:00\") }}'
data_sensor_time = '{{ (logical_date).strftime(\"%Y-%m-%d %H:00:00\") }}'
eldorado_config_option_pairs_list = [("processingTime", processing_time), ("env", job_environment.execution_env)]

job_task = EmrJobTask(
    name="inventory-quality-adjustment-generation",
    class_name=job_class_name,
    executable_path=jar_path,
    timeout_timedelta=timedelta(hours=20),
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task

wait_complete = DatasetCheckSensor(
    task_id='check_ial_data_readiness',
    dag=dag.airflow_dag,
    ds_date=data_sensor_time,
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    datasets=upstream_datasets_list,
    timeout=60 * 60 * 6  # 6 hours
)

# we won't create the cluster until all dependent upstream datasets are created
wait_complete >> cluster_task.first_airflow_op()

vertica_import_open_gate = PythonOperator(
    python_callable=ExternalGateOpen,
    provide_context=True,
    op_kwargs={
        'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
        'sproc_arguments': {
            'gatingType': gating_type_id,
            'grain': TaskBatchGrain_Hourly,
            'dateTimeToOpen': processing_time
        }
    },
    task_id="vertica_import_open_gate",
    retries=3,
    retry_delay=timedelta(minutes=5)
)

# open the LWF gate after model data is ready
job_task.last_airflow_op() >> vertica_import_open_gate

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
adag = dag.airflow_dag
