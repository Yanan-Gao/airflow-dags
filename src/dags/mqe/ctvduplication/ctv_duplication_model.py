from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
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
from datasources.sources.avails_datasources import AvailsDatasources

upstream_datasets_list = [
    AvailsDatasources.publisher_agg_daily_dataset,
    AvailsDatasources.identity_and_deal_agg_hourly_dataset.with_check_type("day").with_region("us-east-1")
]

job_environment = TtdEnvFactory.get_from_system()

dag = TtdDag(
    dag_id="ctv-duplicationrate-model",
    start_date=datetime(2024, 11, 23),
    schedule_interval="0 5 * * *",
    retries=1,
    max_active_runs=2,
    retry_delay=timedelta(minutes=60),
    tags=["MQE"],
    slack_tags=slack_groups.mqe.name,
    slack_channel=slack_groups.mqe.alarm_channel,
    enable_slack_alert=True
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_capacity = 400
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_8xlarge().with_ebs_size_gb(2048).with_fleet_weighted_capacity(10),
        M5.m5_12xlarge().with_ebs_size_gb(3072).with_fleet_weighted_capacity(15),
        M5.m5_16xlarge().with_ebs_size_gb(4096).with_fleet_weighted_capacity(20)
    ],
    on_demand_weighted_capacity=core_fleet_capacity,
)

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5
aws_region = "us-east-1"

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

cluster_task = EmrClusterTask(
    name="ctv-duplicationrate-model-job",
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
gating_type_id = 2000512  # dbo.fn_Enum_GatingType_ImportCTVDuplicationRateStatistics()
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
execution_date_time = '{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}'

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar"
job_class_name = "jobs.duplication.CtvDuplicationRate"
date_macro = """{{ data_interval_start.to_date_string() }}"""
eldorado_config_option_pairs_list = [("date", date_macro), ("env", job_environment.execution_env)]

spark_options_list = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
                      ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                      ("conf", "spark.driver.memory=100G"), ("conf", "spark.driver.cores=16"),
                      ("conf", "spark.sql.shuffle.partitions=5000")]

job_task = EmrJobTask(
    name="model-generation",
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
    task_id='check_avails_data_readiness',
    dag=dag.airflow_dag,
    ds_date="{{ data_interval_start.to_datetime_string() }}",
    lookback=1,  # check on previous day's dataset as well, as that may also get processed in this run
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
            'grain': TaskBatchGrain_Daily,
            'dateTimeToOpen': execution_date_time
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
