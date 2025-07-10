from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
job_environment = TtdEnvFactory.get_from_system()
DATE = '{{ds}}'

ttddag = TtdDag(
    dag_id="conversion-lift-benchmarks",
    start_date=datetime(2024, 9, 12, 10, 30),
    schedule_interval='0 20 * * *',  # run on 0:20 UTC every day
    depends_on_past=False,
    slack_channel=slack_groups.MEASURE_TASKFORCE_LIFT.team.alarm_channel,
    tags=['measurement'],
    run_only_latest=True,
    retries=2
)

spark_options_list = [("executor-memory", "16G"), ("driver-memory", "16G"), ("driver-cores", "4"), ("executor-cores", "4"),
                      ("conf", "spark.kryoserializer.buffer.max=2047m"), ("conf", "spark.driver.maxResultSize=0"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.sql.shuffle.partitions=12800"), ("conf", "spark.default.parallelism=12800")]
options_list = [('date', DATE), ('shouldRunRetroactively', 'false')]

cluster_tags = {
    "process": "conversion-lift-timeseries-data",
    "Team": slack_groups.MEASURE_TASKFORCE_LIFT.team.jira_team,
}

exec_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar'

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[EmrInstanceType("i3.4xlarge", 16, 122).with_ebs_size_gb(64).with_fleet_weighted_capacity(4)],
    on_demand_weighted_capacity=30
)

cluster = EmrClusterTask(
    name="insights-liftcalc-benchmarks",
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    enable_prometheus_monitoring=True
)

step = EmrJobTask(
    name="insights-liftcalc-benchmarks-step",
    class_name="com.thetradedesk.jobs.insights.conversionlift.Benchmarks",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=options_list,
    configure_cluster_automatically=True,
    executable_path=exec_jar,
    timeout_timedelta=timedelta(hours=5)
)

# Trigger Vertica import via DataMover by opening the Gate
lift_benchmarks_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': 2000223,  # dbo.fn_enum_GatingType_ImportConversionLiftBenchmarks()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': DATE
            }
        },
        task_id="lift_benchmarks_vertica_import_open_gate",
    )
)

cluster.add_sequential_body_task(step)
ttddag >> cluster >> lift_benchmarks_vertica_import_open_gate
DAG = ttddag.airflow_dag
