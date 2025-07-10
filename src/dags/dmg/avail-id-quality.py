from ttd.el_dorado.v2.base import TtdDag
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

date_macro = "{{ (dag_run.start_date + macros.timedelta(days=-1)).strftime(\"%Y-%m-%d\") }}"
job_jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/el-dorado-assembly.jar"
env = TtdEnvFactory.get_from_system()

logworkflow_connection = 'lwdb'
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
gating_type_ids = [
    2000359, 2000360, 2000361, 2000362
]  # dbo.fn_Enum_GatingType_ImportAvailsIdQualityByDeal(), dbo.fn_Enum_GatingType_ImportAvailsIdQualityBySSP(), dbo.fn_Enum_GatingType_ImportAvailsIdQualityByPublisher(), dbo.fn_Enum_GatingType_ImportAvailsIdQualityByProperty()

java_options_list = [("date", date_macro), ("ttd.env", env)]

spark_options_list = [('conf', 'spark.dynamicAllocation.enabled=true'), ("conf", "spark.speculation=false"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                      ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.sql.adaptive.enabled=true"),
                      ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"), ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
                      ("conf", "maximizeResourceAllocation=true")]

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C5.c5d_4xlarge().with_fleet_weighted_capacity(4),
        C5.c5d_9xlarge().with_fleet_weighted_capacity(9),
        C5.c5d_12xlarge().with_fleet_weighted_capacity(12),
        C5.c5d_18xlarge().with_fleet_weighted_capacity(18)
    ],
    on_demand_weighted_capacity=60
)

dag = TtdDag(
    dag_id="dmg-avails-id-quality-daily",
    start_date=datetime(2024, 5, 14, 3) - timedelta(days=1),
    schedule_interval="0 3 * * *",  # Interval scheduled to run 3:00 AM UTC daily
    slack_channel=DEAL_MANAGEMENT.team.alarm_channel,
    tags=[DEAL_MANAGEMENT.team.jira_team],
    run_only_latest=True
)
adag = dag.airflow_dag

cluster_task = EmrClusterTask(
    name="AvailsIdQualityDaily_cluster",
    cluster_tags={"Team": DEAL_MANAGEMENT.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True
)

job_task = EmrJobTask(
    name="AvailsIdQuality_step",
    class_name="jobs.markets.idquality.AvailsIdQuality",
    executable_path=job_jar,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_options_list,
    configure_cluster_automatically=True
)


def open_lwdb_gate(**context):
    task_date: datetime = context["logical_date"]
    dt = task_date.replace(hour=0, minute=0, second=0, microsecond=0)
    log_start_time = dt.strftime("%Y-%m-%d %H:00:00")
    for gating_type_id in gating_type_ids:
        ExternalGateOpen(
            mssql_conn_id=logworkflow_connection,
            sproc_arguments={
                'gatingType': gating_type_id,
                'grain': TaskBatchGrain_Daily,
                'dateTimeToOpen': log_start_time
            }
        )


avail_idquality_open_lwdb_gate = OpTask(
    op=PythonOperator(task_id='avail_idquality_open_lwdb_gate', python_callable=open_lwdb_gate, provide_context=True)
)

cluster_task.add_parallel_body_task(job_task)
dag >> cluster_task >> avail_idquality_open_lwdb_gate
