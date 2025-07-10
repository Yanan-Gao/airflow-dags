import copy
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.compute_optimized.c6g import C6g
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask

dag_name = "perf-automation-diagnosis-etl"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

###############################################################################
# DAG
###############################################################################

# The top-level dag
diagnosis_etl_dag = TtdDag(
    dag_id="perf-automation-diagnosis-etl",
    start_date=datetime(2024, 11, 27),
    schedule_interval=timedelta(hours=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    max_active_runs=4,
    retry_delay=timedelta(minutes=5),
    enable_slack_alert=False,
    tags=["AUDAUTO"]
)

dag = diagnosis_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
internalauctionresultslog_dataset = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    data_name="internalauctionresultslog",
    version=1,
    hour_format="hour={hour}",
    success_file=None,
).with_check_type(check_type="hour")

adgroup_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="adgroup",
    version=1,
    env_aware=False,
    success_file=None,
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='internalauctionresultslog_data_available',
        datasets=[internalauctionresultslog_dataset, adgroup_dataset],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 5,
    )
)

###############################################################################
# clusters
###############################################################################
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C6g.c6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
    ],
    on_demand_weighted_capacity=20,
)

diagnosis_etl_cluster_task = EmrClusterTask(
    name=dag_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    cluster_tags={'Team': AUDAUTO.team.jira_team},
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300,
    additional_application_configurations=application_configuration
)

###############################################################################
# steps
###############################################################################
spark_options_list = [("executor-memory", "6G"), ("executor-cores", "5"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.sql.shuffle.partitions=50"),
                      ("conf", "spark.default.parallelism=50"), ("conf", "spark.dynamicAllocation.enabled=true")]

AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/spark-3.5.1/audience.jar"
DATE_TIME = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"

diagnosis_data_generator = EmrJobTask(
    name="DiagnosisDataGenerator",
    class_name="com.thetradedesk.audience.jobs.DiagnosisDataGenerator",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=[('dateTime', DATE_TIME)],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=1)
)
diagnosis_etl_cluster_task.add_parallel_body_task(diagnosis_data_generator)

diagnosis_data_monitor = EmrJobTask(
    name="DiagnosisDataMonitor",
    class_name="com.thetradedesk.audience.jobs.DiagnosisDataMonitor",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=[('dateTime', DATE_TIME)],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=1)
)
diagnosis_etl_cluster_task.add_parallel_body_task(diagnosis_data_monitor)

vertica_import = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': 'lwdb',
            'sproc_arguments': {
                'gatingType': 2000511,  # dbo.fn_Enum_GatingType_ImportDistributedAlgoDiagnosisVertica()
                'grain': 100001,  # dbo.fn_Enum_TaskBatchGrain_Hourly()
                'dateTimeToOpen': '{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'
            }
        },
        task_id="vertica_import",
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# Flow
diagnosis_etl_dag >> dataset_sensor >> diagnosis_etl_cluster_task >> final_dag_status_step
diagnosis_data_generator >> diagnosis_data_monitor >> final_dag_status_step
diagnosis_data_generator >> vertica_import >> final_dag_status_step
