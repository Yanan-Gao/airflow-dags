from datetime import datetime
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.ttdenv import TtdEnvFactory
from dags.datperf.datasets import kongming_oosattribution_dataset
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

# Get data_interval_start and subtract 5 days
exec_date_formatted = '{{ (data_interval_start - macros.timedelta(days=5)).strftime("%Y%m%d") }}'
geronimo_jar = 's3://thetradedesk-mlplatform-us-east-1/libs/geronimo/jars/prod/geronimo.jar'
environment = TtdEnvFactory.get_from_system()

schedule_interval = "0 4 * * *"
# DAG Definition
segment_selection_dag = TtdDag(
    dag_id="perf-automation-valgo-missioncontrol-ev-decomposition",
    start_date=datetime(2025, 5, 4),
    schedule_interval=schedule_interval,
    dag_tsg='',
    retries=1,
    max_active_runs=1,
    enable_slack_alert=False,
    tags=['DATPERF']
)
dag = segment_selection_dag.airflow_dag

upstream_etl_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="upstream_etl_available",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start - macros.timedelta(days=5)).strftime("%Y-%m-%d 23:00:00") }}',
    datasets=[kongming_oosattribution_dataset],
)

# Cluster Setup
instance_configs = {
    'master':
    EmrFleetInstanceTypes(instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1),
    'core':
    EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
        ],
        on_demand_weighted_capacity=56
    )
}

etl_cluster = EmrClusterTask(
    name="valgo-ev-decompose-cluster",
    cluster_tags={"Team": DATPERF.team.jira_team},
    master_fleet_instance_type_configs=instance_configs['master'],
    core_fleet_instance_type_configs=instance_configs['core'],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=-1,
    environment=environment
)

# Common Configurations
spark_config = [("executor-memory", "100G"),
                ("executor-cores", "15"), ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                ("conf", "spark.driver.memory=110G"), ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=10000"),
                ("conf", "spark.driver.maxResultSize=20G"), ("conf", "spark.dynamicAllocation.enabled=true"),
                ("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25")]

job_config = [("date", exec_date_formatted)]

# Tasks
tasks = {
    'ial-preprocess':
    EmrJobTask(
        name="etl-ial-preprocess",
        class_name="job.EVDecompositionIALJoin",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config,
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    ),
    'ev-auc-preprocess-conversion':
    EmrJobTask(
        name="etl-ev-auc-preprocess",
        class_name="job.EVDecompositionAUCProcessor_2",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=job_config +
        [("basePath", "s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/dev/investigation_control/v=1")],
        additional_args_option_pairs_list=spark_config,
        executable_path=geronimo_jar
    )
}

# Add tasks to cluster and set dependencies
for task in tasks.values():
    etl_cluster.add_parallel_body_task(task)

tasks['ial-preprocess'] >> tasks['ev-auc-preprocess-conversion']

final_status_check = FinalDagStatusCheckOperator(dag=dag)

# DAG Dependencies
segment_selection_dag >> etl_cluster
upstream_etl_sensor >> etl_cluster.first_airflow_op()
etl_cluster.last_airflow_op() >> final_status_check
