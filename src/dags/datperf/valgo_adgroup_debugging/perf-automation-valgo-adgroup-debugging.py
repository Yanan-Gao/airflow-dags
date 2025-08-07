import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from dags.datperf.datasets import ial_dataset
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [("executor-memory", "202G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=110G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=10000"),
                      ("conf", "spark.driver.maxResultSize=50G"), ("conf", "spark.dynamicAllocation.enabled=true"),
                      ("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25"),
                      ("conf", "spark.sql.mapKeyDedupPolicy=LAST_WIN")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

execution_start_ymd = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
execution_start_h = "{{ data_interval_start.strftime(\"%H\") }}"
execution_start_dt = "{{ data_interval_start.to_datetime_string() }}"

ELDORADO_DATPERF_JAR = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-datperf-assembly.jar"

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
debugging_dag = TtdDag(
    dag_id="perf-automation-valgo-adgroup-debugging",
    start_date=datetime(2024, 10, 15),
    schedule_interval=timedelta(hours=1),
    max_active_runs=4,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = debugging_dag.airflow_dag

###############################################################################
# S3 Data Sensors
###############################################################################

bidrequest_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='bidrequest_data_available',
    datasets=[RtbDatalakeDatasource.rtb_bidrequest_v5],
    # looks for success file for this hour
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

internalauction_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='internalauction_data_available',
    datasets=[ial_dataset.with_check_type("hour")],
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12,  # wait up to 12 hours
)

###############################################################################
# Cluster config
###############################################################################

cores = 768

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_16xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5.r5_24xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(96)
    ],
    on_demand_weighted_capacity=cores
)

cluster = EmrClusterTask(
    name="ValueAlgoAdGroupDebuggingCluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

###############################################################################
# Job step: produce aggregate data
###############################################################################

step = EmrJobTask(
    name="ValueAlgoAdGroupDebuggingJob",
    class_name="com.thetradedesk.jobs.valuealgos.AdGroupDebugging",
    executable_path=ELDORADO_DATPERF_JAR,
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=java_settings_list + [('date', execution_start_ymd), ('hour', execution_start_h),
                                                            ('writePartitions', '600')],
    timeout_timedelta=timedelta(hours=2)
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

cluster.add_parallel_body_task(step)

debugging_dag >> cluster

[internalauction_sensor, bidrequest_sensor] >> cluster.first_airflow_op()

cluster.last_airflow_op() >> final_dag_status_step
