from datetime import datetime, timedelta

import copy

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r6a import R6a

from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders

from dags.datperf.utils.spark_config_utils import get_spark_args

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}]

# Jar
GERONIMO_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/geronimo/jars/prod/geronimo.jar"

execution_date_Y_m_d = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
execution_date_H = "{{ data_interval_start.strftime(\"%H\") }}"
execution_start_dt = "{{ data_interval_start.to_datetime_string() }}"
contextual_sensor_date = "{{(data_interval_start + macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}}"

geronimo_contextual_etl = TtdDag(
    dag_id="perf-automation-geronimo-contextual-etl",
    start_date=datetime(2025, 5, 23, 5),
    schedule_interval="30 * * * *",
    run_only_latest=False,
    max_active_runs=5,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/display/EN/Geronimo+ETL+TSG',
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = geronimo_contextual_etl.airflow_dag

###############################################################################
# S3 Data Sensors
###############################################################################

bidimpression_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='bidimpression_data_available',
    datasets=[Datasources.perfauto.bid_impression_dataset.with_check_type("hour")],
    # looks for success file for this hour
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 12  # wait up to 12 hours
)

contextual_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="upstream_dataset_check_aws",
    datasets=[
        Datasources.contextual.contextual_requested_urls.with_check_type("hour").as_read(),
        Datasources.contextual.cxt_results.with_check_type("hour").as_read()
    ],
    ds_date=contextual_sensor_date,
    poke_interval=60 * 10,  # in seconds
    timeout=60 * 60 * 12,  # wait up to 12 hours
    cloud_provider=CloudProviders.aws,
)

###############################################################################
# Cluster config
###############################################################################

cluster_capacity = 80

cluster_params = R6a.r6a_16xlarge().calc_cluster_params(
    instances=cluster_capacity, min_executor_memory=202, max_cores_executor=32, memory_tolerance=0.95
)

spark_args = get_spark_args(cluster_params) + [("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25")]

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6a.r6a_16xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R6a.r6a_32xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=cluster_capacity
)

brbf_contextual_etl_cluster = EmrClusterTask(
    name="BrBfContextualEtlCluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

bfbrcontextual_step = EmrJobTask(
    name="BrBfContextual",
    class_name="job.ContextualPreparation",
    executable_path=GERONIMO_JAR,
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=java_settings_list + [('date', execution_date_Y_m_d), ('hour', execution_date_H)],
    timeout_timedelta=timedelta(hours=2)
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

brbf_contextual_etl_cluster.add_parallel_body_task(bfbrcontextual_step)

[bidimpression_sensor, contextual_sensor] >> brbf_contextual_etl_cluster.first_airflow_op()

geronimo_contextual_etl >> brbf_contextual_etl_cluster

brbf_contextual_etl_cluster.last_airflow_op() >> final_dag_status_step
