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
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders

from dags.datperf.utils.spark_config_utils import get_spark_args

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}]

# Jar
GERONIMO_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/geronimo/jars/prod/geronimo.jar"

execution_date_Y_m_d = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
execution_date_H = "{{ data_interval_start.strftime(\"%H\") }}"
execution_datehour = "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"
execution_start_dt = "{{ data_interval_start.to_datetime_string() }}"
contextual_sensor_date = "{{(data_interval_start + macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')}}"

# Number of bidfeedback hours to be joined with one hour of bid requests
bf_hours = 2

geronimo_etl = TtdDag(
    dag_id="perf-automation-geronimo-etl",
    start_date=datetime(2024, 11, 7, 13),
    schedule_interval=timedelta(hours=1),
    max_active_runs=5,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/display/EN/Geronimo+ETL+TSG',
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = geronimo_etl.airflow_dag

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

bidfeedback_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='bidfeedback_data_available',
    datasets=[RtbDatalakeDatasource.rtb_bidfeedback_v5],
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
        Datasources.perfauto.eligible_user_data_raw.with_check_type("hour")
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

bfbf_etl_cluster = EmrClusterTask(
    name="BfBrEtlCluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

bfbr_step1 = EmrJobTask(
    name="BrBf1",
    class_name="job.BrBf",
    executable_path=GERONIMO_JAR,
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=java_settings_list + [('date', execution_date_Y_m_d), ('hour', execution_date_H),
                                                            ('numberOfBfHours', bf_hours), ('writePartitions', '1200'),
                                                            ('addSeenInBiddingData', 'true')],
    timeout_timedelta=timedelta(hours=2)
)

eligible_user_data_step = EmrJobTask(
    name="EligibleUserDataLogConverter",
    class_name="job.EligibleUserDataLogConverter",
    executable_path=GERONIMO_JAR,
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=java_settings_list + [("dateHourToProcess", execution_datehour), ('writePartitions', '2500'),
                                                            ('partitions', '6000')],
    timeout_timedelta=timedelta(hours=1)
)

feature_log_step = EmrJobTask(
    name="FeatureLogConverter",
    class_name="job.GeronimoCommonFeatureLogConverter",
    executable_path=GERONIMO_JAR,
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=java_settings_list + [("dateToProcess", execution_date_Y_m_d), ('hourToProcess', execution_date_H)],
    timeout_timedelta=timedelta(hours=1)
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

bfbf_etl_cluster.add_sequential_body_task(feature_log_step)
bfbf_etl_cluster.add_sequential_body_task(eligible_user_data_step)
bfbf_etl_cluster.add_sequential_body_task(bfbr_step1)

[bidrequest_sensor, bidfeedback_sensor, contextual_sensor] >> bfbf_etl_cluster.first_airflow_op()

geronimo_etl >> bfbf_etl_cluster

bfbf_etl_cluster.last_airflow_op() >> final_dag_status_step
