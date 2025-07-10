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
from datasources.sources.sib_datasources import SibDatasources

from dags.datperf.utils.spark_config_utils import get_spark_args

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}]

# Below, we schedule the job start for 3 am every day (approx. when the SIB data for the prior day should be available.)
# Since we do daily processing, the execution_date will be the day prior to when the job actually runs.
# So, the sensors should check availability for the execution_date day.
execution_date_Y_m_d = "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"
execution_start_dt = "{{ data_interval_start.to_datetime_string() }}"

# Jar
GERONIMO_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/geronimo/jars/prod/geronimo.jar"

# SIB prep step: run every day at 3am (that's approx. when the SIB data should be all available)
geronimo_sib_prep = TtdDag(
    dag_id="perf-automation-geronimo-sib-prep",
    start_date=datetime(2024, 11, 7, 3),
    schedule_interval=timedelta(days=1),
    max_active_runs=5,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/display/EN/Geronimo+SeenInBidding+Data+Preparation+TSG',
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = geronimo_sib_prep.airflow_dag

sib_device_level_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='sib_device_level_data_available',
    datasets=[SibDatasources.sibv2_device_seven_day_rollup],
    # looks for success file for this hour
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 24,  # wait up to 24 hours
)

sib_group_level_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='sib_group_level_data_available',
    datasets=[SibDatasources.sibv2_daily(xd_vendor_id=10)],
    # looks for success file for this hour
    ds_date=execution_start_dt,
    poke_interval=60 * 10,
    timeout=60 * 60 * 24,  # wait up to 24 hours
)

###############################################################################
# Cluster config
###############################################################################

cluster_capacity = 45

cluster_params = R6a.r6a_16xlarge().calc_cluster_params(
    instances=cluster_capacity, min_executor_memory=202, max_cores_executor=32, memory_tolerance=0.95
)
spark_args = get_spark_args(cluster_params)

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

geronimo_sib_prep_cluster = EmrClusterTask(
    name="GeronimoSibPrepCluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": DATPERF.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True
)

sib_prep_step = EmrJobTask(
    name="GeronimoSibDataPreparationStep",
    class_name="job.GeronimoSibDataPreparation",
    executable_path=GERONIMO_JAR,
    additional_args_option_pairs_list=copy.deepcopy(spark_args),
    eldorado_config_option_pairs_list=java_settings_list + [
        ('date', execution_date_Y_m_d),
        ('writePartitions', '500')  # 2023-10: with 500 each output partition will be approx. 320 MB
    ],
    timeout_timedelta=timedelta(hours=1)
)
geronimo_sib_prep_cluster.add_sequential_body_task(sib_prep_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# Group level is usually available before device level SIB data, so check data availability in that order
sib_group_level_sensor >> sib_device_level_sensor >> geronimo_sib_prep_cluster.first_airflow_op()

geronimo_sib_prep >> geronimo_sib_prep_cluster

geronimo_sib_prep_cluster.last_airflow_op() >> final_dag_status_step
