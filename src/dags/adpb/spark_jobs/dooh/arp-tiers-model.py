import logging
from datetime import timedelta, datetime

from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python import PythonOperator

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.compute_optimized.c7gd import C7gd
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.tasks.op import OpTask

job_num_hours_to_process: int = 1
job_latency = 5  # Runs 5 hours behind of the result's hour. This allows the results to be in S3 about 3.5 hours before they are needed.

job_name = 'dooh-arp-tiers-model'
job_schedule_interval = timedelta(hours=job_num_hours_to_process)
job_start_date = datetime(2024, 8, 5, 13, 0)

job_hours_latency: timedelta = timedelta(hours=job_latency + job_num_hours_to_process)

days_to_lookback = "6,7,8,14,21"

active_running_jobs = 4

jar_path: str = 's3://ttd-build-artefacts/channels/styx/snapshots/master/latest/styx-assembly.jar'

cluster_tags = {"Team": "CHNL"}

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel="#scrum-adpb-alerts",
    retries=3,
    retry_delay=timedelta(minutes=30)
)

####################################################################################################################
# clusters
####################################################################################################################

instance_types = [
    C7gd.c7gd_4xlarge().with_fleet_weighted_capacity(4),
    C7gd.c7gd_8xlarge().with_fleet_weighted_capacity(8),
    C7gd.c7gd_12xlarge().with_fleet_weighted_capacity(12)
]

ondemand_weighted_capacity = 180

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_8xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=ondemand_weighted_capacity
)

arp_tiers_model_cluster = EmrClusterTask(
    name="dooh_arp_tiers_model_cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%dT%H:%M:%S"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") + job_hours_latency
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


def check_incoming_data_exists(job_date_str: str, num_hours_to_process: int, **kwargs):
    for hour_delta in range(num_hours_to_process):
        job_date = datetime.strptime(job_date_str, job_datetime_format) + timedelta(hours=hour_delta)
        cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

        logging.info(f"Checking if input data exists for {job_date} and lookback of: {days_to_lookback}")
        for day_to_lookback in days_to_lookback.split(','):
            day_to_lookback_delta = -1 * int(day_to_lookback)
            if not Datasources.dooh.on_target_visit_counts.with_check_type('hour').check_data_exist(
                    cloud_storage, job_date + timedelta(days=day_to_lookback_delta)):
                raise AirflowNotFoundException

    return True


get_job_run_datetime_op = OpTask(
    op=PythonOperator(
        task_id='get_job_run_datetime',
        python_callable=get_job_run_datetime,
        op_kwargs=dict(execution_date='{{ ts_nodash }}'),
        provide_context=True,
        do_xcom_push=True
    )
)

run_date_str_template = f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{get_job_run_datetime_op.first_airflow_op().task_id}") }}}}'

#   Fail and retry if No New Data
check_incoming_data_exists = OpTask(
    op=PythonOperator(
        task_id='check_incoming_data_exists',
        python_callable=check_incoming_data_exists,
        op_kwargs=dict(job_date_str=run_date_str_template, num_hours_to_process=job_num_hours_to_process),
        retries=6,
        provide_context=True,
        retry_exponential_backoff=True,
        retry_delay=timedelta(minutes=20),
    )
)

####################################################################################################################
# steps
####################################################################################################################

default_config = [('conf', 'spark.dynamicAllocation.enabled=true'), ('conf', 'spark.speculation=false'),
                  ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'),
                  ('conf', 'spark.sql.files.ignoreCorruptFiles=true')]

on_target_config = list(default_config)
on_target_config.append(('conf', 'spark.driver.maxResultSize=19g'))

for hour_delta in range(job_num_hours_to_process):

    backfill_step = EmrJobTask(
        name="on_target_backfill_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.v2.ontargetcalculator.OnTargetCalculatorBackFillJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('datetime', run_date_str_template),
            ('hourToStartDelta', f'{hour_delta}'),
            ('daysToLookBack', days_to_lookback),
            ('coldStorageCrossDeviceLookupOutputSamplingRate', '1'),
            ('coldStorageCrossDeviceLookupOutputParquetRootPath', Datasources.dooh.cold_storage_cross_device_lookup_output.get_root_path()),
            ('openlineage.enable', 'false')  # Disable until AIFUN-531 is resolved
        ],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=arp_tiers_model_cluster.cluster_specs
    )

    arp_model_step = EmrJobTask(
        name="arp_model_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.model.ArpTiersModelJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('datetime', run_date_str_template),
            ('hourToStartDelta', f'{hour_delta}'),
            ('modelLookbackDays', days_to_lookback),
            ('dealsDataSetLookbackDays', '7'),
            ('locationUniqueUsersMinimumThreshold', '10'),
            ('arpTiersModelParquetRootPath', Datasources.dooh.arp_tiers_model.get_root_path()),
            ('openlineage.enable', 'false')  # Disable until AIFUN-531 is resolved
        ],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=arp_tiers_model_cluster.cluster_specs
    )

    backfill_step >> arp_model_step
    arp_tiers_model_cluster.add_parallel_body_task(backfill_step)
    arp_tiers_model_cluster.add_parallel_body_task(arp_model_step)

dag >> get_job_run_datetime_op >> check_incoming_data_exists >> arp_tiers_model_cluster

adag = dag.airflow_dag
