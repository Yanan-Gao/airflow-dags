import logging
from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowNotFoundException
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask

job_name = 'dooh-avail-screen-processor'
job_start_date = datetime(2024, 8, 12, 12, 0)
job_schedule_interval = "0 * * * *"
active_running_jobs = 1
jar_path: str = 's3://ttd-build-artefacts/channels/styx/snapshots/master/latest/styx-assembly.jar'
job_hours_latency: timedelta = timedelta(hours=2)

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
    retries=1,
    retry_delay=timedelta(minutes=10)
)

####################################################################################################################
# cluster
####################################################################################################################

base_ebs_size = 64

master_instance_type = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_instance_type = EmrFleetInstanceTypes(
    instance_types=[
        R7g.r7g_8xlarge().with_ebs_size_gb(base_ebs_size * 2).with_fleet_weighted_capacity(1),
        R7gd.r7gd_8xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=2
)

cluster = EmrClusterTask(
    name="dooh_avail_screen_processor_cluster",
    master_fleet_instance_type_configs=master_instance_type,
    cluster_tags={
        'Team': 'DATPRD',
    },
    core_fleet_instance_type_configs=core_instance_type,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%dT%H:00:00"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") - job_hours_latency
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


def check_incoming_data_exists(job_date_str: str, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    logging.info(f"Checking if input data exists for {job_date}")

    if not Datasources.dooh.dooh_avail_logs_hour.with_check_type('hour').check_data_exist(cloud_storage, job_date):
        raise AirflowNotFoundException

    return True


get_job_run_datetime_task = OpTask(
    op=PythonOperator(
        task_id='get_job_run_datetime',
        python_callable=get_job_run_datetime,
        op_kwargs=dict(execution_date='{{ ts_nodash }}'),
        provide_context=True,
        do_xcom_push=True
    )
)

run_date_str_template = f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{get_job_run_datetime_task.first_airflow_op().task_id}") }}}}'

check_incoming_data_exists_task = OpTask(
    op=PythonOperator(
        task_id='check_incoming_data_exists',
        python_callable=check_incoming_data_exists,
        op_kwargs=dict(job_date_str=run_date_str_template),
        retries=2,
        provide_context=True,
        do_xcom_push=True
    )
)

####################################################################################################################
# steps
####################################################################################################################

default_config = [
    ('conf', 'spark.dynamicAllocation.enabled=true'),
    ('conf', 'spark.speculation=false'),
    ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'),
    ('conf', 'spark.sql.files.ignoreCorruptFiles=true'),
]

avail_screen_processor_step = EmrJobTask(
    name="dooh_avail_screen_processor_step",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.screeninventory.DoohAvailScreenProcessorJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('dateToProcess', run_date_str_template)],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=5),
    cluster_specs=cluster.cluster_specs
)

external_screen_inventory_updater_step = EmrJobTask(
    name="dooh_external_screen_inventory_updater_step",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.screeninventory.DoohExternalScreenInventoryUpdaterJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('dateToProcess', run_date_str_template)],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=cluster.cluster_specs
)

internal_screen_inventory_updater_step = EmrJobTask(
    name="dooh_internal_screen_inventory_updater_step",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.screeninventory.DoohInternalScreenInventoryUpdaterJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=cluster.cluster_specs
)

deal_screen_lookup_step = EmrJobTask(
    name="dooh_deal_screen_lookup_step",
    executable_path=jar_path,
    class_name="jobs.dooh.DoohDealScreensHourlyLookupJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('dateToProcess', run_date_str_template)],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=cluster.cluster_specs
)

avail_screen_processor_step >> external_screen_inventory_updater_step >> internal_screen_inventory_updater_step
avail_screen_processor_step >> deal_screen_lookup_step
cluster.add_parallel_body_task(avail_screen_processor_step)
cluster.add_parallel_body_task(external_screen_inventory_updater_step)
cluster.add_parallel_body_task(internal_screen_inventory_updater_step)
cluster.add_parallel_body_task(deal_screen_lookup_step)

dag >> get_job_run_datetime_task >> check_incoming_data_exists_task >> cluster

adag = dag.airflow_dag
