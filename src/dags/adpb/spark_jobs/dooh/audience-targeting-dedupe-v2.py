import logging
from datetime import timedelta, datetime

from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask

job_num_days_to_load = 5
job_num_hours_to_process = 6

job_name = 'dooh-audience-targeting-dedupe-v2'
job_schedule_interval = timedelta(hours=job_num_hours_to_process)
job_start_date = datetime(2024, 8, 6, 0, 0)
job_environment = TtdEnvFactory.get_from_system()

job_hours_latency: timedelta = timedelta(days=job_num_days_to_load, hours=job_num_hours_to_process)

active_running_jobs = 3

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
gating_type_id = 2000105  # dbo.fn_enum_GatingType_ImportDoohLocationVisits()
cross_device_gating_type_id = 2000109  # dbo.fn_enum_GatingType_ImportDoohLocationVisitsCrossDevice()
TaskBatchGrain_Hourly = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()

jar_path: str = 's3://ttd-build-artefacts/channels/styx/snapshots/master/latest/styx-assembly.jar'

cluster_tags = {"Team": "CHNL"}

# Bootstrap script for TLS to work on Aerospike on-prem. Imports TTD Root CA
bootstrap_script_location = 's3://ttd-identity/datapipeline/jars/bootstrap'
bootstrap_script = f'{bootstrap_script_location}/import-ttd-certs.sh'

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
    retry_delay=timedelta(minutes=20)
)

####################################################################################################################
# cluster
####################################################################################################################

base_ebs_size = 64

dedupe_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6gd_2xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

dedupe_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6gd_4xlarge().with_fleet_weighted_capacity(1),
        M6g.m6gd_8xlarge().with_fleet_weighted_capacity(2),
        R6gd.r6gd_4xlarge().with_fleet_weighted_capacity(1),
        R6gd.r6gd_8xlarge().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=15
)

dedupe_cluster = EmrClusterTask(
    name="dooh_audience_targeting_dedupe_v2_cluster",
    master_fleet_instance_type_configs=dedupe_master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=dedupe_core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2
)

# 20% increase in the cluster size while re-processing data (IM-926)
cross_device_ondemand_weighted_capacity = 240 + 48

targeting_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

cross_device_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C5.c5d_4xlarge().with_ebs_size_gb(base_ebs_size * 12).with_fleet_weighted_capacity(4),
        M5d.m5d_xlarge().with_fleet_weighted_capacity(1),
        M6g.m6gd_4xlarge().with_fleet_weighted_capacity(4),
        M6g.m6gd_8xlarge().with_fleet_weighted_capacity(8),
        M6g.m6gd_12xlarge().with_fleet_weighted_capacity(12)
    ],
    on_demand_weighted_capacity=cross_device_ondemand_weighted_capacity
)

cross_device_cluster = EmrClusterTask(
    name="dooh_location_visits_cross_device_cluster",
    master_fleet_instance_type_configs=targeting_master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=cross_device_core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    ec2_subnet_ids=["subnet-0e82171b285634d41"],  # us-east-1d
    emr_managed_master_security_group="sg-008678553e48f48a3",
    emr_managed_slave_security_group="sg-02fa4e26912fd6530",
    service_access_security_group="sg-0b0581bc37bcac50a",
    bootstrap_script_actions=[ScriptBootstrapAction(bootstrap_script, [bootstrap_script_location])],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%dT%H:00:00"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") - job_hours_latency
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


def check_incoming_data_exists(job_date_str: str, num_hours_to_process, num_days_to_load, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    for h in range(num_days_to_load * 24 + num_hours_to_process):
        hour_to_check = job_date + timedelta(hours=h)
        logging.info(f"Checking if input data exists for {hour_to_check}")

        if not Datasources.dooh.location_visits_CSV.with_check_type('hour').check_data_exist(cloud_storage, hour_to_check):
            raise AirflowNotFoundException

    return True


def should_cross_device_location_visits(job_date_str, num_hours_to_process):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    # Cross Device the location visits if we've processed all the hours in the day
    res = job_date.hour + num_hours_to_process >= 24
    logging.info(f"Should cross device location visits for {job_date}, hours to process {num_hours_to_process}?: {res}")
    return res


def check_deduped_location_visits_exists(job_date_str: str, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format).replace(hour=0, minute=0, second=0, microsecond=0)
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    for h in range(24):
        hour_to_check = job_date + timedelta(hours=h)
        logging.info(f"Checking if deduped location visits data exists for {hour_to_check}")

        if not Datasources.dooh.location_visits_deduped_v3.with_check_type('hour').check_data_exist(cloud_storage, hour_to_check):
            raise AirflowNotFoundException

    return True


def add_success_file(job_date_str: str, num_hours_to_process, **kwargs):
    hook = AwsCloudStorage()
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    for h in range(num_hours_to_process):
        date = job_date + timedelta(hours=h)
        path = Datasources.dooh.location_visits_deduped_v3._get_full_key(date)
        hook.load_string(
            string_data="", key=f"{path}/_SUCCESS", bucket_name=Datasources.dooh.location_visits_deduped_v3.bucket, replace=True
        )
    return "Success markers written"


def open_lwdb_gate_hourly(mssql_conn_id, gating_type, job_date_str: str, num_hours_to_process, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    for h in range(num_hours_to_process):
        date = job_date + timedelta(hours=h)
        log_start_time = date.strftime('%Y-%m-%d %H:00:00')
        ExternalGateOpen(
            mssql_conn_id=mssql_conn_id,
            sproc_arguments={
                'gatingType': gating_type,
                'grain': TaskBatchGrain_Hourly,
                'dateTimeToOpen': log_start_time
            }
        )


def open_lwdb_gate_daily(mssql_conn_id, gating_type, job_date_str: str, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    log_start_time = job_date.strftime('%Y-%m-%d')
    ExternalGateOpen(
        mssql_conn_id=mssql_conn_id,
        sproc_arguments={
            'gatingType': gating_type,
            'grain': TaskBatchGrain_Daily,
            'dateTimeToOpen': log_start_time
        }
    )


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
        op_kwargs=dict(
            job_date_str=run_date_str_template, num_hours_to_process=job_num_hours_to_process, num_days_to_load=job_num_days_to_load
        ),
        retries=3,
        provide_context=True,
        do_xcom_push=True
    )
)

# 2. Check if a full day has been processed
check_should_cross_device_location_visits = OpTask(
    op=ShortCircuitOperator(
        task_id="check_should_cross_device_location_visits",
        python_callable=should_cross_device_location_visits,
        op_kwargs={
            "job_date_str": run_date_str_template,
            "num_hours_to_process": job_num_hours_to_process
        },
        dag=dag.airflow_dag,
        trigger_rule="none_failed"
    )
)

check_deduped_location_visits_exists = OpTask(
    op=PythonOperator(
        task_id='check_deduped_location_visits_exists',
        python_callable=check_deduped_location_visits_exists,
        op_kwargs=dict(job_date_str=run_date_str_template),
        retries=3,
        provide_context=True,
        do_xcom_push=True
    )
)

fill_s3_success_markers = OpTask(
    op=PythonOperator(
        task_id="fill_s3_success_markers",
        python_callable=add_success_file,
        op_kwargs=dict(
            job_date_str=run_date_str_template,
            num_hours_to_process=job_num_hours_to_process,
        ),
        retries=3,
        provide_context=True,
        dag=dag.airflow_dag,
    )
)

####################################################################################################################
# steps
####################################################################################################################

default_config = [('conf', 'spark.dynamicAllocation.enabled=true'), ('conf', 'spark.speculation=false'),
                  ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'), ('conf', 'spark.driver.maxResultSize=0'),
                  ('conf', 'spark.sql.files.ignoreCorruptFiles=true')]

location_visits_dedupe_step = EmrJobTask(
    name="location_visits_hourly_dedupe_v2",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.dedupe.LocationVisitsHourlyDedupeJobV2",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[('datetime', run_date_str_template), ('numHoursToProcess', job_num_hours_to_process),
                                       ('numDaysToLoad', job_num_days_to_load),
                                       ('locationVisitsRootPath', Datasources.dooh.location_visits_CSV.get_dataset_path()),
                                       ('locationVisitsHourlyParquetRootPath', Datasources.dooh.location_visits_deduped_v3.get_root_path())
                                       ],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=dedupe_cluster.cluster_specs
)

dedupe_cluster.add_parallel_body_task(location_visits_dedupe_step)

location_visits_cross_device_step = EmrJobTask(
    name="dooh_location_visits_cross_device",
    executable_path=jar_path,
    class_name="jobs.dooh.crossdevice.LocationVisitsCrossDeviceJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ('datetime', run_date_str_template),
        ('locationVisitsHourlyParquetRootPath', Datasources.dooh.location_visits_deduped_v3.get_root_path()),
        ('locationVisitsCrossDeviceParquetRootPath', Datasources.dooh.location_visits_cross_device.get_root_path())
    ],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=cross_device_cluster.cluster_specs
)

cross_device_cluster.add_parallel_body_task(location_visits_cross_device_step)
coldstorage_aerospike_host = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", limit=1)}}'

location_visits_cross_device_targeting_data_lookup_step = EmrJobTask(
    name="dooh_location_visits_cross_device_targeting_data_lookup",
    executable_path=jar_path,
    class_name="jobs.dooh.v2.targetingdatalookup.LocationVisitsCrossDeviceTargetingDataLookupJob",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ('datetime', run_date_str_template), ('locationVisitsCrossDeviceSamplingRate', 1),
        ('locationVisitsCrossDeviceParquetRootPath', Datasources.dooh.location_visits_cross_device.get_root_path()),
        ('coldStorageLookupOutputParquetRootPath', Datasources.dooh.cold_storage_cross_device_lookup_output.get_root_path()),
        ('digitalOutOfHomeAdGroupsRootPath', Datasources.dooh.active_adgroups.get_root_path()),
        ('extraAudienceRootPath', Datasources.dooh.extra_audiences.get_root_path()), ("aerospikeHost", coldstorage_aerospike_host),
        ("aerospikePort", 4333), ("redisHost", "gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com"),
        ("redisPort", 6379)
    ],
    additional_args_option_pairs_list=default_config,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=1),
    cluster_specs=cross_device_cluster.cluster_specs
)

# Trigger cross-device Vertica import via DataMover by opening the Gate
cross_device_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=open_lwdb_gate_daily,
        provide_context=True,
        op_kwargs=dict(
            mssql_conn_id=logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            gating_type=cross_device_gating_type_id,
            job_date_str=run_date_str_template,
        ),
        task_id="cross_device_vertica_import_open_gate",
    )
)

cross_device_cluster.add_parallel_body_task(location_visits_cross_device_targeting_data_lookup_step)

location_visits_cross_device_step >> location_visits_cross_device_targeting_data_lookup_step

# Trigger Vertica import via DataMover by opening the Gate
visits_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=open_lwdb_gate_hourly,
        provide_context=True,
        op_kwargs=dict(
            mssql_conn_id=logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            gating_type=gating_type_id,
            job_date_str=run_date_str_template,
            num_hours_to_process=job_num_hours_to_process,
        ),
        task_id="avail_agg_vertica_import_open_gate",
    )
)

dag >> get_job_run_datetime_op >> check_incoming_data_exists
check_incoming_data_exists >> dedupe_cluster >> fill_s3_success_markers >> visits_vertica_import_open_gate \
    >> check_should_cross_device_location_visits >> check_deduped_location_visits_exists
check_deduped_location_visits_exists >> cross_device_cluster >> cross_device_vertica_import_open_gate

adag = dag.airflow_dag
