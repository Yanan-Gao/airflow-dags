import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow.exceptions import AirflowNotFoundException
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask

job_name = 'dooh-audience-targeting-model'
job_schedule_interval = timedelta(hours=3)
job_start_date = datetime(2024, 8, 7, 12, 0)
job_environment = TtdEnvFactory.get_from_system()

job_hours_latency: timedelta = timedelta(hours=6)
job_num_hours_to_process: int = 3
offset_days: int = 6

active_running_jobs = 3

jar_path: str = 's3://ttd-build-artefacts/channels/styx/snapshots/master/latest/styx-assembly.jar'

cluster_tags = {"Team": "CHNL"}

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
gating_type_id = 2000110  # dbo.fn_enum_GatingType_ImportDoohCrossDeviceLookup()
TaskBatchGrain_Hourly = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()

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
    retries=2,
    retry_delay=timedelta(minutes=30)
)

####################################################################################################################
# clusters
####################################################################################################################

targeting_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_8xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

model_base_ebs_size = 256

aggregation_ondemand_weighted_capacity = 40

aggregation_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R7gd.r7gd_4xlarge().with_ebs_size_gb(model_base_ebs_size).with_fleet_weighted_capacity(1),
        M7g.m7gd_4xlarge().with_ebs_size_gb(model_base_ebs_size).with_fleet_weighted_capacity(1),
        M7g.m7gd_16xlarge().with_ebs_size_gb(model_base_ebs_size * 4).with_fleet_weighted_capacity(4)
    ],
    on_demand_weighted_capacity=aggregation_ondemand_weighted_capacity
)

aggregation_cluster = EmrClusterTask(
    name="dooh_audience_targeting_aggregation_cluster",
    master_fleet_instance_type_configs=targeting_master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=aggregation_core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

application_configuration = [{
    "classification": "capacity-scheduler",
    "properties": {
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}, {
    "classification": "spark-defaults",
    "properties": {
        "spark.dynamicAllocation.enabled": "true"
    }
}, {
    "classification": "spark",
    "properties": {
        "maximizeResourceAllocation": "true"
    }
}]

model_ondemand_weighted_capacity = 375

model_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7gd_8xlarge().with_ebs_size_gb(model_base_ebs_size * 2).with_fleet_weighted_capacity(2),
        M7g.m7gd_4xlarge().with_ebs_size_gb(model_base_ebs_size).with_fleet_weighted_capacity(1),
        M7g.m7gd_16xlarge().with_ebs_size_gb(model_base_ebs_size * 4).with_fleet_weighted_capacity(4)
    ],
    on_demand_weighted_capacity=model_ondemand_weighted_capacity
)

model_cluster = EmrClusterTask(
    name="dooh_audience_targeting_model_cluster",
    master_fleet_instance_type_configs=targeting_master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=model_core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    # additional_application_configurations=application_configuration,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%dT%H:%M:%S"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") - job_hours_latency
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


def get_model_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") - job_hours_latency + timedelta(days=1)
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


def check_incoming_data_exists(job_date_str: str, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

    logging.info(f"Checking if input data exists for {job_date}")

    if (not Datasources.dooh.location_visits_deduped_v3.with_check_type('hour').check_data_exist(cloud_storage,
                                                                                                 job_date - timedelta(days=offset_days))
            or not Datasources.dooh.cold_storage_lookup_output_v2.with_check_type('hour').check_data_exist(
                cloud_storage, job_date - timedelta(days=offset_days))):
        raise AirflowNotFoundException

    return True


def open_lwdb_gate(mssql_conn_id, gating_type, grain, job_date_str: str, num_hours_to_process, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    for h in range(num_hours_to_process):
        date = job_date - timedelta(days=offset_days) + timedelta(hours=h)
        log_start_time = date.strftime('%Y-%m-%d %H:00:00')
        ExternalGateOpen(
            mssql_conn_id=mssql_conn_id, sproc_arguments={
                'gatingType': gating_type,
                'grain': grain,
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

get_model_job_run_datetime_op = OpTask(
    op=PythonOperator(
        task_id='get_model_job_run_datetime',
        python_callable=get_model_job_run_datetime,
        op_kwargs=dict(execution_date='{{ ts_nodash }}'),
        provide_context=True,
        do_xcom_push=True
    )
)

run_date_str_template = f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{get_job_run_datetime_op.first_airflow_op().task_id}") }}}}'
model_run_date_str_template = f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="{get_model_job_run_datetime_op.first_airflow_op().task_id}") }}}}'

#   Fail and retry if No New Data
check_incoming_data_exists = OpTask(
    op=PythonOperator(
        task_id='check_incoming_data_exists',
        python_callable=check_incoming_data_exists,
        op_kwargs=dict(job_date_str=run_date_str_template),
        retries=3,
        provide_context=True
    )
)

####################################################################################################################
# steps
####################################################################################################################

default_step_config = [('conf', 'spark.dynamicAllocation.enabled=true'), ('conf', 'spark.speculation=false'),
                       ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'),
                       ('conf', 'spark.sql.files.ignoreCorruptFiles=true')]
model_step_config = list(default_step_config)
model_step_config.append(('conf', 'spark.driver.maxResultSize=37g'))

aggregation_step_config = list(default_step_config)
aggregation_step_config.append(('conf', 'spark.driver.maxResultSize=8g'))
aggregation_step_config.append(('conf', 'spark.kryoserializer.buffer.max=128m'))

model_steps = []
late_aggregation_steps = []
for hour_delta in range(job_num_hours_to_process):

    # model steps
    late_location_visits_aggregation_step = EmrJobTask(
        name="location_visits_targeting_data_late_aggregation_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.aggregation.LocationVisitsAggregationJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('datetime', run_date_str_template), ('hourToStartDelta', f'{hour_delta}'), ('numHoursToProcess', '1'),
            ('lateAggregationDayOffset', offset_days), ('coldStorageCrossDeviceLookupOutputSamplingRate', '1'),
            ('locationVisitsHourlyParquetRootPath', Datasources.dooh.location_visits_deduped.get_root_path()),
            ('coldStorageLookupOutputParquetRootPath', Datasources.dooh.cold_storage_lookup_output.get_root_path()),
            ('locationVisitsTargetingDataParquetRootPath', Datasources.dooh.location_visits_aggregated.get_root_path()),
            ('coldStorageCrossDeviceLookupOutputParquetRootPath', Datasources.dooh.cold_storage_cross_device_lookup_output.get_root_path())
        ],
        additional_args_option_pairs_list=aggregation_step_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=aggregation_cluster.cluster_specs
    )
    late_aggregation_steps.append(late_location_visits_aggregation_step)

    model_step = EmrJobTask(
        name="audience_targeting_model_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.model.AudienceTargetingModelJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('datetime', model_run_date_str_template), ('hourToStartDelta', f'{hour_delta}'), ('numHoursToProcess', '1'),
            ('locationVisitsTargetingDataAggregatedParquetRootPath', Datasources.dooh.location_visits_aggregated.get_root_path()),
            ('audienceTargetingModelParquetRootPath', Datasources.dooh.audience_targeting_model.get_root_path()),
            ('digitalOutOfHomeAdGroupsRootPath', Datasources.dooh.active_adgroups.get_root_path()),
            ('extraAudienceRootPath', Datasources.dooh.extra_audiences.get_root_path()), ('otpModelStartDayOffset', str(offset_days)),
            ('otpModelEndDayOffset', '21'), ('partitions', '500'), ('locationUniqueUsersMinimumThreshold', 10),
            ('openlineage.enable', 'false')
        ],
        additional_args_option_pairs_list=model_step_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=model_cluster.cluster_specs
    )
    model_steps.append(model_step)

    # adding steps to the clusters
    aggregation_cluster.add_parallel_body_task(late_location_visits_aggregation_step)
    model_cluster.add_parallel_body_task(model_step)

# Trigger Vertica import via DataMover by opening the Gate
cold_storage_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=open_lwdb_gate,
        provide_context=True,
        op_kwargs=dict(
            mssql_conn_id=logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            gating_type=gating_type_id,
            grain=TaskBatchGrain_Hourly,
            job_date_str=run_date_str_template,
            num_hours_to_process=job_num_hours_to_process,
        ),
        task_id="cold_storage_vertica_import_open_gate",
    )
)

dag >> get_job_run_datetime_op >> check_incoming_data_exists >> get_model_job_run_datetime_op >> aggregation_cluster >> cold_storage_vertica_import_open_gate >> model_cluster

for hour in range(job_num_hours_to_process):

    # Don't start next hour until previous hour is complete
    if hour > 0:
        late_aggregation_steps[hour - 1] >> late_aggregation_steps[hour]
        model_steps[hour - 1] >> model_steps[hour]

adag = dag.airflow_dag
