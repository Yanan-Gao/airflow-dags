import logging
from datetime import timedelta, datetime

from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python import PythonOperator

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.compute_optimized.c6g import C6g
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.ttdenv import TtdEnvFactory
from ttd.datasets.hour_dataset import HourGeneratedDataset

job_num_hours_to_process: int = 3
job_dedupe_latency = 129  # hours (5.25 days + 3 hours) to sync with dooh-audience-targeting-dedupe-v2 lookback window of 5 days)

job_name = 'dooh-on-target-calculator'
job_schedule_interval = "0 */3 * * *"
job_start_date = datetime(2024, 8, 12, 12, 0)
job_environment = TtdEnvFactory.get_from_system()

job_hours_latency: timedelta = timedelta(hours=job_dedupe_latency + job_num_hours_to_process)

number_of_days_to_lookback = 5

active_running_jobs = 4

logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
baseline_gating_type_id = 2000120  # dbo.fn_enum_GatingType_ImportDoohArpBaselineHourly()
report_gating_type_id = 2000121  # dbo.fn_enum_GatingType_ImportDoohArpReportHourly()
TaskBatchGrain_Hourly = 100001  # dbo.fn_Enum_TaskBatchGrain_Hourly()

jar_path: str = 's3://ttd-build-artefacts/channels/styx/snapshots/master/latest/styx-assembly.jar'

cluster_tags = {"Team": "ADPB"}

# Bootstrap script for TLS to work on Aerospike on-prem. Imports TTD Root CA
root_ca_bootstrap_script_location = 's3://ttd-identity/datapipeline/jars/bootstrap'
root_ca_bootstrap_script = f'{root_ca_bootstrap_script_location}/import-ttd-certs.sh'
root_ca_action = ScriptBootstrapAction(root_ca_bootstrap_script, [root_ca_bootstrap_script_location])

# Cold storage bandwidth monitoring
monitoring_bootstrap_script_location = 's3://ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.8/latest/monitoring-scripts/coldstoragereader'
monitoring_bootstrap_script = f'{monitoring_bootstrap_script_location}/install-start-iftop.sh'
monitoring_script_action = ScriptBootstrapAction(
    monitoring_bootstrap_script, [monitoring_bootstrap_script_location, 'dooh_coldstorage_aerospike_network_traffic', 'dooh']
)

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

coldstorage_instance_types = [
    C6g.c6gd_4xlarge().with_fleet_weighted_capacity(4),
    C6g.c6gd_8xlarge().with_fleet_weighted_capacity(8),
    C6g.c6gd_12xlarge().with_fleet_weighted_capacity(12),
    C6g.c6gd_16xlarge().with_fleet_weighted_capacity(16),
    C5.c5d_4xlarge().with_fleet_weighted_capacity(4)
]

instance_types = [
    C6g.c6gd_4xlarge().with_fleet_weighted_capacity(4),
    C6g.c6gd_8xlarge().with_fleet_weighted_capacity(8),
    C6g.c6gd_12xlarge().with_fleet_weighted_capacity(12),
    C5.c5d_4xlarge().with_fleet_weighted_capacity(4)
]

on_target_ondemand_weighted_capacity = 480
coldstorage_ondemand_weighted_capacity = 100

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

on_target_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=on_target_ondemand_weighted_capacity
)

coldstorage_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=coldstorage_instance_types, on_demand_weighted_capacity=coldstorage_ondemand_weighted_capacity
)

on_target_cluster = EmrClusterTask(
    name="dooh_on_target_calculator_cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=on_target_core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2
)

coldstorage_lookup_cluster = EmrClusterTask(
    name="dooh_coldstorage_lookup_cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=coldstorage_core_fleet_instance_type_configs,
    ec2_subnet_ids=["subnet-0e82171b285634d41"],  # us-east-1d
    emr_managed_master_security_group="sg-008678553e48f48a3",
    emr_managed_slave_security_group="sg-02fa4e26912fd6530",
    service_access_security_group="sg-0b0581bc37bcac50a",
    bootstrap_script_actions=[root_ca_action, monitoring_script_action],
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2
)

####################################################################################################################
# operators
####################################################################################################################

job_datetime_format: str = "%Y-%m-%dT%H:%M:%S"


def get_job_run_datetime(execution_date, **kwargs) -> str:
    date = datetime.strptime(execution_date, "%Y%m%dT%H%M%S") - job_hours_latency
    datetime_str = date.strftime(job_datetime_format)
    return datetime_str


def check_incoming_data_exists(job_date_str: str, num_hours_to_process: int, **kwargs):
    for hour_delta in range(num_hours_to_process):
        job_date = datetime.strptime(job_date_str, job_datetime_format) + timedelta(hours=hour_delta)
        cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()

        logging.info(f"Checking if input data exists for {job_date}")

        if not Datasources.dooh.location_visits_deduped_v3.with_check_type('hour').check_data_exist(cloud_storage, job_date):
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

#   Fail and retry if No New Data
check_incoming_data_exists_task = OpTask(
    op=PythonOperator(
        task_id='check_incoming_data_exists',
        python_callable=check_incoming_data_exists,
        op_kwargs=dict(job_date_str=run_date_str_template, num_hours_to_process=job_num_hours_to_process),
        retries=3,
        provide_context=True
    )
)


def add_success_file(job_date_str: str, num_hours_to_process, datasource: HourGeneratedDataset, **kwargs):
    aws_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    for h in range(num_hours_to_process):
        date = job_date + timedelta(hours=h)
        path = datasource._get_full_key(date)

        logging.info(f"Adding _SUCCESS files to {datasource.bucket}/{path}")

        aws_storage.put_object(bucket_name=datasource.bucket, key=f"{path}/_SUCCESS", body="", replace=True)
    return "Success markers written"


def open_lwdb_gate_hourly(mssql_conn_id, gating_type, job_date_str: str, num_hours_to_process, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    for h in range(num_hours_to_process):
        date = job_date + timedelta(hours=h)
        log_start_time = date.strftime('%Y-%m-%d %H:00:00')

        logging.info(f"Open gate. gatingType: {gating_type}, grain: {TaskBatchGrain_Hourly}, dateTimeToOpen: {log_start_time}")

        ExternalGateOpen(
            mssql_conn_id=mssql_conn_id,
            sproc_arguments={
                'gatingType': gating_type,
                'grain': TaskBatchGrain_Hourly,
                'dateTimeToOpen': log_start_time
            }
        )


# TODO could use just one function
baseline_fill_s3_success_markers = OpTask(
    op=PythonOperator(
        task_id="baseline_fill_s3_success_markers",
        python_callable=add_success_file,
        op_kwargs=dict(
            job_date_str=run_date_str_template, num_hours_to_process=job_num_hours_to_process, datasource=Datasources.dooh.arp_baseline
        ),
        retries=3,
        provide_context=True,
        dag=dag.airflow_dag,
    )
)

report_fill_s3_success_markers = OpTask(
    op=PythonOperator(
        task_id="report_fill_s3_success_markers",
        python_callable=add_success_file,
        op_kwargs=dict(
            job_date_str=run_date_str_template, num_hours_to_process=job_num_hours_to_process, datasource=Datasources.dooh.arp_report
        ),
        retries=3,
        provide_context=True,
        dag=dag.airflow_dag,
    )
)

# Trigger Vertica import via DataMover by opening the Gate
baseline_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=open_lwdb_gate_hourly,
        provide_context=True,
        op_kwargs=dict(
            mssql_conn_id=logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            gating_type=baseline_gating_type_id,
            job_date_str=run_date_str_template,
            num_hours_to_process=job_num_hours_to_process,
        ),
        task_id="baseline_vertica_import_open_gate",
    )
)

report_vertica_import_open_gate = OpTask(
    op=PythonOperator(
        python_callable=open_lwdb_gate_hourly,
        provide_context=True,
        op_kwargs=dict(
            mssql_conn_id=logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            gating_type=report_gating_type_id,
            job_date_str=run_date_str_template,
            num_hours_to_process=job_num_hours_to_process,
        ),
        task_id="report_vertica_import_open_gate_report",
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

on_target_steps = []
arp_steps = []
coldstorage_steps = []

coldstorage_aerospike_host = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", limit=1)}}'

for hour_delta in range(job_num_hours_to_process):

    coldstorage_step = EmrJobTask(
        name="targeting_data_lookup_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.v2.targetingdatalookup.LocationVisitsTargetingDataLookupJobV2",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[('datetime', run_date_str_template), ('hourToStartDelta', f'{hour_delta}'),
                                           ("aerospikeHost", coldstorage_aerospike_host), ("aerospikePort", 4333),
                                           ("redisHost", "gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com"),
                                           ("redisPort", 6379)],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=coldstorage_lookup_cluster.cluster_specs
    )
    coldstorage_steps.append(coldstorage_step)

    on_target_step = EmrJobTask(
        name="on_target_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.v2.ontargetcalculator.OnTargetCalculatorHourlyJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('datetime', run_date_str_template), ('hourToStartDelta', f'{hour_delta}'),
            ('coldStorageCrossDeviceLookupOutputSamplingRate', '1'),
            ('coldStorageCrossDeviceLookupOutputParquetRootPath', Datasources.dooh.cold_storage_cross_device_lookup_output.get_root_path())
        ],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=on_target_cluster.cluster_specs
    )
    on_target_steps.append(on_target_step)

    backfill_step = EmrJobTask(
        name="on_target_backfill_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.v2.ontargetcalculator.OnTargetCalculatorBackFillJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('datetime', run_date_str_template), ('hourToStartDelta', f'{hour_delta}'),
            ('numberOfDaysToLookBack', number_of_days_to_lookback), ('coldStorageCrossDeviceLookupOutputSamplingRate', '1'),
            ('coldStorageCrossDeviceLookupOutputParquetRootPath', Datasources.dooh.cold_storage_cross_device_lookup_output.get_root_path())
        ],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=on_target_cluster.cluster_specs
    )

    arp_step = EmrJobTask(
        name="arp_historical_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.v2.arp.ArpReportWithHistoricalDataJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[('datetime', run_date_str_template), ('hourToStartDelta', f'{hour_delta}'),
                                           ('numberOfDaysToLookBack', number_of_days_to_lookback)],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=on_target_cluster.cluster_specs
    )
    arp_steps.append(arp_step)

    baseline_step = EmrJobTask(
        name="arp_baseline_{0}".format(hour_delta),
        executable_path=jar_path,
        class_name="jobs.dooh.v2.arp.ArpBaselineJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[('datetime', run_date_str_template), ('hourToStartDelta', f'{hour_delta}'),
                                           ('numberOfDaysToLookBack', number_of_days_to_lookback)],
        additional_args_option_pairs_list=on_target_config,
        cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
        cluster_specs=on_target_cluster.cluster_specs
    )

    on_target_step >> backfill_step >> baseline_step >> arp_step
    on_target_cluster.add_parallel_body_task(on_target_step)
    on_target_cluster.add_parallel_body_task(backfill_step)
    on_target_cluster.add_parallel_body_task(baseline_step)
    on_target_cluster.add_parallel_body_task(arp_step)

    coldstorage_lookup_cluster.add_parallel_body_task(coldstorage_step)

dag >> get_job_run_datetime_task >> check_incoming_data_exists_task >> coldstorage_lookup_cluster >> on_target_cluster >> baseline_fill_s3_success_markers >> report_fill_s3_success_markers >> baseline_vertica_import_open_gate >> report_vertica_import_open_gate

for hour in range(job_num_hours_to_process):

    # Don't start next hour until previous hour is complete
    if hour > 0:
        coldstorage_steps[hour - 1] >> coldstorage_steps[hour]
        arp_steps[hour - 1] >> on_target_steps[hour]

adag = dag.airflow_dag
