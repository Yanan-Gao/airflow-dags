from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.interop.logworkflow_callables import ExecuteOnDemandDataMove
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_environment = TtdEnvFactory.get_from_system()
env_str = "prod" if job_environment == TtdEnvFactory.prod else "test"

brandlift_ttd_dag = TtdDag(
    dag_id="brand-lift-dooh-exposure-lat-long",
    start_date=datetime(2024, 9, 11, 0, 0),
    schedule_interval='0 6 * * *',
    depends_on_past=False,
    run_only_latest=True,
    slack_channel=slack_groups.MEASUREMENT_UPPER.team.alarm_channel,
    tags=['measurement']
)

###############################################################################
# Data Mover Gate open task
###############################################################################

# LWDB configs
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"
logworkflow_connection_open_gate = logworkflow_connection if job_environment == TtdEnvFactory.prod \
    else logworkflow_sandbox_connection

datamover_open_gate_export_latlong_vertica = OpTask(
    op=PythonOperator(
        dag=brandlift_ttd_dag.airflow_dag,
        python_callable=ExecuteOnDemandDataMove,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'taskId': 1000605,  # dbo.fn_Enum_Task_ExportBrandLiftDoohLatLongReport()
                'prefix': 'date={{ data_interval_start.strftime("%Y%m%d") }}/',
                'args': {
                    'StartDateUtcInclusive': '{{ data_interval_start.strftime("%Y-%m-%d") }}',
                    'EndDateUtcExclusive': '{{ data_interval_end.strftime("%Y-%m-%d") }}'
                }
            }
        },
        task_id="logworkflow_trigger_export_latlong_vertica",
    )
)

###############################################################################
# S3 dataset sources
###############################################################################

exported_latlong_dooh_impression_data = DateGeneratedDataset(
    bucket="ttd-brandlift",
    path_prefix="env=prod/dooh/exposure/staging",
    data_name="latlong/v=1/ExportBrandLiftDoohLatLongReport/VerticaAws",
    date_format="date=%Y%m%d",
    version=None,
    env_aware=False,
    success_file="_SUCCESS",
)

#  Decided not to add a backup step due to the following reasons
#  1. It is unlikely that rerunning the data mover over the same time range will result in differences in exported data.
#  2. It is easy to jump in a clear the staging directory manually.
#  3. To rerun downstream tasks, instead of clearing the DAG, clear the sensor/emr-cluster task instead to rerun downstream tasks
#  if we want to add a backup step, do the following:
#  1. add get, put, delete permissions for s3://ttd-brandlift here:
#  https://gitlab.adsrvr.org/thetradedesk/teams/cloud/infradesc/-/blob/master/teams/data-processing/airflow.jsonnet?ref_type=heads#L36
#  2. Uncomment code below, and add backup_previous_run_datamover as the very first step

# backup_exported_latlong_dooh_impression_data = DateGeneratedDataset(
#     bucket="ttd-brandlift",
#     path_prefix=f"env={env_str}/dooh/exposure/staging-backup",
#     data_name="latlong/v=1/ExportBrandLiftDoohLatLongReport/VerticaAws",
#     date_format="date=%Y%m%d",
#     version=None,
#     env_aware=False
# )
#
# backup_previous_run_datamover = DatasetTransferTask(
#     name="backup_previous_run_datamover",
#     dataset=exported_latlong_dooh_impression_data,
#     src_cloud_provider=CloudProviders.aws,
#     dst_cloud_provider=CloudProviders.aws,
#     dst_dataset=backup_exported_latlong_dooh_impression_data,
#     partitioning_args=exported_latlong_dooh_impression_data.get_partitioning_args(ds_date="{{ data_interval_start.strftime('%Y-%m-%d') }}"),
#     transfer_timeout=timedelta(hours=2),
# )

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="export_vertica_latlong_data_complete",
        datasets=[exported_latlong_dooh_impression_data],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60,
        timeout=60 * 60 * 3,  # 2 hours
    )
)

###############################################################################
# clusters
###############################################################################

brandlift_cluster_tags = {
    "process": "brandlift",
    "Team": slack_groups.MEASUREMENT_UPPER.team.jira_team,
}

brandlift_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

brandlift_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_2xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=3, spot_weighted_capacity=0
)

brandlift_main_cluster_task = EmrClusterTask(
    name="brandlift_dooh_exposure_lat_long_main_cluster",
    core_fleet_instance_type_configs=brandlift_core_fleet_instance_type_configs,
    master_fleet_instance_type_configs=brandlift_master_fleet_instance_type_configs,
    cluster_tags=brandlift_cluster_tags,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    log_uri=f's3://ttd-brandlift/env={env_str}/dooh/exposure/airflow-logs/latlong/',
    enable_prometheus_monitoring=True,
    additional_application_configurations=[
        {
            'Classification': 'spark',
            'Properties': {
                'maximizeResourceAllocation': 'true'
            }
        },
    ]
)

spark_config = [("conf", "spark.sql.files.maxRecordsPerFile=1000000"), ("conf", "spark.sql.adaptive.enabled=true"),
                ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true")]

eldorado_config = [
    # When running in prod test, uncomment below to read from test and write to test
    # otherwise it defaults to read from prod, write to test
    # start
    # ('ttd.BrandLiftDoohLatLongInputDataSet.isInChain', 'true'),
    # end
    ('ttd.brandlift.latlongtransform.dataIntervalStartDate', "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"),
    ('ttd.brandlift.latlongtransform.coalesceToNumFiles', "10"),
    ('ttd.BrandLiftDoohLatLongOutputDataSet.outFormat', "csv"),
]

brandlift_main_step = EmrJobTask(
    name="brandlift_dooh_exposure_lat_long_transform",
    class_name="com.thetradedesk.jobs.brandlift.LatLongTransform",
    additional_args_option_pairs_list=spark_config,
    eldorado_config_option_pairs_list=eldorado_config,
    timeout_timedelta=timedelta(hours=4),
    configure_cluster_automatically=True,
    executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar",
)

###############################################################################
# DAG structure
###############################################################################

brandlift_main_cluster_task.add_sequential_body_task(brandlift_main_step)

brandlift_ttd_dag >> datamover_open_gate_export_latlong_vertica >> dataset_sensor >> brandlift_main_cluster_task

DAG = brandlift_ttd_dag.airflow_dag
