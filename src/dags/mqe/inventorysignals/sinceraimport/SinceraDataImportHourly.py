import logging
from datetime import timedelta, datetime

from airflow.sensors.python import PythonSensor

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2025, 6, 10)
job_schedule_interval = "55 * * * *"
max_retries: int = 3
retry_delay: timedelta = timedelta(minutes=30)

job_environment = TtdEnvFactory.get_from_system()


def check_sincera_output(run_date_time_arg: str, **kwargs):
    """
       Checks if data is available.
    """

    run_date_time = datetime.strptime(run_date_time_arg, "%Y-%m-%dT%H:%M")
    date_str = run_date_time.strftime('%Y%m%d')
    hour_str = run_date_time.strftime('%H')
    sincera_bucket = "customer-thetradedesk-sync"
    wildcard_keys = [
        f"950/inbound/UrlFeed_{date_str}_{hour_str}.csv.gz", f"950/outbound/UrlFeed_{date_str}_{hour_str}*.parquet",
        f"953/outbound/*UrlFeed_{date_str}_{hour_str}*.parquet"
    ]
    logging.info(run_date_time_arg)
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    for wildcard_key in wildcard_keys:
        if aws_storage.check_for_wildcard_key(bucket_name=sincera_bucket, wildcard_key=wildcard_key):
            logging.info(f"Found file {wildcard_key}")
        else:
            logging.info(f"File not ready {wildcard_key}")
            return False
    logging.info("All files found!")
    return True


dag_id = "mqe-sincera-data-import-hourly"
if job_environment != TtdEnvFactory.prod:
    dag_id = "mqe-sincera-data-import-hourly-test"

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_groups.mqe.alarm_channel,
    slack_tags=slack_groups.mqe.name,
    retries=max_retries,
    retry_delay=retry_delay,
    tags=["MQE", "Sincera"],
    enable_slack_alert=True
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(128)],
    on_demand_weighted_capacity=1,
)

core_fleet_capacity = 2
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_16xlarge().with_fleet_weighted_capacity(2).with_ebs_size_gb(128)],
    on_demand_weighted_capacity=core_fleet_capacity,
)

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
aws_region = "us-east-1"

cluster_task = EmrClusterTask(
    name="SinceraDataImportHourly",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.mqe.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    environment=job_environment,
    emr_release_label=emr_release_label,
    region_name=aws_region
)

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar"
job_class_name = "jobs.inventorysignals.sinceraimport.v2.SinceraDataImportHourly"
datetime_macro = """{{ data_interval_start.strftime("%Y-%m-%dT%H:00") }}"""
eldorado_config_option_pairs_list = [("runtime", datetime_macro), ("env", job_environment.execution_env)]

job_task = EmrJobTask(
    name="SinceraDataImportHourly",
    class_name=job_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)
sincera_dataset_op = OpTask(
    op=PythonSensor(
        task_id="check_sincera_dataset",
        dag=adag,
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 10,  # wait up to 10 hours,
        mode="reschedule",
        python_callable=check_sincera_output,
        op_kwargs={"run_date_time_arg": datetime_macro},
    ),
)

cluster_task.add_parallel_body_task(job_task)

dag >> sincera_dataset_op >> cluster_task
