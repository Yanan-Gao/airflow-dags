from datetime import timedelta, datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack import slack_groups
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2025, 7, 26, 23)
job_schedule_interval = "@hourly"
max_retries: int = 0
retry_delay: timedelta = timedelta(minutes=30)

job_environment = TtdEnvFactory.get_from_system()

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar"
job_class_name = "jobs.inventorysignals.sinceradiffcache.SinceraUrlMetricCacheExporter"
datetime_macro = """{{ data_interval_end.strftime("%Y-%m-%dT%H:00") }}"""
eldorado_config_option_pairs_list = [("runtime", datetime_macro), ("env", job_environment.execution_env),
                                     ("ttd.ds.UrlMetricDataset.isInChain", "true")]

dag_id = "mqe-sincera-url-metric-cache-export-hourly"

a2cr_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-inventory-quality",
    path_prefix="sincera",
    data_name="a2cr",
    env_path_configuration=MigratedDatasetPathConfiguration(),
    version=1,
)

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_groups.mqe.alarm_channel,
    slack_tags=slack_groups.mqe.name,
    retries=max_retries,
    retry_delay=retry_delay,
    tags=["MQE", "Sincera"],
    enable_slack_alert=True,
    run_only_latest=False,
    depends_on_past=True
)

adag = dag.airflow_dag
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(128)],
    on_demand_weighted_capacity=1,
)

core_fleet_capacity = 4
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_16xlarge().with_fleet_weighted_capacity(2).with_ebs_size_gb(128)],
    on_demand_weighted_capacity=core_fleet_capacity,
)
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
aws_region = "us-east-1"

cluster_task = EmrClusterTask(
    name="SinceraUrlMetricCacheExportHourly",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.mqe.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    environment=job_environment,
    emr_release_label=emr_release_label,
    region_name=aws_region
)

job_task = EmrJobTask(
    name="SinceraUrlMetricCacheExportHourly",
    class_name=job_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)

a2cr_dataset_op = OpTask(
    op=DatasetCheckSensor(
        dag=adag,
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}",
        datasets=[a2cr_dataset.with_check_type("hour")],
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 10,
    )
)
cluster_task.add_parallel_body_task(job_task)

dag >> a2cr_dataset_op >> cluster_task
