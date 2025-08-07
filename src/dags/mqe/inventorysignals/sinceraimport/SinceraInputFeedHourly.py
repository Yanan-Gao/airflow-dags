from datasources.sources.avails_datasources import AvailsDatasources
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

from ttd.slack import slack_groups
from datetime import timedelta, datetime

from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2025, 6, 10)
job_schedule_interval = "1 * * * *"
max_retries: int = 3
retry_delay: timedelta = timedelta(minutes=30)

job_environment = TtdEnvFactory.get_from_system()

dag_id = "mqe-sincera-input-feed-hourly"
if job_environment != TtdEnvFactory.prod:
    dag_id = "mqe-sincera-input-feed-hourly-test"

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
    name="SinceraInputFeedHourly",
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
job_class_name = "jobs.inventorysignals.sinceraimport.v2.SinceraDataImportInputFeedHourly"
datetime_macro = """{{ data_interval_start.strftime("%Y-%m-%dT%H:00") }}"""
eldorado_config_option_pairs_list = [("runtime", datetime_macro), ("env", job_environment.execution_env)]

spark_options_list = [("executor-memory", "15G"), ("executor-cores", "10"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=40G"),
                      ("conf", "spark.driver.maxResultSize=6G"), ("conf", f"spark.sql.shuffle.partitions={core_fleet_capacity}")]

job_task = EmrJobTask(
    name="SinceraInputFeedHourly",
    class_name=job_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)
dataset_check = OpTask(
    op=DatasetCheckSensor(
        dag=adag,
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}",
        datasets=[AvailsDatasources.full_url_agg_hourly_dataset.with_check_type("hour").with_region(aws_region)],
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 8,  # 8 hours
    )
)

cluster_task.add_parallel_body_task(job_task)

dag >> dataset_check >> cluster_task
