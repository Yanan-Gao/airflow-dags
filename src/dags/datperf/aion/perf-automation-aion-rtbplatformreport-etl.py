from datetime import datetime, timedelta

from dags.datperf.datasets import rtbplatformreport_dataset
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, PySparkEmrTask
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.python_wheel_databricks_task import PythonWheelDatabricksTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.eldorado.base import TtdDag

from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

DATE = '{{ data_interval_start.strftime("%Y%m%d") }}'

ENV = TtdEnvFactory.get_from_system().dataset_write_env
OUTPUT_PATH = f"s3://thetradedesk-mlplatform-us-east-1/env={ENV}/value-pacing-aion/data/"

TEST_WHL_BRANCH = "kws-DATPERF-6293-RTBPlatformReport-Airflow"
WHL_ENV = f'dev/{TEST_WHL_BRANCH}' if ENV == 'test' else ENV
WHEEL_PATH = f"s3://thetradedesk-mlplatform-us-east-1/libs/aion/{WHL_ENV}/latest/value_pacing_aion-0.0.0-py3-none-any.whl"
SLACK_ALERT = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/aion"
docker_image_tag = "2986754"

rtbplatformreport_etl_dag: TtdDag = TtdDag(
    dag_id="perf-automation-aion-rtbplatformreport-etl",
    start_date=datetime(2025, 4, 3),
    schedule_interval="0 3 * * *",  # every day at 3 am
    max_active_runs=1,
    retry_delay=timedelta(minutes=20),
    slack_tags="DATPERF",
    slack_channel="#scrum-perf-automation-alerts-testing" if ENV == 'test' else '#pod-aion-alerts',
    slack_alert_only_for_prod=False
)

dag = rtbplatformreport_etl_dag.airflow_dag

daily_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='daily_dataset_sensor',
        poke_interval=60 * 30,
        timeout=60 * 60 * 12,
        ds_date="{{ data_interval_start.to_datetime_string() }}",
        datasets=[rtbplatformreport_dataset]
    )
)

rtbplatformreport_task = PythonWheelDatabricksTask(
    package_name="value_pacing_aion",
    entry_point="rtb_platform_report_etl",
    parameters=[f"--env={ENV}", f"--date={DATE}", f"--output_path={OUTPUT_PATH}"],
    job_name="rtb_platform_report_etl",
    whl_paths=[WHEEL_PATH]
)

db_workflow = DatabricksWorkflow(
    job_name="perf-automation-aion-rtbplatformreport-etl",
    cluster_name="perf-automation-aion-rtbplatformreport-etl-cluster",
    cluster_tags={"Team": "DATPERF"},
    worker_node_type="r5d.8xlarge",
    worker_node_count=20,
    databricks_spark_version="15.4.x-scala2.12",
    tasks=[rtbplatformreport_task],
    ebs_config=DatabricksEbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=2
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

cluster_spec = EmrClusterSpecs(
    cluster_name="rtb_platform_report_etl_emr_cluster",
    environment=TtdEnvFactory.get_from_system(),
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
)

additional_spark_options_list_etl = [
    ("executor-memory", "8G"),
    ("executor-cores", "1"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=100G"),
    ("conf", "spark.driver.cores=32"),
    ("conf", "spark.sql.shuffle.partitions=5000"),
    ("conf", "spark.default.parallelism=5000"),
    ("conf", "spark.driver.maxResultSize=6G"),
    ("conf", "spark.executor.memoryOverhead=15G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    }
}]

etl_cluster = DockerEmrClusterTask(
    name="aion_rtb_platform_report_etl-cluster",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="/usr/lib/app/",
    cluster_tags={"Team": DATPERF.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    additional_application_configurations=application_configuration,
)

etl_task = PySparkEmrTask(
    name="etl_task",
    entry_point_path="/home/hadoop/app/aion/etl/jobs/export_rtbplatformreports.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=additional_spark_options_list_etl,
    command_line_arguments=[f"--env={ENV}", f"--date={DATE}", f"--output_path={OUTPUT_PATH}"],
    timeout_timedelta=timedelta(hours=18),
)

etl_cluster.add_parallel_body_task(etl_task)

###############################################################################
# Dependencies
###############################################################################

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

rtbplatformreport_etl_dag >> daily_dataset_sensor >> db_workflow >> final_dag_status_step
# rtbplatformreport_etl_dag >> etl_cluster >> final_dag_status_step
