import time
from datetime import timedelta, datetime
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.graphics_optimized.g4 import G4
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import (
    DockerEmrClusterTask,
    DockerCommandBuilder,
    DockerRunEmrTask,
    DockerSetupBootstrapScripts,
)
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
num_executors = 100
num_partitions = int(round(3 * num_executors)) * 30
spark_options_list = [
    ("executor-memory", "100G"),
    ("executor-cores", "16"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=100G"),
    ("conf", "spark.driver.cores=15"),
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % num_partitions),
    ("conf", "spark.driver.maxResultSize=50G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.memory.storageFraction=0.25"),
    ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0"),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    },
}]

# Docker config
docker_registry = "production.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-audauto/audience_models"
docker_image_tag = "latest"

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = "#dev-perf-auto-alerts-rsm"
    slack_tags = AUDAUTO.team.sub_team
else:
    slack_channel = "#scrum-perf-automation-alerts-testing"
    slack_tags = None

environment = TtdEnvFactory.get_from_system()
docker_install_script = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/audience-models/prod/latest/install_docker_gpu_new.sh"
bootstrap_script_configuration = DockerSetupBootstrapScripts(install_docker_gpu_location=docker_install_script)

erm_embedding_dag = TtdDag(
    dag_id="perf-automation-erm-embedding",
    start_date=datetime(2024, 8, 20, 2, 0),
    schedule_interval=timedelta(days=1),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO", "RSM"],
)

adag = erm_embedding_dag.airflow_dag

###############################################################################
# Sensors
###############################################################################
# data sensor for the adgroup, campaign, campaignseed and campaignpacingsettings
# do not need to check policy table cuz one rsm emb available then policy table availabe
rsm_emb = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata",
    data_name="prod/audience/embedding/RSM",
    version=1,
    date_format="%Y%m%d000000",
    env_aware=False,
    success_file='_SUCCESS',
)

adgroup_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/adgroup",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
    success_file=None,
)

campaign_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/campaign",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
    success_file=None,
)

campaignpacingsettings_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/campaignpacingsettings",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
    success_file=None,
)

campaignseed_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/campaignseed",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
    success_file=None,
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="data_available",
        datasets=[
            rsm_emb,
            adgroup_data,
            campaign_data,
            campaignpacingsettings_data,
            campaignseed_data,
        ],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

###############################################################################
# cluster
###############################################################################
erm_embedding_cluster_task = DockerEmrClusterTask(
    name="ERMEmbeddingGeneration",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G4.g4dn_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G4.g4dn_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    environment=environment,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=2 * 60 * 60,
    entrypoint_in_image="/opt/application/jobs/",
    builtin_bootstrap_script_configuration=bootstrap_script_configuration,
)

###############################################################################
# steps
###############################################################################
env = environment.execution_env
date_str = '{{ data_interval_start.strftime("%Y%m%d000000") }}'
policy_table_path = "./input/policy_table/"
rsm_embedding_input_path_s3 = f"s3://thetradedesk-mlplatform-us-east-1/configdata/{env}/audience/embedding/RSM/v=1/"
erm_embedding_output_path_s3 = f"s3://thetradedesk-mlplatform-us-east-1/configdata/{env}/audience/embedding/ERM/v=1/"

argument_list = [
    f"--env={env}",
    f"--date={date_str}",
    f'--policy_table_path="{policy_table_path}"',
    f'--rsm_embedding_input_path_s3="{rsm_embedding_input_path_s3}"',
    f'--erm_embedding_output_path_s3="{erm_embedding_output_path_s3}"',
]

erm_embedding_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/erm_embedding.py",
    additional_execution_parameters=argument_list,
)

erm_embedding_step = DockerRunEmrTask(
    name="ERMEmbeddingGeneration",
    docker_run_command=erm_embedding_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)
erm_embedding_cluster_task.add_parallel_body_task(erm_embedding_step)

###############################################################################
# Modify S3 file operators
###############################################################################

# for incremental model training
update_erm_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_erm_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{env}/audience/embedding/ERM/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

delay_task = OpTask(op=PythonOperator(task_id="delay_task", python_callable=lambda: time.sleep(300), dag=adag))

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
(erm_embedding_dag >> dataset_sensor >> erm_embedding_cluster_task >> delay_task >> update_erm_current_file_task >> final_dag_status_step)
