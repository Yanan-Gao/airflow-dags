from datetime import timedelta, datetime

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    }
}]

# Docker config
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-audauto/audience_models"
docker_image_tag = "1989902"  # for 80 percentile threshold docker

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = "#dev-perf-auto-alerts-rsm"
    slack_tags = AUDAUTO.team.sub_team
else:
    slack_channel = "#scrum-perf-automation-alerts-testing"
    slack_tags = None

environment = TtdEnvFactory.get_from_system()

erm_training_dag = TtdDag(
    dag_id="perf-automation-erm-training",
    start_date=datetime(2024, 7, 20, 2, 0),
    schedule_interval=timedelta(days=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=True,
    tags=["AUDAUTO", "ERM"]
)

adag = erm_training_dag.airflow_dag

###############################################################################
# Sensors
###############################################################################
rsm_etl_dag_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="erm_etl_sensor",
        external_dag_id="perf-automation-erm-etl",
        external_task_id=None,  # wait for the entire DAG to complete
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
        timeout=15 * 60 * 60,  # timeout in seconds - wait 15 hours
        mode="reschedule",  # release the worker slot between pokes
        dag=adag
    )
)

###############################################################################
# cluster
###############################################################################
erm_training_cluster_task = DockerEmrClusterTask(
    name="ERMTraining",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G5.g5_16xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G5.g5_16xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    environment=environment,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=2 * 60 * 60,
    entrypoint_in_image="/opt/application/jobs/"
)

###############################################################################
# steps
###############################################################################
env = environment.execution_env
date_str = "{{ data_interval_start.strftime(\"%Y%m%d000000\") }}"
policy_table_path = "./input/policy_table/"
# training.py adds model type and version to each output path when writing to S3
if env == 'prod':
    env1 = 'dev'
else:
    env1 = 'prodTest'
model_output_path = f"s3://thetradedesk-mlplatform-us-east-1/features/data/firstPartyPixel/v=1/{env1}/models/"
incremental_training_enabled = "false"
full_train_start_date = 20240514
incremental_training_cadence = 7

training_argument_list = [
    "--run_train", f"--env={env}", f"--date={date_str}", "--model_type=AEM", f'--policy_table_path="{policy_table_path}"',
    f'--model_output_path_s3="{model_output_path}"', f"--incremental_training_enabled={incremental_training_enabled}",
    f"--full_train_start_date={full_train_start_date}", f"--incremental_training_cadence={incremental_training_cadence}"
]

training_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/main.py",
    additional_parameters=["--gpus all", "-e TF_GPU_THREAD_MODE=gpu_private"],
    additional_execution_parameters=training_argument_list,
)

model_training_step = DockerRunEmrTask(
    name="ModelTraining", docker_run_command=training_docker_command.build_command(), timeout_timedelta=timedelta(hours=12)
)
erm_training_cluster_task.add_parallel_body_task(model_training_step)

###############################################################################
# Modify S3 file operators
###############################################################################
update_policy_table_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_policy_table_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{env}/audience/policyTable/AEM/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

# for incremental model training
update_full_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"features/data/firstPartyPixel/v=1/{env1}/models/_CURRENT",
        date="{{ data_interval_start.strftime(\"%Y%m%d\") }}",
        append_file=False,
        dag=adag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
erm_training_dag >> rsm_etl_dag_sensor >> erm_training_cluster_task
model_training_step >> update_policy_table_current_file_task >> update_full_model_current_file_task >> final_dag_status_step
