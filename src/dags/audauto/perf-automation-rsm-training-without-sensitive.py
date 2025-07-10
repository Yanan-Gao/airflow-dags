import time
from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask, DockerSetupBootstrapScripts
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
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
spark_options_list = [("executor-memory", "100G"), ("executor-cores", "16"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=100G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
                      ("conf", "spark.default.parallelism=%s" % num_partitions), ("conf", "spark.driver.maxResultSize=50G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25"), ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0")]

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
docker_image_tag = "1998528"

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = "#dev-perf-auto-alerts-rsm"
    slack_tags = AUDAUTO.team.sub_team
else:
    slack_channel = "#scrum-perf-automation-alerts-testing"
    slack_tags = None

environment = TtdEnvFactory.get_from_system()
model = "RSM"
docker_install_script = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/install_docker_gpu_new.sh"
bootstrap_script_configuration = DockerSetupBootstrapScripts(install_docker_gpu_location=docker_install_script)

AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"

rsm_training_dag = TtdDag(
    dag_id="perf-automation-rsm-training-without-sensitive",
    start_date=datetime(2024, 8, 6, 2, 0),
    schedule_interval=timedelta(days=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO", "RSM"]
)

adag = rsm_training_dag.airflow_dag
###############################################################################
# Sensors
###############################################################################
rsm_etl_dag_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="rsm_etl_sensor",
        external_dag_id="perf-automation-rsm-etl",
        external_task_id=None,  # wait for the entire DAG to complete
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
        timeout=15 * 60 * 60,  # timeout in seconds - wait 15 hours
        mode="reschedule",  # release the worker slot between pokes
        dag=adag
    )
)

# need all the oos job to complete before metric preparation
rsm_oos_etl_dag_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="rsm_oos_etl_sensor",
        external_dag_id="perf-automation-rsm-oos-etl",
        external_task_id=None,  # wait for the entire DAG to complete
        allowed_states=[TaskInstanceState.SUCCESS, TaskInstanceState.FAILED],
        check_existence=False,
        poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
        timeout=12 * 60 * 60,  # timeout in seconds - wait 12 hours
        mode="reschedule",  # release the worker slot between pokes
        dag=adag
    )
)

###############################################################################
# cluster
###############################################################################
rsm_training_cluster_task = DockerEmrClusterTask(
    name="RelevanceScoreModelTraining",
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
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    environment=environment,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=2 * 60 * 60,
    entrypoint_in_image="/opt/application/jobs/",
    builtin_bootstrap_script_configuration=bootstrap_script_configuration
)

###############################################################################
# steps
###############################################################################
env = environment.execution_env
date_str = "{{ data_interval_start.strftime(\"%Y%m%d000000\") }}"
policy_table_path = "./input/policy_table/"
model_output_path = f"s3://thetradedesk-mlplatform-us-east-1/models/{env}/"
embedding_output_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/{env}/audience/embedding/"
incremental_training_enabled = "true"
full_train_start_date = 20240514
incremental_training_cadence = 7

training_argument_list = [
    '--run_train', f'--env={env}', f'--date={date_str}', '--model_type=RSM', f'--policy_table_path="{policy_table_path}"',
    f'--model_output_path_s3="{model_output_path}"', f'--embedding_output_path_s3="{embedding_output_path}"',
    f'--incremental_training_enabled={incremental_training_enabled}', f'--full_train_start_date={full_train_start_date}',
    f'--incremental_training_cadence={incremental_training_cadence}'
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
rsm_training_cluster_task.add_parallel_body_task(model_training_step)

# prepare_metric_step = PySparkEmrTask(
#     name="MetricsPreparation",
#     entry_point_path="/home/hadoop/app/prepare_metric.py",
#     docker_registry=docker_registry,
#     image_name=docker_image_name,
#     image_tag=docker_image_tag,
#     additional_args_option_pairs_list=spark_options_list,
#     command_line_arguments=[
#         f"--env={env}", f"--date={date_str}", f'--policy_table_path={policy_table_path}', f'--model_output_path_s3="{model_output_path}"',
#         f"--incremental_training_enabled={incremental_training_enabled}", f"--full_train_start_date={full_train_start_date}",
#         f"--incremental_training_cadence={incremental_training_cadence}"
#     ],
#     timeout_timedelta=timedelta(hours=12),
# )
# rsm_training_cluster_task.add_parallel_body_task(prepare_metric_step)

###############################################################################
# Modify S3 file operators
###############################################################################

# for incremental model training
update_full_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{env}/RSM/experiment/sensitive/withoutsensitive/_CURRENT",
        date=date_str,
        append_file=False,
        dag=adag,
    )
)

delay_task = OpTask(op=PythonOperator(task_id="delay_task", python_callable=lambda: time.sleep(3600), dag=adag))

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
rsm_training_dag >> rsm_etl_dag_sensor >> rsm_training_cluster_task
rsm_training_dag >> rsm_oos_etl_dag_sensor >> rsm_training_cluster_task
(model_training_step >> delay_task >> update_full_model_current_file_task >> final_dag_status_step)
