from typing import List

import copy
from datetime import datetime, timedelta

from airflow.sensors.external_task_sensor import ExternalTaskSensor

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import (
    DockerEmrClusterTask,
    DockerCommandBuilder,
    DockerRunEmrTask,
    PySparkEmrTask,
)
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.general_purpose.m4 import M4
from ttd.ec2.emr_instance_types.graphics_optimized.g4 import G4
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.ttdenv import TtdEnvFactory
from dags.audauto.utils import utils

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    },
}]


def execution_date_format_fn(format="%Y%m%d"):
    """returns a function for calculating dates using the given format"""

    def calc_execution_date(add_days=1, add_hours=0):
        return (
            '{{ data_interval_start.add(days=$days_interval$).add(hours=$hours_interval$).strftime("$format$") }}'
            .replace("$days_interval$", str(add_days)).replace("$hours_interval$", str(add_hours)).replace("$format$", format)
        )

    return calc_execution_date


cluster_tags = {
    "Team": AUDAUTO.team.jira_team,
}

# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
python_date_format = "%Y%m%d"
python_date_str = execution_date_format_fn()
environment = TtdEnvFactory.prod
promote_to_rollout = True

# Jar
kongming_jar = ("s3://thetradedesk-mlplatform-us-east-1/libs/kongming/jars/prod/kongming_production.jar")

docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
pytorch_docker_image_name = "ttd-base/audauto/kongming_lantern"
pytorch_docker_image_tag = "release"
docker_image_name = "ttd-base/audauto/kongming"
docker_image_tag = "release"

# Training Configs
# needed for isolated model
train_set = "full_roas_isolate"
experiment = "roas_isolate_test"
# train_set = "full_roas"
instance_type = "emr_roas"
model_date = python_date_str(0)
extended_features: List[str] = []
extended_features_flag = ",".join(extended_features)
task = "roas"
trainset_version = 2

# Scoring Configs
num_days_to_score = 7
scoring_batch_size = 65536
virtual_cores = 80

calibration_weighted_capacity = 100

#######################################################
# End of setting area
#######################################################

#######################################################
# Code Area
#######################################################

# Airflow inputs

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = "#dev-perf-auto-alerts-cpa-roas"
    slack_tags = AUDAUTO.team.sub_team
    enable_slack_alert = True
else:
    slack_channel = "#scrum-perf-automation-alerts-testing"
    slack_tags = None
    enable_slack_alert = True

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
roas_training_dag = TtdDag(
    dag_id="perf-automation-roas-train-isolate",
    start_date=datetime(2024, 11, 1),
    schedule_interval=utils.schedule_interval,
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    retries=2,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    run_only_latest=False,
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
)
dag = roas_training_dag.airflow_dag

# need all the raw data to complete before training
roas_etl_dag_sensor = ExternalTaskSensor(
    task_id="roas_etl_sensor",
    external_dag_id="perf-automation-roas-etl",
    external_task_id=None,  # wait for the entire DAG to complete
    allowed_states=["success"],
    check_existence=False,
    poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
    timeout=24 * 60 * 60,  # timeout in seconds - wait 24 hours
    mode="reschedule",  # release the worker slot between pokes
    dag=dag,
)


def create_single_master_instance_docker_cluster(
    cluster_name: str,
    docker_registry: str,
    docker_image_name: str,
    docker_image_tag: str,
    instance_type: EmrInstanceType,
    worker_instance_type: EmrInstanceType = M4.m4_large(),
):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[instance_type.with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[worker_instance_type.with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )

    return DockerEmrClusterTask(
        name=cluster_name,
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        environment=environment,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(application_configuration),
    )


# Model Training Step
model_training_cluster = create_single_master_instance_docker_cluster(
    cluster_name="ROASModelTrainingIsolate",
    docker_registry=docker_registry,
    docker_image_name=pytorch_docker_image_name,
    docker_image_tag=pytorch_docker_image_tag,
    instance_type=G5.g5_16xlarge(),
)
training_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=pytorch_docker_image_name,
    execution_language="python3.10",
    docker_image_tag=pytorch_docker_image_tag,
    # needed for isolated model
    path_to_app="/opt/application/app/training_roas_isolate.py",
    # path_to_app="/opt/application/app/training_roas.py",
    additional_parameters=[
        "--gpus all",
        "--ipc=host",
        "-e PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True",
        "-e TF_GPU_THREAD_MODE=gpu_private",
        "-e NVIDIA_DISABLE_REQUIRE=1",
    ],
    additional_execution_parameters=[
        f"train_set={train_set}",
        f"instance={instance_type}",
        f"instance.env={environment}",
        f"instance.model_creation_date={model_date}",
        "instance.push_summary_stats=False",
        "train_set.save_reshaped_single_tf_model=True",
        "train_set.mlflow_registry_test_name=stc-isolate-test",
        f"instance.experiment={experiment}",
        "+training.num_retrain=0",
        "hyperparameter.activation=elu",
        "hyperparameter.concat_mlp_units=[64,2]",
    ],
)
model_training_task = DockerRunEmrTask(
    name="ModelTrainingIsolate",
    docker_run_command=training_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=72),
)
model_training_cluster.add_parallel_body_task(model_training_task)

# Scoring Step
scoring_cluster = create_single_master_instance_docker_cluster(
    cluster_name="ROASScoringIsolate",
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    instance_type=R5.r5_24xlarge().with_fleet_weighted_capacity(6),
    worker_instance_type=R5.r5_xlarge(),
)


def generate_scoring_task_for_date(model_date: str, scoring_date: str, index: int):
    scoring_docker_command = DockerCommandBuilder(
        docker_registry=docker_registry,
        docker_image_name=docker_image_name,
        docker_image_tag=docker_image_tag,
        execution_language="python3.10",
        path_to_app='/opt/application/app/main.py',
        additional_execution_parameters=[
            f"--env={environment}",
            "--run_score=true",
            f"--model_creation_date={model_date}",
            "--model_path=./input/model/",
            f"--extended_features={extended_features_flag}",
            f"--score_dates={scoring_date}",
            f"--task={task}",
            # Read model from prod/experiment=xxx, policy and scoring dataset from prod
            "--ttd_AdGroupPolicyMappingDataset_experimentName=",
            "--ttd_DailyOfflineScoringDataset_experimentName=",
            f"--experiment={experiment}",
            f"--scoring_batch_size={scoring_batch_size}",
            f"--virtual_cores={virtual_cores}",
        ]
    )
    return DockerRunEmrTask(
        name=f"Scoring_{index}",
        docker_run_command=scoring_docker_command.build_command(),
        timeout_timedelta=timedelta(hours=1),
    )


scoring_tasks: List[str] = []


def generate_scoring_tasks(model_date: str, scoring_dates: List[str]):
    for index in range(len(scoring_dates)):
        scoring_task = generate_scoring_task_for_date(model_date=model_date, scoring_date=scoring_dates[index], index=index)
        if len(scoring_tasks) != 0:
            scoring_tasks[-1] >> scoring_task

        scoring_tasks.append(scoring_task)
        scoring_cluster.add_parallel_body_task(scoring_task)


# todo: check if enough data exists
scoring_dates = [python_date_str(-1 * day) for day in range(num_days_to_score)]
generate_scoring_tasks(model_date=model_date, scoring_dates=scoring_dates)
# scoring_sub_clusters = generate_scoring_subdag()

# Calibration Step
calibration_cluster = utils.create_calculation_docker_cluster(
    cluster_name="ROASModelCalibrationIsolate",
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    master_instance_type=G4.g4dn_8xlarge(),
    weighted_capacity=calibration_weighted_capacity,
    entrypoint_in_image="opt/application/app/"
)
calibrate_model_task = PySparkEmrTask(
    name="ModelCalibration",
    entry_point_path="/home/hadoop/app/calibration_roas.py",
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    additional_args_option_pairs_list=utils.get_spark_options_list(int(round(3 * calibration_weighted_capacity)) * 30),
    command_line_arguments=[
        f"--env={environment}",
        f"--date={model_date}",
        f"--model_creation_date={model_date}",
        f"--task={task}",
        f"--training_cluster_id={model_training_cluster.cluster_id}",
        # Read policy from prod; prediction and model from prod/experiment=xxx
        "--ttd_AdGroupPolicyMappingDataset_experimentName=",
        f"--experiment={experiment}",
        f"--promote_to_rollout={promote_to_rollout}",
    ],
    timeout_timedelta=timedelta(hours=1),
    python_distribution="python3.10",
)
calibration_cluster.add_parallel_body_task(calibrate_model_task)

# DAG Dependencies
roas_training_dag >> model_training_cluster
roas_etl_dag_sensor >> model_training_cluster.first_airflow_op()
model_training_cluster >> scoring_cluster >> calibration_cluster

final_dag_check = FinalDagStatusCheckOperator(dag=dag)
calibration_cluster.last_airflow_op() >> final_dag_check
