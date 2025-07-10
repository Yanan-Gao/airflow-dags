from typing import List

import copy
from datetime import datetime, timedelta

from airflow.sensors.external_task_sensor import ExternalTaskSensor

from dags.audauto.utils.test_verification import create_wait_operator, create_test_verification_cluster
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
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.ttdenv import TtdEnvFactory
from dags.audauto.utils import utils

experiment_name = ""
promote_to_rollout = True

# Set to `True` when the current model is a test.
# Ensure that the `promote_model_to_rollout` flag is set to `False` and that the
# `METRIC_VERIFICATION_WAIT_TIME` and `TEST_NAME` config values are specified.
test_verification = False

env_specific_argument_lantern: List[str] = [] if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else [
    "train_set.mlflow_registry_test_name=roas_prodtest_make_mlflow_happy",
]

env_specific_argument_kongming_python: List[str] = []

DEFAULT_ROLLOUT_REGISTRY = "internal.docker.adsrvr.org"  # temporary fix will switch to back to prod which is
# "production.docker.adsrvr.org" once it's merged
DEFAULT_ROLLOUT_IMAGE_TAG = "3274591"  # "3.0.2"

# Rollout flag defaults
DEFAULT_ROLLOUT_STRATEGY = "custom"
DEFAULT_SUCCESS_THRESHOLD = 99.5
DEFAULT_ROLLOUT_FEATURE_FLAG_NAME = "revenue-model-rollout"
DEFAULT_SAMPLING_KEY = "conversionaggregateid"
DEFAULT_STAGING_TO_PROD_DELAY_MINUTES = 20.0
DEFAULT_CUSTOM_PERCENTAGES = "1.0,10.0,50.0,100.0"
DEFAULT_CUSTOM_INTERVALS_IN_MINUTES = "40.0,20.0,20.0"

# Test Model Version Verification Defaults
DEFAULT_METRIC_VERIFICATION_WAIT_TIME = 40
DEFAULT_TEST_NAME = ""

docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
pytorch_docker_image_name = "ttd-base/audauto/kongming_lantern"
pytorch_docker_image_tag = "release"
docker_image_name = "ttd-base/audauto/kongming"
docker_image_tag = "release"

# Training Configs
# needed for isolated model
train_set = "full_roas_isolate"
instance_type = "emr_roas"
model_date = "{{ ds_nodash }}"
extended_features: List[str] = []
extended_features_flag = ",".join(extended_features)
task = "roas"
trainset_version = 2

# Scoring Configs
num_days_to_score = 7
scoring_batch_size = 65536
virtual_cores = 72

calibration_weighted_capacity = 100

#######################################################
# End of setting area
#######################################################

#######################################################
# Code Area
#######################################################

# Airflow inputs
ROLLOUT_STRATEGY = f'{{{{ dag_run.conf.get("rollout_strategy") if dag_run.conf is not none and dag_run.conf.get("rollout_strategy") is not none else "{DEFAULT_ROLLOUT_STRATEGY}" }}}}'

SUCCESS_THRESHOLD = f'{{{{ dag_run.conf.get("success_threshold") if dag_run.conf is not none and dag_run.conf.get("success_threshold") is not none else "{DEFAULT_SUCCESS_THRESHOLD}" }}}}'

ROLLOUT_FEATURE_FLAG_NAME = f'{{{{ dag_run.conf.get("rollout_feature_flag_name") if dag_run.conf is not none and dag_run.conf.get("rollout_feature_flag_name") is not none else "{DEFAULT_ROLLOUT_FEATURE_FLAG_NAME}" }}}}'

SAMPLING_KEY = f'{{{{ dag_run.conf.get("sampling_key") if dag_run.conf is not none and dag_run.conf.get("sampling_key") is not none else "{DEFAULT_SAMPLING_KEY}" }}}}'

STAGING_TO_PROD_DELAY_MINUTES = f'{{{{ dag_run.conf.get("staging_to_prod_delay_minutes") if dag_run.conf is not none and dag_run.conf.get("staging_to_prod_delay_minutes") is not none else "{DEFAULT_STAGING_TO_PROD_DELAY_MINUTES}" }}}}'

CUSTOM_PERCENTAGES = f'{{{{ dag_run.conf.get("custom_percentages") if dag_run.conf is not none and dag_run.conf.get("custom_percentages") is not none else "{DEFAULT_CUSTOM_PERCENTAGES}" }}}}'

CUSTOM_INTERVALS_IN_MINUTES = f'{{{{ dag_run.conf.get("custom_intervals_in_minutes") if dag_run.conf is not none and dag_run.conf.get("custom_intervals_in_minutes") is not none else "{DEFAULT_CUSTOM_INTERVALS_IN_MINUTES}" }}}}'

ROLLOUT_IMAGE_TAG = f'{{{{ dag_run.conf.get("rollout_image_tag") if dag_run.conf is not none and dag_run.conf.get("rollout_image_tag") is not none else "{DEFAULT_ROLLOUT_IMAGE_TAG}" }}}}'

METRIC_VERIFICATION_WAIT_TIME = f'{{{{ dag_run.conf.get("metric_verification_wait_time") if dag_run.conf is not none and dag_run.conf.get("metric_verification_wait_time") is not none else "{DEFAULT_METRIC_VERIFICATION_WAIT_TIME}" }}}}'

TEST_NAME = f'{{{{ dag_run.conf.get("test_name") if dag_run.conf is not none and dag_run.conf.get("test_name") is not none else "{DEFAULT_TEST_NAME}" }}}}'

# Route errors to test channel in test environment
slack_channel, slack_tags, enable_slack_alert = utils.get_env_vars()
environment = TtdEnvFactory.get_from_system()


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
        cluster_tags=utils.cluster_tags,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        environment=environment,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(utils.get_application_configuration()),
    )


# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
roas_training_dag = TtdDag(
    dag_id="perf-automation-roas-training",
    start_date=datetime(2025, 6, 9),
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

# Model Training Step
model_training_cluster = utils.create_single_master_instance_docker_cluster(
    cluster_name="ROASModelTraining",
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
        f"train_set={train_set}", f"instance={instance_type}", f"instance.env={environment}", f"instance.model_creation_date={model_date}",
        "instance.push_summary_stats=False"
    ] + env_specific_argument_lantern,
)
model_training_task = DockerRunEmrTask(
    name="ModelTraining",
    docker_run_command=training_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=72),
)
model_training_cluster.add_parallel_body_task(model_training_task)


def generate_scoring_subtask_for_date(scoring_date: str, index: int):
    scoring_cluster = create_single_master_instance_docker_cluster(
        cluster_name=f'ROASScoring_{index}',
        docker_registry=docker_registry,
        docker_image_name=docker_image_name,
        docker_image_tag=docker_image_tag,
        instance_type=R5.r5_24xlarge(),
        worker_instance_type=R5.r5_4xlarge(),
    )

    scoring_docker_command = DockerCommandBuilder(
        docker_registry=docker_registry,
        docker_image_name=docker_image_name,
        docker_image_tag=docker_image_tag,
        execution_language="python3.10",
        path_to_app='/opt/application/app/main.py',
        additional_parameters=[
            "-v /mnt/tmp:/opt/application/tmp",
        ],
        additional_execution_parameters=[
            f"--env={environment}", "--run_score=true", f"--model_creation_date={model_date}", "--model_path=./input/model/",
            f"--extended_features={extended_features_flag}", f"--score_dates={scoring_date}", f"--task={task}",
            f"--scoring_batch_size={scoring_batch_size}", f"--virtual_cores={virtual_cores}", "--neofy_roas_model=0"
        ] + env_specific_argument_kongming_python
    )
    scoring_task = DockerRunEmrTask(
        name="Scoring", docker_run_command=scoring_docker_command.build_command(), timeout_timedelta=timedelta(hours=1)
    )
    scoring_cluster.add_parallel_body_task(scoring_task)

    return scoring_cluster


def generate_scoring_subdag():
    scoring_sub_clusters = []
    for day in range(num_days_to_score):
        decrement_day = -1 * (num_days_to_score - 1) + day
        # ds is template variable that's declared as template variable down the calling stack.
        # however the template and regular parameter don't play well, they are prased at the same time.
        # so I use catnation to forst parsing of the regular parameter first.
        scoring_date = "{{ macros.ds_format(macros.ds_add(ds, " + str(decrement_day) + "), '%Y-%m-%d', '%Y%m%d')}}"
        scoring_subtask_for_date = generate_scoring_subtask_for_date(scoring_date=scoring_date, index=day)
        scoring_sub_clusters.append(scoring_subtask_for_date)
    return scoring_sub_clusters


# todo: check if enough data exists
scoring_sub_clusters = generate_scoring_subdag()

# Score Stats Calculation and Tensorflow Calibration Step
tf_calibration_cluster = utils.create_calculation_docker_cluster(
    cluster_name="ROASModelCalibration",
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    master_instance_type=G4.g4dn_8xlarge(),
    weighted_capacity=calibration_weighted_capacity,
    entrypoint_in_image="opt/application/app/"
)
tf_calibrate_model_task = PySparkEmrTask(
    name="ModelCalibration",
    entry_point_path="/home/hadoop/app/calibration_roas.py",
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    additional_args_option_pairs_list=utils.get_spark_options_list(int(round(3 * calibration_weighted_capacity)) * 30),
    command_line_arguments=[
        f"--env={environment}", f"--date={model_date}", f"--model_creation_date={model_date}", f"--task={task}",
        f"--training_cluster_id={model_training_cluster.cluster_id}", f"--promote_to_rollout={promote_to_rollout}", "--neofy_roas_model=1"
    ] + env_specific_argument_kongming_python,
    timeout_timedelta=timedelta(hours=1),
    python_distribution="python3.10",
)
tf_calibration_cluster.add_parallel_body_task(tf_calibrate_model_task)

# Neofy Calibration Step
neofy_calibration_cluster = utils.create_single_master_instance_docker_cluster(
    cluster_name="ROASModelNeofyCalibration",
    docker_registry=docker_registry,
    docker_image_name=pytorch_docker_image_name,
    docker_image_tag=pytorch_docker_image_tag,
    instance_type=G5.g5_16xlarge(),
)
neofy_calibration_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=pytorch_docker_image_name,
    execution_language="python3.10",
    docker_image_tag=pytorch_docker_image_tag,
    path_to_app="/opt/application/app/calibration_roas.py",
    additional_parameters=[
        "--gpus all",
        "--ipc=host",
        "-e PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True",
        "-e TF_GPU_THREAD_MODE=gpu_private",
        "-e NVIDIA_DISABLE_REQUIRE=1",
    ],
    additional_execution_parameters=[
        f"train_set={train_set}", f"instance={instance_type}", f"instance.env={environment}", f"instance.model_creation_date={model_date}",
        "instance.push_summary_stats=False", "train_set.translate_model_to_tensorflow=True"
    ] + env_specific_argument_lantern,
)
model_neofy_calibration_task = DockerRunEmrTask(
    name="ModelNeofyCalibration",
    docker_run_command=neofy_calibration_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=72),
)
neofy_calibration_cluster.add_parallel_body_task(model_neofy_calibration_task)

# Model Warmup and Register Step
register_cluster = utils.create_single_master_instance_docker_cluster(
    cluster_name='ROASRegister',
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    instance_type=C5.c5_xlarge(),
)

register_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    execution_language="python3.10",
    path_to_app='/opt/application/app/model_rollout_roas.py',
    additional_execution_parameters=[
        f"--env={environment}", f"--model_creation_date={model_date}", f"--date={model_date}", "--model_path=./input/model/",
        f"--task={task}", f"--training_cluster_id={model_training_cluster.cluster_id}", f"--promote_to_rollout={promote_to_rollout}",
        "--neofy_roas_model=1"
    ] + env_specific_argument_kongming_python
)
register_task = DockerRunEmrTask(
    name="Register", docker_run_command=register_docker_command.build_command(), timeout_timedelta=timedelta(hours=1)
)
register_cluster.add_parallel_body_task(register_task)

# DAG Dependencies
roas_training_dag >> model_training_cluster
roas_etl_dag_sensor >> model_training_cluster.first_airflow_op()
for scoring_cluster in scoring_sub_clusters:
    model_training_cluster >> scoring_cluster >> tf_calibration_cluster >> neofy_calibration_cluster >> register_cluster

final_dag_check = FinalDagStatusCheckOperator(dag=dag)

# TODO add experiment check once implemented
if environment == TtdEnvFactory.prod and promote_to_rollout:
    docker_rollout_image_name = "ttd-base/datperf/dalgo_utils"
    docker_rollout_registry = DEFAULT_ROLLOUT_REGISTRY
    core_and_master_fleet_rollout_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    model_rollout_command_line_arguments = [
        f'--rollout_strategy={ROLLOUT_STRATEGY}', f'--model_name={"roas"}', f'--success_threshold={SUCCESS_THRESHOLD}',
        f'--rollout_feature_flag_name={ROLLOUT_FEATURE_FLAG_NAME}', f'--sampling_key={SAMPLING_KEY}', f'--environment={environment}',
        f'--staging_to_prod_delay_minutes={STAGING_TO_PROD_DELAY_MINUTES}', f'--training_cluster_id={model_training_cluster.cluster_id}',
        f'--custom_percentages={CUSTOM_PERCENTAGES}', f'--custom_intervals_in_minutes={CUSTOM_INTERVALS_IN_MINUTES}'
    ]
    rollout_cluster_task = DockerEmrClusterTask(
        name="ModelRollout",
        image_name=docker_rollout_image_name,
        image_tag=ROLLOUT_IMAGE_TAG,
        docker_registry=docker_rollout_registry,
        master_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        core_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        cluster_tags=utils.cluster_tags,
        emr_release_label="emr-6.9.0",
        environment=environment,
        additional_application_configurations=copy.deepcopy(utils.get_application_configuration()),
        enable_prometheus_monitoring=True,
        entrypoint_in_image="/opt/application/app/",
        retries=0
    )

    docker_command = DockerCommandBuilder(
        docker_registry=docker_rollout_registry,
        docker_image_name=docker_rollout_image_name,
        docker_image_tag=ROLLOUT_IMAGE_TAG,
        execution_language="python3",
        path_to_app="/lib/dalgo_utils/janus/kickoff_rollout.py",
        additional_parameters=["--shm-size=5g", "--ulimit memlock=-1"],
        additional_execution_parameters=model_rollout_command_line_arguments,
    )

    rollout_step = DockerRunEmrTask(
        name="ModelRollout", docker_run_command=docker_command.build_command(), timeout_timedelta=timedelta(hours=3)
    )
    rollout_cluster_task.add_sequential_body_task(rollout_step)
    register_cluster >> rollout_cluster_task
    rollout_cluster_task.last_airflow_op() >> final_dag_check
elif test_verification:
    metric_verification_wait_task = create_wait_operator(METRIC_VERIFICATION_WAIT_TIME, dag)

    test_verification_cluster_task = create_test_verification_cluster(
        docker_dalgo_image_tag=ROLLOUT_IMAGE_TAG,
        cluster_tags=utils.cluster_tags,
        model_name="roas",
        success_threshold=SUCCESS_THRESHOLD,
        training_cluster_id=model_training_cluster.cluster_id,
        test_name=TEST_NAME,
        wait_duration_seconds=METRIC_VERIFICATION_WAIT_TIME
    )

    register_cluster >> test_verification_cluster_task
    register_cluster.last_airflow_op() >> metric_verification_wait_task
    metric_verification_wait_task >> test_verification_cluster_task.first_airflow_op()
    test_verification_cluster_task.last_airflow_op() >> final_dag_check
else:
    register_cluster.last_airflow_op() >> final_dag_check
