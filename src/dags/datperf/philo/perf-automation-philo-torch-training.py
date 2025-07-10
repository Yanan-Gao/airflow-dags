# flake8: noqa: F541
from datetime import datetime, timedelta
import copy

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask
from ttd.docker import DockerCommandBuilder, DockerEmrClusterTask, DockerRunEmrTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.ttdenv import TtdEnvFactory
from dags.audauto.utils.test_verification import create_wait_operator, create_test_verification_cluster

airflow_env = TtdEnvFactory.get_from_system()
promote_model_to_rollout = False

# Set to `True` when the current model is a test.
# Ensure that the `promote_model_to_rollout` flag is set to `False` and that the
# `METRIC_VERIFICATION_WAIT_TIME` and `TEST_NAME` config values are specified.
test_verification = False

# Philo version setup, always remember to change for dev and prod
MODEL_VERSION = 6
DEFAULT_REGISTRY = "internal.docker.adsrvr.org"
MODEL_NAME = "philo"
DEFAULT_IMAGE = "philo-torch-training"
DEFAULT_IMAGE_TAG_FLAG = "release"
DEFAULT_MODEL_ENV = "prod"  # dev or prodTest
READ_ENV = "dev" if DEFAULT_MODEL_ENV == "dev" else "prod"  # used for data sync
DEFAULT_DATA_PATH_PREFIX = "features/data"
TRAIN_MODE = "batch"
# if use incremental later, add this back
# INCREMENTAL = "true" if TRAIN_MODE == "incremental" else "false"
# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
# Jar
SCRIPT_RUNNER_JAR = ("s3://thetradedesk-mlplatform-us-east-1/libs/philo/jars/prod/philo.jar")

# script flag values
DEFAULT_BUCKET = "thetradedesk-mlplatform-us-east-1"
DEFAULT_PREFIX_FLAG = "global"
DEFAULT_META_PREFIX_FLAG = "globalmetadata"
DEFAULT_FORMAT = "csv"
DEFAULT_OUTPUT_FLAG = f"s3://{DEFAULT_BUCKET}/models/"
# if use incremental later, add this back
# DEFAULT_LATEST_MODEL_FLAG = f"s3://thetradedesk-mlplatform-us-east-1/models/{DEFAULT_MODEL_ENV}/philo/v={MODEL_VERSION}/{DEFAULT_REGION_FLAG}/"
DEFAULT_MLFLOW_TEST_NAME = ""
# DEFAULT_MLFLOW_TEST_NAME = "cjo-philo-torch-smoke-test-2"

# if not a prod job (determined by dev tag or ttd_env and default_model_env), and if mlflow test name not assigned
if (DEFAULT_IMAGE_TAG_FLAG != "release" or airflow_env != TtdEnvFactory.prod
        or DEFAULT_MODEL_ENV != "prod") and not DEFAULT_MLFLOW_TEST_NAME:
    DEFAULT_MLFLOW_TEST_NAME = DEFAULT_IMAGE_TAG_FLAG

# Rollout configurables
DEFAULT_ROLLOUT_IMAGE_TAG = "3.0.2"

# Rollout flag defaults
DEFAULT_ROLLOUT_STRATEGY = "custom"
DEFAULT_SUCCESS_THRESHOLD = 99.5
DEFAULT_ROLLOUT_FEATURE_FLAG_NAME = "click-model-rollout"
DEFAULT_SAMPLING_KEY = "BidRequestId"
DEFAULT_STAGING_TO_PROD_DELAY_MINUTES = 20.0
DEFAULT_CUSTOM_PERCENTAGES = "1.0,10.0,50.0,100.0"
DEFAULT_CUSTOM_INTERVALS_IN_MINUTES = "40.0,20.0,20.0"

# Test Model Version Verification Defaults
DEFAULT_METRIC_VERIFICATION_WAIT_TIME = 40

# Airflow inputs
ROLLOUT_STRATEGY = f'{{{{ dag_run.conf.get("rollout_strategy") if dag_run.conf is not none and dag_run.conf.get("rollout_strategy") is not none else "{DEFAULT_ROLLOUT_STRATEGY}" }}}}'

SUCCESS_THRESHOLD = f'{{{{ dag_run.conf.get("success_threshold") if dag_run.conf is not none and dag_run.conf.get("success_threshold") is not none else "{DEFAULT_SUCCESS_THRESHOLD}" }}}}'

ROLLOUT_FEATURE_FLAG_NAME = f'{{{{ dag_run.conf.get("rollout_feature_flag_name") if dag_run.conf is not none and dag_run.conf.get("rollout_feature_flag_name") is not none else "{DEFAULT_ROLLOUT_FEATURE_FLAG_NAME}" }}}}'

SAMPLING_KEY = f'{{{{ dag_run.conf.get("sampling_key") if dag_run.conf is not none and dag_run.conf.get("sampling_key") is not none else "{DEFAULT_SAMPLING_KEY}" }}}}'

STAGING_TO_PROD_DELAY_MINUTES = f'{{{{ dag_run.conf.get("staging_to_prod_delay_minutes") if dag_run.conf is not none and dag_run.conf.get("staging_to_prod_delay_minutes") is not none else "{DEFAULT_STAGING_TO_PROD_DELAY_MINUTES}" }}}}'

CUSTOM_PERCENTAGES = f'{{{{ dag_run.conf.get("custom_percentages") if dag_run.conf is not none and dag_run.conf.get("custom_percentages") is not none else "{DEFAULT_CUSTOM_PERCENTAGES}" }}}}'

CUSTOM_INTERVALS_IN_MINUTES = f'{{{{ dag_run.conf.get("custom_intervals_in_minutes") if dag_run.conf is not none and dag_run.conf.get("custom_intervals_in_minutes") is not none else "{DEFAULT_CUSTOM_INTERVALS_IN_MINUTES}" }}}}'

ROLLOUT_IMAGE_TAG = f'{{{{ dag_run.conf.get("rollout_image_tag") if dag_run.conf is not none and dag_run.conf.get("rollout_image_tag") is not none else "{DEFAULT_ROLLOUT_IMAGE_TAG}" }}}}'

MODEL_DATE = f'{{{{ dag_run.conf.get("model_date") if dag_run.conf is not none and dag_run.conf.get("model_date") is not none else (data_interval_start + macros.timedelta(days=1)).strftime("%Y%m%d") }}}}'

IMAGE_TAG = f'{{{{ dag_run.conf.get("image_tag") if dag_run.conf is not none and dag_run.conf.get("image_tag") is not none else "{DEFAULT_IMAGE_TAG_FLAG}" }}}}'

#MODEL_ENV = f'{{{{ dag_run.conf.get("model_env") if dag_run.conf is not none and dag_run.conf.get("model_env") is not none else "{DEFAULT_MODEL_ENV}" }}}}'

FORMAT = f'{{{{ dag_run.conf.get("format") if dag_run.conf is not none and dag_run.conf.get("format") is not none else "{DEFAULT_FORMAT}" }}}}'

OUTPUT_PATH = f'{{{{ dag_run.conf.get("output_path") if dag_run.conf is not none and dag_run.conf.get("output_path") is not none else "{DEFAULT_OUTPUT_FLAG}" }}}}'

MLFLOW_TEST_NAME = f'{{{{ dag_run.conf.get("mlflow_test_name") if dag_run.conf is not none and dag_run.conf.get("mlflow_test_name") is not none else "{DEFAULT_MLFLOW_TEST_NAME}" }}}}'

METRIC_VERIFICATION_WAIT_TIME = f'{{{{ dag_run.conf.get("metric_verification_wait_time") if dag_run.conf is not none and dag_run.conf.get("metric_verification_wait_time") is not none else "{DEFAULT_METRIC_VERIFICATION_WAIT_TIME}" }}}}'


def spark_options_list(num_executors):
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
    ]
    return spark_options_list


application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    },
}]

cluster_tags = {
    "Team": DATPERF.team.jira_team,
}
####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
philo_train = TtdDag(
    dag_id="perf-automation-philo-torch-training",
    start_date=datetime(2025, 1, 5),
    schedule_interval=timedelta(days=1),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/yJLqCg",
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(hours=1),
    default_args={"owner": "DATPERF"},
    slack_alert_only_for_prod=True,
    tags=["DATPERF", "Philo"],
    enable_slack_alert=False,
)

dag = philo_train.airflow_dag

####################################################################################################################
# S3 dataset sensors
####################################################################################################################
philo_etl_data = DateGeneratedDataset(
    bucket=DEFAULT_BUCKET,
    path_prefix=f"{DEFAULT_DATA_PATH_PREFIX}/{MODEL_NAME}/v={MODEL_VERSION}/{READ_ENV}",
    data_name="global",
    date_format="year=%Y/month=%m/day=%d",
    env_aware=False,
    version=None,
)
philo_etl_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id="data_available",
        datasets=[philo_etl_data],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 6,
    )
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[G5.g5_24xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C5.c5_xlarge().with_fleet_weighted_capacity(1),
        M5.m5_xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

cluster = DockerEmrClusterTask(
    name="PhiloTorchModelTrain",
    image_name=DEFAULT_IMAGE,
    image_tag=DEFAULT_IMAGE_TAG_FLAG,
    docker_registry=DEFAULT_REGISTRY,
    entrypoint_in_image="opt/application/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags=cluster_tags,
    emr_release_label="emr-6.9.0",
    environment=airflow_env,
    enable_prometheus_monitoring=True,
    additional_application_configurations=copy.deepcopy(application_configuration),
)
base_python_command = [
    "--train_mode=batch",
    f"--env={DEFAULT_MODEL_ENV}",
    f"--model_creation_date={MODEL_DATE}",
    f"--verbose=info",
    "--privacy_awareness",
]

additional_parameters = [
    "--gpus all",
    "--rm",
    "-e NVIDIA_DISABLE_REQUIRE=1",
    "-e OMP_NUM_THREADS=1",
    "-e MKL_NUM_THREADS=1",
    "--shm-size=10g",
    "--ulimit memlock=-1",
    "-v /mnt/datasets:/var/tmp/input/datasets",
    "-v /mnt/metadata:/var/tmp/input/metadata",
    "-v /mnt/output:/var/tmp/output",
]

base_s3_data_prefix = f"s3://{DEFAULT_BUCKET}/{DEFAULT_DATA_PATH_PREFIX}"

additional_execution_parameters = ["--nproc-per-node=4", "/opt/application/app/model_run.py", f"--nodummy", *base_python_command]
if promote_model_to_rollout:
    additional_execution_parameters.append("--auto_staging")
else:
    additional_execution_parameters.append("--noauto_staging")
if MLFLOW_TEST_NAME and not (DEFAULT_MODEL_ENV == "prod" and airflow_env == TtdEnvFactory.prod):
    additional_execution_parameters.append(f"--mlflow_registry_test_name={MLFLOW_TEST_NAME}")
pytorch_training_docker_command_phase_1 = DockerCommandBuilder(
    docker_registry=DEFAULT_REGISTRY,
    docker_image_name=DEFAULT_IMAGE,
    docker_image_tag=IMAGE_TAG,
    execution_language="torchrun",
    path_to_app="--nnodes=1",
    additional_parameters=[
        *additional_parameters,
        "--ipc=host",
        "-e TF_GPU_THREAD_MODE=gpu_private",
    ],
    additional_execution_parameters=additional_execution_parameters,
)
pytorch_training_task_phase_1 = DockerRunEmrTask(
    name="PyTorchModelTraining",
    docker_run_command=pytorch_training_docker_command_phase_1.build_command(),
    timeout_timedelta=timedelta(hours=72),
)

cluster.add_parallel_body_task(pytorch_training_task_phase_1)

final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies

philo_train >> philo_etl_sensor >> cluster

if DEFAULT_MODEL_ENV == "prod" and airflow_env == TtdEnvFactory.prod and promote_model_to_rollout:
    docker_rollout_image_name = "ttd-base/datperf/dalgo_utils"
    docker_rollout_registry = "production.docker.adsrvr.org"
    core_and_master_fleet_rollout_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    model_rollout_command_line_arguments = [
        f'--rollout_strategy={ROLLOUT_STRATEGY}', f'--model_name={"philo"}', f'--success_threshold={SUCCESS_THRESHOLD}',
        f'--rollout_feature_flag_name={ROLLOUT_FEATURE_FLAG_NAME}', f'--sampling_key={SAMPLING_KEY}', f'--environment={airflow_env}',
        f'--staging_to_prod_delay_minutes={STAGING_TO_PROD_DELAY_MINUTES}', f'--training_cluster_id={cluster.cluster_id}',
        f'--custom_percentages={CUSTOM_PERCENTAGES}', f'--custom_intervals_in_minutes={CUSTOM_INTERVALS_IN_MINUTES}'
    ]
    rollout_cluster_task = DockerEmrClusterTask(
        name="ModelRollout",
        image_name=docker_rollout_image_name,
        image_tag=ROLLOUT_IMAGE_TAG,
        docker_registry=docker_rollout_registry,
        master_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        core_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label="emr-6.9.0",
        environment=airflow_env,
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
        entrypoint_in_image="/opt/application/app/",
        retries=0
    )

    rollout_docker_command = DockerCommandBuilder(
        docker_registry=docker_rollout_registry,
        docker_image_name=docker_rollout_image_name,
        docker_image_tag=ROLLOUT_IMAGE_TAG,
        execution_language="python3",
        path_to_app="/lib/dalgo_utils/janus/kickoff_rollout.py",
        additional_parameters=["--shm-size=5g", "--ulimit memlock=-1"],
        additional_execution_parameters=model_rollout_command_line_arguments,
    )

    rollout_step = DockerRunEmrTask(
        name="ModelRollout", docker_run_command=rollout_docker_command.build_command(), timeout_timedelta=timedelta(hours=3)
    )
    rollout_cluster_task.add_sequential_body_task(rollout_step)
    cluster >> rollout_cluster_task
    rollout_cluster_task.last_airflow_op() >> final_dag_status_step
elif test_verification:
    metric_verification_wait_task = create_wait_operator(METRIC_VERIFICATION_WAIT_TIME, dag)

    test_verification_cluster_task = create_test_verification_cluster(
        docker_dalgo_image_tag=ROLLOUT_IMAGE_TAG,
        cluster_tags=cluster_tags,
        model_name="philo",
        success_threshold=SUCCESS_THRESHOLD,
        training_cluster_id=cluster.cluster_id,
        test_name=MLFLOW_TEST_NAME,
        wait_duration_seconds=METRIC_VERIFICATION_WAIT_TIME
    )

    cluster >> test_verification_cluster_task
    cluster.last_airflow_op() >> metric_verification_wait_task
    metric_verification_wait_task >> test_verification_cluster_task.first_airflow_op()
    test_verification_cluster_task.last_airflow_op() >> final_dag_status_step
else:
    cluster.last_airflow_op() >> final_dag_status_step
