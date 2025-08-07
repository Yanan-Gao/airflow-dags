# flake8: noqa: F541
import copy
from datetime import datetime, timedelta

from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.docker import DockerCommandBuilder, DockerEmrClusterTask, DockerRunEmrTask
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from dags.audauto.utils.test_verification import create_wait_operator, create_test_verification_cluster

# Set to `True` when the current model is a test.
# Ensure that the `promote_model_to_rollout` flag is set to `False` and that the
# `METRIC_VERIFICATION_WAIT_TIME` and `TEST_NAME` config values are specified.
test_verification = False

airflow_env = TtdEnvFactory.get_from_system()

# Philo version setup, always remember to change for dev and prod
MODEL_VERSION = 5
DEFAULT_REGISTRY = "internal.docker.adsrvr.org"
DEFAULT_IMAGE = "philo-tensorflow-training"
DEFAULT_IMAGE_TAG_FLAG = "release"
DEFAULT_MODEL_ENV = "prod"  # dev or prodTest

TRAIN_MODE = "batch"
INCREMENTAL = "true" if TRAIN_MODE == "incremental" else "false"
# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
DATE_MACRO = ('{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}')
DEFAULT_YEAR = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y") }}'
DEFAULT_MONTH = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%m") }}'
DEFAULT_DAY = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%d") }}'
DEFAULT_MODEL_DATE = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y%m%d") }}'  # data will be ready after midnight

# Jar
SCRIPT_RUNNER_JAR = ("s3://thetradedesk-mlplatform-us-east-1/libs/philo/jars/prod/philo.jar")

# scripts
DEFAULT_SCRIPT_LOCATION = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/scripts/"
if DEFAULT_IMAGE_TAG_FLAG != "release":
    DEFAULT_SCRIPT_LOCATION = f"s3://thetradedesk-mlplatform-us-east-1/libs/philo/scripts/mergerequests/{DEFAULT_IMAGE_TAG_FLAG}/"

CLUSTERSETUP = "clustersetup.sh"
MODELRUN = "modelrun.sh"

# Rollout flag defaults
DEFAULT_ROLLOUT_STRATEGY = "custom"
DEFAULT_ROLLOUT_IMAGE_TAG = "3.0.2"
DEFAULT_SUCCESS_THRESHOLD = 99.0
DEFAULT_ROLLOUT_FEATURE_FLAG_NAME = "click-model-rollout"
DEFAULT_SAMPLING_KEY = "BidRequestId"
DEFAULT_STAGING_TO_PROD_DELAY_MINUTES = 20.0
DEFAULT_CUSTOM_PERCENTAGES = "1.0,10.0,50.0,100.0"
DEFAULT_CUSTOM_INTERVALS_IN_MINUTES = "40.0,20.0,20.0"

# Test Model Version Verification Defaults
DEFAULT_METRIC_VERIFICATION_WAIT_TIME = 40
DEFAULT_TEST_NAME = ""

# script flag values
DEFAULT_PREFIX_FLAG = "global"
DEFAULT_META_PREFIX_FLAG = "globalmetadata"
DEFAULT_FORMAT = "csv"
DEFAULT_OUTPUT_FLAG = "s3://thetradedesk-mlplatform-us-east-1/models/"
DEFAULT_REGION_FLAG = "global"
DEFAULT_LATEST_MODEL_FLAG = f"s3://thetradedesk-mlplatform-us-east-1/models/{DEFAULT_MODEL_ENV}/philo/v={MODEL_VERSION}/{DEFAULT_REGION_FLAG}/"
DEFAULT_MLFLOW_TEST_NAME = ""
if DEFAULT_IMAGE_TAG_FLAG != "release" or DEFAULT_MODEL_ENV != "prod":
    DEFAULT_MLFLOW_TEST_NAME = DEFAULT_IMAGE_TAG_FLAG

DEFAULT_DOCKER_GPU_SETUP = ("s3://ttd-build-artefacts/mlops/scripts/docker/v1/install_docker_gpu.sh")
DEFAULT_DOCKER_IDLE_PREVENTION = "s3://ttd-build-artefacts/mlops/scripts/docker/gpu/v1/setup-docker-idle-prevention-cron.sh"

# parse args from manual trigger, if any
SCRIPT_LOCATION = f'{{{{ dag_run.conf.get("script_location") if dag_run.conf is not none and dag_run.conf.get("script_location") is not none else "{DEFAULT_SCRIPT_LOCATION}" }}}}'

START_DATE = f'{{{{ dag_run.conf.get("start_date") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%Y-%m-%d") }}}}'

MODEL_DATE = f'{{{{ dag_run.conf.get("model_date") if dag_run.conf is not none and dag_run.conf.get("model_date") is not none else (data_interval_start + macros.timedelta(days=1)).strftime("%Y%m%d") }}}}'

IMAGE_TAG = f'{{{{ dag_run.conf.get("image_tag") if dag_run.conf is not none and dag_run.conf.get("image_tag") is not none else "{DEFAULT_IMAGE_TAG_FLAG}" }}}}'

MODEL_ENV = f'{{{{ dag_run.conf.get("model_env") if dag_run.conf is not none and dag_run.conf.get("model_env") is not none else "{DEFAULT_MODEL_ENV}" }}}}'

FORMAT = f'{{{{ dag_run.conf.get("format") if dag_run.conf is not none and dag_run.conf.get("format") is not none else "{DEFAULT_FORMAT}" }}}}'

PREFIX = f'{{{{ dag_run.conf.get("prefix") if dag_run.conf is not none and dag_run.conf.get("prefix") is not none else "{DEFAULT_PREFIX_FLAG}" }}}}'

META_PREFIX = f'{{{{ dag_run.conf.get("meta_prefix") if dag_run.conf is not none and dag_run.conf.get("meta_prefix") is not none else "{DEFAULT_META_PREFIX_FLAG}" }}}}'

OUTPUT_PATH = f'{{{{ dag_run.conf.get("output_path") if dag_run.conf is not none and dag_run.conf.get("output_path") is not none else "{DEFAULT_OUTPUT_FLAG}" }}}}'

REGION = f'{{{{ dag_run.conf.get("region") if dag_run.conf is not none and dag_run.conf.get("region") is not none else "{DEFAULT_REGION_FLAG}" }}}}'

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

# if we get a date, we need y/m/d to properly check for upstream datasets (philo etl in this case)
YEAR = f'{{{{ macros.datetime.strptime(dag_run.conf.get("start_date"), "%Y-%m-%d").strftime("%Y") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%Y") }}}}'

MONTH = f'{{{{ macros.datetime.strptime(dag_run.conf.get("start_date"), "%Y-%m-%d").strftime("%m") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%m") }}}}'

DAY = f'{{{{ macros.datetime.strptime(dag_run.conf.get("start_date"), "%Y-%m-%d").strftime("%d") if dag_run.conf is not none and dag_run.conf.get("start_date") is not none else (data_interval_start + macros.timedelta(days=0)).strftime("%d") }}}}'

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    },
}]

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
philo_train = TtdDag(
    dag_id="perf-automation-philo-training-global",
    start_date=datetime(2024, 8, 7),
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
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"features/data/philo/v={MODEL_VERSION}",
    data_name="global",
    date_format="year=%Y/month=%m/day=%d",
    env_aware=True,
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
core_and_master_fleet_rollout_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)
bootstrap_script_actions = [
    ScriptBootstrapAction(SCRIPT_LOCATION + CLUSTERSETUP),
    ScriptBootstrapAction(DEFAULT_DOCKER_GPU_SETUP),
    ScriptBootstrapAction(DEFAULT_DOCKER_IDLE_PREVENTION),
]
cluster = EmrClusterTask(
    name="philoModelTrain",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label="emr-6.9.0",
    bootstrap_script_actions=bootstrap_script_actions,
    cluster_auto_termination_idle_timeout_seconds=-1,
    additional_application_configurations=copy.deepcopy(application_configuration),
)

model_train_command_line_arguments = [
    "-t",
    IMAGE_TAG,
    "-e",
    MODEL_ENV,
    "-o",
    OUTPUT_PATH,
    "-r",
    REGION,
    "-m",
    TRAIN_MODE,
    "-c",
    MODEL_DATE,
]
# Optional args
airflow_env = TtdEnvFactory.get_from_system()
if airflow_env == TtdEnvFactory.prodTest:
    MLFLOW_TEST_NAME_CLI = (
        f"{{{{ "
        f'"-n" + dag_run.conf.get("mlflow_test_name")'
        f' if dag_run.conf is not none and dag_run.conf.get("mlflow_test_name") is not none'
        f' else "-n {DEFAULT_MLFLOW_TEST_NAME}" }}}}'
    )
    model_train_command_line_arguments.append(MLFLOW_TEST_NAME_CLI)

model_train = EmrJobTask(
    name="ModelTraining",
    class_name=None,
    executable_path=SCRIPT_LOCATION + MODELRUN,
    command_line_arguments=model_train_command_line_arguments,
    timeout_timedelta=timedelta(hours=24),
    job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
)

cluster.add_parallel_body_task(model_train)

final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies

philo_train >> philo_etl_sensor >> cluster

if airflow_env == TtdEnvFactory.prod:
    docker_rollout_image_name = "ttd-base/datperf/dalgo_utils"
    docker_rollout_registry = "production.docker.adsrvr.org"
    core_and_master_fleet_rollout_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    model_rollout_command_line_arguments = [
        f'--rollout_strategy={ROLLOUT_STRATEGY}', f'--success_threshold={SUCCESS_THRESHOLD}',
        f'--rollout_feature_flag_name={ROLLOUT_FEATURE_FLAG_NAME}', f'--sampling_key={SAMPLING_KEY}', f'--environment={MODEL_ENV}',
        f'--staging_to_prod_delay_minutes={STAGING_TO_PROD_DELAY_MINUTES}', f'--training_cluster_id={cluster.cluster_id}',
        f'--custom_percentages={CUSTOM_PERCENTAGES}', f'--custom_intervals_in_minutes={CUSTOM_INTERVALS_IN_MINUTES}'
    ]
    rollout_cluster_task = DockerEmrClusterTask(
        name="ModelRollout",
        image_name=docker_rollout_image_name,
        image_tag=ROLLOUT_IMAGE_TAG,
        docker_registry=docker_rollout_registry,
        master_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        cluster_tags={'Team': DATPERF.team.jira_team},
        core_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        emr_release_label="emr-6.9.0",
        environment=TtdEnvFactory.get_from_system(),
        additional_application_configurations=application_configuration,
        enable_prometheus_monitoring=True,
        entrypoint_in_image="/opt/application/app/",
        retries=0
    )

    docker_command = DockerCommandBuilder(
        docker_registry=docker_rollout_registry,
        docker_image_name=docker_rollout_image_name,
        docker_image_tag=ROLLOUT_IMAGE_TAG,
        execution_language="python3.10",
        path_to_app="/opt/application/app/rollout_philo.py",
        additional_parameters=["--shm-size=5g", "--ulimit memlock=-1"],
        additional_execution_parameters=model_rollout_command_line_arguments,
    )

    rollout_step = DockerRunEmrTask(
        name="ModelRollout", docker_run_command=docker_command.build_command(), timeout_timedelta=timedelta(hours=3)
    )
    rollout_cluster_task.add_sequential_body_task(rollout_step)
    cluster >> rollout_cluster_task
    rollout_cluster_task.last_airflow_op() >> final_dag_status_step
elif test_verification:
    metric_verification_wait_task = create_wait_operator(METRIC_VERIFICATION_WAIT_TIME, dag)

    test_verification_cluster_task = create_test_verification_cluster(
        docker_dalgo_image_tag=ROLLOUT_IMAGE_TAG,
        cluster_tags={'Team': DATPERF.team.jira_team},
        model_name="philo",
        success_threshold=SUCCESS_THRESHOLD,
        training_cluster_id=cluster.cluster_id,
        test_name=TEST_NAME,
        wait_duration_seconds=METRIC_VERIFICATION_WAIT_TIME
    )

    cluster >> test_verification_cluster_task
    cluster.last_airflow_op() >> metric_verification_wait_task
    metric_verification_wait_task >> test_verification_cluster_task.first_airflow_op()
    test_verification_cluster_task.last_airflow_op() >> final_dag_status_step
else:
    cluster.last_airflow_op() >> final_dag_status_step
