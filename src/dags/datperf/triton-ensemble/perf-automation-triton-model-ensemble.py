from ttd.docker import DockerEmrClusterTask, DockerRunEmrTask, DockerCommandBuilder
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
# from ttd.sensors.s3_key_sensor import TtdS3KeySensor
from ttd.tasks.op import OpTask
from airflow.utils.dates import days_ago
from airflow.sensors.s3_key_sensor import S3KeySensor

from datetime import timedelta, datetime

# DOCKER_REGISTRY = "docker.pkgs.adsrvr.org/apps-dev" # <- cloudsmith
DOCKER_REGISTRY = "dev.docker.adsrvr.org"  # <- nexus
DOCKER_IMAGE_NAME = "ttd/triton-kpi/ensemble-model-config"
DOCKER_IMAGE_TAG = "latest"
ENTRYPOINT_PATH = "/lib/app/main.py"

# Sample path: 's3://thetradedesk-mlplatform-us-east-1/models/prod/philo_torch/v=6/global/onnxstacked_sensitive_adgroup/model_files/datetime'
REPO_BUCKET = "thetradedesk-mlplatform-us-east-1"
REPO_PATH_PREFIX = "models/prod"
MODEL_NAME = "philo_torch"
MODEL_VERSION = "6"
READ_ENV = "prod"
ADGROUP_POSTFIX = "global/onnxstacked_sensitive_adgroup/model_files"
BIDREQUEST_POSTFIX = "global/onnxstacked_sensitive_bidrequest/model_files"

dag_pipeline: TtdDag = TtdDag(
    dag_id="perf-automation-philo-update-triton-model-repo",
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",
    max_active_runs=1,
    enable_slack_alert=False,
    retries=1,
    retry_delay=timedelta(minutes=10),
)

dag = dag_pipeline.airflow_dag

timeformat = f"{datetime.now().strftime('%Y%m%d')}????"
adgroup_bucket_key = f"{REPO_PATH_PREFIX}/{MODEL_NAME}/v={MODEL_VERSION}/{ADGROUP_POSTFIX}/{timeformat}/_SUCCESS"

philo_model_adgroup_sensor = OpTask(
    op=S3KeySensor(
        task_id="model-update-wait-for-new-adgroup",
        poke_interval=60 * 60 * 3,
        timeout=60 * 60 * 24,
        bucket_name=REPO_BUCKET,
        bucket_key=adgroup_bucket_key,
        wildcard_match=True,
        mode="reschedule"
    )
)

bidrequest_bucket_key = f"{REPO_PATH_PREFIX}/{MODEL_NAME}/v={MODEL_VERSION}/{BIDREQUEST_POSTFIX}/{timeformat}/_SUCCESS"
philo_model_bidrequest_sensor = OpTask(
    op=S3KeySensor(
        task_id="model-update-wait-for-new-bidrequest",
        poke_interval=60 * 60 * 3,
        timeout=60 * 60 * 24,
        bucket_name=REPO_BUCKET,
        bucket_key=bidrequest_bucket_key,
        wildcard_match=True,
        mode="reschedule"
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

model_update_docker_command = DockerCommandBuilder(
    docker_image_name=DOCKER_IMAGE_NAME,
    docker_image_tag=DOCKER_IMAGE_TAG,
    docker_registry=DOCKER_REGISTRY,
    path_to_app=ENTRYPOINT_PATH,
    additional_execution_parameters=["--model_folder_path=./models"],
)

model_update_step = DockerRunEmrTask(
    name="model-update-run-task",
    docker_run_command=model_update_docker_command.build_command(),
)

model_update_cluster = DockerEmrClusterTask(
    name="model-update-cluster-task",
    image_name=DOCKER_IMAGE_NAME,
    image_tag=DOCKER_IMAGE_TAG,
    docker_registry=DOCKER_REGISTRY,
    entrypoint_in_image="/lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": "DATPERF"},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label="emr-6.7.0",
)

model_update_cluster.add_parallel_body_task(model_update_step)

dag_pipeline >> philo_model_adgroup_sensor >> philo_model_bidrequest_sensor >> model_update_cluster
