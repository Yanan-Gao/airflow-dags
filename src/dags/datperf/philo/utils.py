import copy
from datetime import timedelta
from typing import Optional, List

from ttd.docker import DockerCommandBuilder, DockerRunEmrTask, DockerEmrClusterTask
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DATPERF
from ttd.ttdenv import TtdEnvFactory

APPLICATION_CONFIGURATION = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    },
}]

CLUSTER_TAGS = {
    "Team": DATPERF.team.jira_team,
}


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


def get_training_python_command(
    airflow_env,
    model_env,
    model_date,
    mlflow_test_name,
    sensitive=True,
    sensitive_features: Optional[List[str]] = None,
    concat_model=False,
    experiment_name="",
    mlflow_auto_staging=False,
    train_mode="batch"
):
    base_python_command = [
        "--nproc-per-node=4",
        "/opt/application/app/model_run.py",
        "--nodummy",
        f"--train_mode={train_mode}",
        f"--env={model_env}",
        f"--model_creation_date={model_date}",
        "--verbose=info",
        "--privacy_awareness",
    ]
    if mlflow_test_name and not (model_env == "prod" and airflow_env == TtdEnvFactory.prod):
        base_python_command.append(f"--mlflow_registry_test_name={mlflow_test_name}")
    if sensitive:
        base_python_command.append("--sensitive")
    if sensitive_features:
        base_python_command.append(f"--sensitive_features={','.join(sensitive_features)}")
    if concat_model:
        base_python_command.append("--concat_model")
    if experiment_name:
        base_python_command.append(f"--experiment_name={experiment_name}")
    if mlflow_auto_staging:
        base_python_command.append("--auto_staging")
    return base_python_command


def get_docker_run_emr_task(
    docker_registry, image_name, image_tag, name, model_env, model_date, mlflow_test_name, airflow_env, experiment_name, sensitive,
    sensitive_features, concat_model, mlflow_auto_staging, train_mode
):
    pytorch_training_docker_command = DockerCommandBuilder(
        docker_registry=docker_registry,
        docker_image_name=image_name,
        docker_image_tag=image_tag,
        execution_language="torchrun",
        path_to_app="--nnodes=1",
        additional_parameters=[
            "--gpus all",
            "--rm",
            "-e NVIDIA_DISABLE_REQUIRE=1",
            "-e OMP_NUM_THREADS=1",
            "-e MKL_NUM_THREADS=1",
            "-e TF_GPU_THREAD_MODE=gpu_private",
            "--shm-size=10g",
            "--ulimit memlock=-1",
            "--ipc=host",
            "-v /mnt/datasets:/var/tmp/input/datasets",
            "-v /mnt/metadata:/var/tmp/input/metadata",
            "-v /mnt/output:/var/tmp/output",
        ],
        additional_execution_parameters=get_training_python_command(
            airflow_env=airflow_env,
            model_env=model_env,
            model_date=model_date,
            mlflow_test_name=mlflow_test_name,
            sensitive=sensitive,
            sensitive_features=sensitive_features,
            concat_model=concat_model,
            experiment_name=experiment_name,
            mlflow_auto_staging=mlflow_auto_staging,
            train_mode=train_mode
        ),
    )
    pytorch_training_task = DockerRunEmrTask(
        name=name,
        docker_run_command=pytorch_training_docker_command.build_command(),
        timeout_timedelta=timedelta(hours=72),
    )
    return pytorch_training_task


def get_emr_cluster(image_name, image_tag, docker_registry, cluster_tags, airflow_env):
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
        image_name=image_name,
        image_tag=image_tag,
        docker_registry=docker_registry,
        entrypoint_in_image="opt/application/app/",
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label="emr-6.9.0",
        environment=airflow_env,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(APPLICATION_CONFIGURATION),
    )
    return cluster
