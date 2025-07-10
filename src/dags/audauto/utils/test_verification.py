import time
import copy
from datetime import timedelta
from ttd.ttdenv import TtdEnvFactory
from dags.audauto.utils import utils
from airflow.operators.python_operator import PythonOperator
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask

environment = TtdEnvFactory.get_from_system()

docker_dalgo_image_name = "ttd-base/datperf/dalgo_utils"
docker_dalgo_registry = "production.docker.adsrvr.org"
core_and_master_fleet_rollout_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)


def process_metric_verification_wait(metric_verification_wait_in_seconds, **kwargs):
    try:
        METRIC_VERIFICATION_WAIT_SECONDS = int(metric_verification_wait_in_seconds)
    except ValueError:
        raise ValueError(f"Invalid metric_verification_wait value: {metric_verification_wait_in_seconds}. Must be an integer.")
    time.sleep(METRIC_VERIFICATION_WAIT_SECONDS)


def create_wait_operator(metric_verification_wait_time, dag):
    metric_verification_wait_task = PythonOperator(
        task_id='process_metric_verification',
        python_callable=process_metric_verification_wait,
        provide_context=True,
        op_kwargs={'metric_verification_wait_in_seconds': metric_verification_wait_time},
        dag=dag
    )

    return metric_verification_wait_task


def create_test_verification_cluster(
    docker_dalgo_image_tag, cluster_tags, model_name, success_threshold, training_cluster_id, test_name, wait_duration_seconds
):
    test_verification_command_line_arguments = [
        f'--model_name={model_name}', f'--success_threshold={success_threshold}', f'--training_cluster_id={training_cluster_id}',
        f'--test_name={test_name}'
        f'--wait_duration={wait_duration_seconds}'
    ]

    test_verification_cluster_task = DockerEmrClusterTask(
        name="TestVerification",
        image_name=docker_dalgo_image_name,
        image_tag=docker_dalgo_image_tag,
        docker_registry=docker_dalgo_registry,
        master_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        core_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label="emr-6.9.0",
        environment=environment,
        additional_application_configurations=copy.deepcopy(utils.get_application_configuration()),
        enable_prometheus_monitoring=True,
        entrypoint_in_image="/opt/application/app/",
        retries=0
    )

    test_verification_docker_command = DockerCommandBuilder(
        docker_registry=docker_dalgo_registry,
        docker_image_name=docker_dalgo_image_name,
        docker_image_tag=docker_dalgo_image_tag,
        execution_language="python3",
        path_to_app="/lib/dalgo_utils/janus/testing_model_verification.py",
        additional_parameters=["--shm-size=5g", "--ulimit memlock=-1"],
        additional_execution_parameters=test_verification_command_line_arguments,
    )

    test_verification_step = DockerRunEmrTask(
        name="TestVerification", docker_run_command=test_verification_docker_command.build_command(), timeout_timedelta=timedelta(hours=3)
    )
    test_verification_cluster_task.add_sequential_body_task(test_verification_step)

    return test_verification_cluster_task
