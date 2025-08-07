from datetime import timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, DockerRunEmrTask, DockerCommandBuilder
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from airflow.utils.dates import days_ago
from ttd.ttdenv import TtdEnvFactory

retry_delay: timedelta = timedelta(minutes=15)

dag_name = "mlflow-integration-test"
model_name = "ttd-mlflow-test-airflow-integration-test"
env = TtdEnvFactory.get_from_system()

# Docker information
docker_registry = f"{'production' if env.execution_env == 'prod' else 'internal'}.docker.adsrvr.org"
docker_image_name = "ttd-base/aifun/ttd-mlflow-airflow-integration-test"
docker_image_tag = "latest"

docker_command = DockerCommandBuilder(
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    docker_registry=docker_registry,
    path_to_app="/lib/app/main.py",
)

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=days_ago(1),
    schedule_interval=None,
    max_active_runs=1,
    enable_slack_alert=False,
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

cluster_imdsv1 = DockerEmrClusterTask(
    name=f'{dag_name}-imdsv1',
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": "AIFUN"},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
)

cluster_imdsv2 = DockerEmrClusterTask(
    name=f'{dag_name}-imdsv2',
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": "AIFUN"},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
)
####################################################################################################################
# steps
####################################################################################################################

job_step_v1 = DockerRunEmrTask(name="mlflow-integration-test-v1", docker_run_command=docker_command.build_command())

job_step_v2 = DockerRunEmrTask(name="mlflow-integration-test-v2", docker_run_command=docker_command.build_command())

cluster_imdsv1.add_parallel_body_task(job_step_v1)
cluster_imdsv2.add_parallel_body_task(job_step_v2)
dag >> cluster_imdsv1
cluster_imdsv1 >> cluster_imdsv2

adag = dag.airflow_dag
