from datetime import timedelta

from airflow.operators.python import PythonVirtualenvOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, DockerRunEmrTask, DockerCommandBuilder
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.s3_python_databricks_task import S3PythonDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask
from airflow.utils.dates import days_ago
from ttd.ttdenv import TtdEnvFactory

retry_delay: timedelta = timedelta(minutes=15)

dag_name = "sample-ttd-mlflow-training"
model_training_task_name = "model-training"
model_promotion_task_name = "model-promotion"
model_name = "jada-test-ttd-mlflow-within-emr"
env = TtdEnvFactory.get_from_system()

# Docker information
training_docker_registry = f"{'production' if env.execution_env == 'prod' else 'internal'}.docker.adsrvr.org"
training_docker_image_name = ("ttd-base/aifun/ttd-mlflow-example-model-training")
training_docker_image_tag = "latest"

docker_command = DockerCommandBuilder(
    docker_image_name=training_docker_image_name,
    docker_image_tag=training_docker_image_tag,
    docker_registry=training_docker_registry,
    path_to_app="/lib/app/model_training.py",
    additional_execution_parameters=[f"--env {env.execution_env}", f"--name {model_name}"],
)

model_training_on_databricks = DatabricksWorkflow(
    job_name="databricks-test-model-training-v2",
    cluster_name="databricks-test-model-training-v2",
    cluster_tags={
        "Team": "AIFUN",
        "Project": "databricks-test-model-training",
    },
    ebs_config=DatabricksEbsConfiguration(
        ebs_volume_count=1,
        ebs_volume_size_gb=64,
    ),
    worker_node_type="r5.xlarge",
    driver_node_type="r5.xlarge",
    worker_node_count=1,
    use_photon=False,
    tasks=[
        S3PythonDatabricksTask(
            entrypoint_path=
            "s3://ttd-build-artefacts/eldorado-core/mergerequests/jlw-AIFUN-1708-sample-mlflow-training/latest/monitoring-scripts/model_training.py",
            args=["--env", env.execution_env, "--name", model_name],
            job_name="databricks-test-model-training-v2",
            whl_paths=[
                "s3://ttd-build-artefacts/eldorado-core/mergerequests/jlw-AIFUN-1708-sample-mlflow-training/latest/monitoring-scripts/ttd_mlflow-0.12.5-py3-none-any.whl"
            ],
        ),
    ],
    databricks_spark_version="15.4.x-scala2.12",
    spark_configs={
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
    },
    retries=3,
    retry_delay=timedelta(minutes=5),
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
    retries=1,
    retry_delay=timedelta(minutes=10),
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

####################################################################################################################
# Model training step
####################################################################################################################

training_cluster = DockerEmrClusterTask(
    name="training",
    image_name=training_docker_image_name,
    image_tag=training_docker_image_tag,
    docker_registry=training_docker_registry,
    entrypoint_in_image="/lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": "AIFUN"},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri="s3://ttd-identity/datapipeline/logs/airflow/mlflow-smoke-test",
)

emr_training_step = DockerRunEmrTask(name="training", docker_run_command=docker_command.build_command())

####################################################################################################################
# promotion via python operator
####################################################################################################################


def _promote_model(model_name: str, job_cluster_id: str, stage: str) -> None:
    # pylint: disable=import-outside-toplevel
    from ttd_mlflow import TtdMlflow

    client = TtdMlflow()
    model_versions = client.search_model_versions_by_name_and_job_cluster_id(model_name, job_cluster_id)
    assert (
        len(model_versions) == 1
    ), f"There should be exactly one model version trained under {model_name=} with {job_cluster_id=}, but got {len(model_versions)}"
    model_version = model_versions[0].version
    client.transition_model_version_stage(name=model_name, version=model_version, stage=stage)


def _get_promote_task(model_name: str, job_cluster_id: str, stage: str, additional_task_suffix: str) -> OpTask:
    return OpTask(
        op=PythonVirtualenvOperator(
            task_id=f"promote_model_to_{stage}_{additional_task_suffix}",
            python_callable=_promote_model,
            requirements="ttd-mlflow",
            index_urls=[
                "https://pypi.org/simple", "https://nex.adsrvr.org/repository/pypi/simple",
                "https://nex.adsrvr.org/repository/ttd-pypi-dev/simple"
            ],
            # pylint: disable=use-dict-literal
            op_kwargs=dict(model_name=model_name, job_cluster_id=job_cluster_id, stage=stage),
        )
    )


promote_databricks_model_to_staging = _get_promote_task(
    model_name=model_name,
    job_cluster_id="{{task_instance.xcom_pull(task_ids='databricks-test-model-training-v2_run', key='run_id')}}",
    stage="staging",
    additional_task_suffix="databricks"
)

promote_emr_model_to_staging = _get_promote_task(
    model_name=model_name,
    job_cluster_id="{{task_instance.xcom_pull(task_ids='training_add_task_training', key='job_flow_id')}}",
    stage="staging",
    additional_task_suffix="emr"
)

training_cluster.add_parallel_body_task(emr_training_step)

dag >> training_cluster
emr_training_step >> promote_emr_model_to_staging
model_training_on_databricks >> promote_databricks_model_to_staging
dag >> model_training_on_databricks

adag = dag.airflow_dag
