import functools
from datetime import date, datetime, timedelta
from typing import Final, Iterable, List

from airflow.models import Param, TaskInstance
from airflow.operators.python import PythonOperator

from dags.forecast.utils.model_serving.mr.types import \
    ModelServingEndpointDetails
from dags.forecast.utils.model_serving.task import ModelServingTask
from dags.psr.model_validation.validation_functions import (
    MetricValidationFunction, ModelValidationError, is_abs_diff_of_under_and_above_target_less_than_threshold,
    is_greater_than_on_target_threshold
)
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.graphics_optimized.g4 import G4
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.ec2.emr_instance_types.storage_optimized.i4i import I4i
from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.databricks_runtime import DatabricksRuntimeSpecification, DatabricksRuntimeVersion
from ttd.eldorado.xcom.helpers import get_push_xcom_task_id, get_xcom_pull_jinja_string
from ttd.slack.slack_groups import PSR
from ttd.spark import Databricks, SingleBackend, SparkWorkflow
from ttd.spark_workflow.tasks.pyspark_task import PySparkTask
from ttd.spark_workflow.tasks.spark_task import SparkTask
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

####################################################################################################################
# DAG
####################################################################################################################


def _get_env():
    _env = TtdEnvFactory.get_from_system()
    return _env.execution_env.lower() if _env in (TtdEnvFactory.prod, TtdEnvFactory.dev) else TtdEnvFactory.test.execution_env.lower()


DAG_NAME = "adgroup-embedding-training"
TASK_TAGS = {
    "Process": DAG_NAME,
    "Team": PSR.team.jira_team,
}
DEFAULT_S3_BUCKET = "ttd-psr"
DEFAULT_S3_ROOT_FOLDER = f"env={_get_env()}"
_MODEL_TYPES: Final = ("da", "legacy", "da-bagged", "legacy-bagged")

adgroup_embeddings_training_dag: TtdDag = TtdDag(
    dag_id=DAG_NAME,
    start_date=datetime(2025, 1, 22, 2, 0),
    schedule_interval=timedelta(days=1),
    retries=0,
    max_active_runs=1,
    depends_on_past=False,  # The current execution does not depend on any previous execution.
    run_only_latest=True,  # If we miss previous weeks, we don't care, just run the current one
    enable_slack_alert=False,  # TODO Reenable slack alerts
    slack_alert_only_for_prod=True,
    slack_tags=PSR.team.alarm_channel,
    slack_channel=PSR.team.alarm_channel,
    tags=[PSR.team.jira_team],
    default_args={"owner": PSR.team.jira_team},
    params={
        "s3_bucket":
        Param(DEFAULT_S3_BUCKET, type="string"),
        "s3_root_folder":
        Param(DEFAULT_S3_ROOT_FOLDER, type="string", enum=["env=prod", "env=test", "env=dev"]),
        "s3_tasks_location":
        Param(
            f"s3://{DEFAULT_S3_BUCKET}/{DEFAULT_S3_ROOT_FOLDER}/model/airflow/release/latest/tasks",
            type="string",
            pattern=r"^s3:\/\/\S+[^\/]$",  # starts with "s3://", has no-space-chars and does not end with "/"
        ),
        "s3_wheels_location":
        Param(
            f"s3://{DEFAULT_S3_BUCKET}/{DEFAULT_S3_ROOT_FOLDER}/model/airflow/release/latest/dist",
            type="string",
            pattern=r"^s3:\/\/\S+[^\/]$",  # starts with "s3://", has no-space-chars and does not end with "/"
        ),
        "data_extraction_date_inclusive":
        Param(date.today().strftime("%Y-%m-%d"), type="string", format="date"),
        "training_timewindow_in_days":
        Param(14, type="integer", minimum=1),
    },
)

dag = adgroup_embeddings_training_dag.airflow_dag

# https://gitlab.adsrvr.org/thetradedesk/teams/psr/adgroup-embedding-training/-/tree/master/tasks?ref_type=heads
# Under typical usage, this DAG runs files located in this repo

####################################################################################################################
# Tasks
####################################################################################################################
_s3_command_line_arguments: List[str] = [
    "--bucket-name",
    "{{ params.s3_bucket }}",
    "--bucket-root-folder",
    "{{ params.s3_root_folder }}",
]


def _create_data_extraction_task(data_type: str) -> PySparkTask:
    return PySparkTask(
        task_name=f"extract_{data_type}",
        python_entrypoint_location="{{ params.s3_tasks_location }}/data_extraction.py",
        additional_command_line_arguments=[
            *_s3_command_line_arguments,
            "--data-extraction-date-inclusive",
            "{{ params.data_extraction_date_inclusive }}",
            "--data-type",
            data_type,
        ],
    )


tasks_extract = [_create_data_extraction_task(data_type) for data_type in ["audience", "budget", "currency", "params"]]

_data_processing_command_line_arguments: List[str] = [
    "--training-start-date-inclusive",
    "{{ ds }}",
    "--training-timewindow-in-days",
    "{{ params.training_timewindow_in_days }}",
]

task_rollup_data = PySparkTask(
    task_name="rollup_data",
    python_entrypoint_location="{{ params.s3_tasks_location }}/rollup_data.py",
    additional_command_line_arguments=[*_s3_command_line_arguments, *_data_processing_command_line_arguments],
)


def _create_model_training_task(model_type: str) -> PySparkTask:
    return PySparkTask(
        task_name=f"model_training_{model_type}",
        python_entrypoint_location="{{ params.s3_tasks_location }}/model_training.py",
        additional_command_line_arguments=[
            *_s3_command_line_arguments,
            *_data_processing_command_line_arguments,
            "--model-type",
            model_type,
        ],
        do_xcom_push=True,
    )


tasks_model_training = {model_type: _create_model_training_task(model_type) for model_type in _MODEL_TYPES}

# Here we define our thresholds for model performance
# If our metric is not of "acceptable" quality, this model will be rejected
# https://thetradedesk.slack.com/archives/C0501GW7C8G/p1740067710868349?thread_ts=1739909495.707449&cid=C0501GW7C8G
_MODEL_TYPE_TO_METRIC_VALIDATION_FUNCTIONS: Final = {
    "da": [
        functools.partial(is_greater_than_on_target_threshold, threshold=40.0),
        functools.partial(is_abs_diff_of_under_and_above_target_less_than_threshold, threshold=10.0),
    ],
    "legacy": [
        functools.partial(is_greater_than_on_target_threshold, threshold=35.0),
        functools.partial(is_abs_diff_of_under_and_above_target_less_than_threshold, threshold=5.0)
    ],
}
# The bagged models can have the same thresholds as the non-bagged models for now
_MODEL_TYPE_TO_METRIC_VALIDATION_FUNCTIONS.update({
    f"{model_type}-bagged": validation_functions
    for model_type, validation_functions in _MODEL_TYPE_TO_METRIC_VALIDATION_FUNCTIONS.items()
})

models_without_validation = set(_MODEL_TYPES) - set(_MODEL_TYPE_TO_METRIC_VALIDATION_FUNCTIONS)

if models_without_validation:
    raise AssertionError(f"The following models have no validation checks: {models_without_validation}")


def _validate_model_test_metrics(
    model_training_task_name: str, metric_validation_functions: Iterable[MetricValidationFunction], task_instance: TaskInstance, **context
):
    spend_test_metrics = task_instance.xcom_pull(task_ids=get_push_xcom_task_id(model_training_task_name), key="spend_test_metrics")
    exceptions = []
    for metric_validation_function in metric_validation_functions:
        try:
            metric_validation_function(spend_test_metrics=spend_test_metrics)
        except ModelValidationError as e:
            exceptions.append(e)
    if exceptions:
        raise ExceptionGroup("There were model validation errors", exceptions)


def _create_model_validation_task(
    model_type: str,
    metric_validation_functions: Iterable[MetricValidationFunction],
) -> OpTask:
    return OpTask(
        op=PythonOperator(
            task_id=f"{model_type}_model_validation",
            python_callable=_validate_model_test_metrics,
            op_kwargs=dict(
                model_training_task_name=tasks_model_training[model_type].task_name,
                metric_validation_functions=metric_validation_functions,
            )
        )
    )


tasks_model_validation = {
    model_type: _create_model_validation_task(model_type, metric_validation_functions)
    for model_type, metric_validation_functions in _MODEL_TYPE_TO_METRIC_VALIDATION_FUNCTIONS.items()
}


def _create_model_packing_task(model_type: str) -> PySparkTask:
    return PySparkTask(
        task_name=f"model_packing_{model_type}",
        python_entrypoint_location="{{ params.s3_tasks_location }}/model_packing.py",
        additional_command_line_arguments=[
            *_s3_command_line_arguments,
            *_data_processing_command_line_arguments,
            "--model-type",
            model_type,
        ],
        do_xcom_push=True,
    )


for task_extract in tasks_extract:
    task_extract >> task_rollup_data

tasks_model_packing = {model_type: _create_model_packing_task(model_type) for model_type in _MODEL_TYPES}

####################################################################################################################
# Cluster / Workflows
####################################################################################################################
whls = ["{{ params.s3_wheels_location }}/adgroup_embedding_training-0.0.0-py3-none-any.whl"]


def _create_spark_workflow_on_databricks(
    job_name: str,
    instance_type: EmrInstanceType,
    databricks_runtime: DatabricksRuntimeSpecification,
    tasks: list[SparkTask],
    **kwargs,
) -> SparkWorkflow:
    return SparkWorkflow(
        job_name=job_name,
        tasks=tasks,
        instance_type=instance_type,
        instance_count=1,
        spark_version=databricks_runtime.spark_version,
        python_version=databricks_runtime.python_version,
        tags=TASK_TAGS,
        retries=2,
        # Leaving 30 mins between each retry
        # This way we should hopefully avoid transient issues
        retry_delay=timedelta(minutes=30),
        whls_to_install=whls,
        backend_chooser=SingleBackend(Databricks()),
        candidate_databricks_runtimes=[databricks_runtime],
        **kwargs,
    )


# TODO For all workflows, we should consider tuning compute, particularly for the driver
# Also, worth checking if we can use single node mode in some cases to avoid double compute

data_extraction_workflow = _create_spark_workflow_on_databricks(
    job_name="data-extraction",
    instance_type=I4i.i4i_32xlarge(),
    databricks_runtime=DatabricksRuntimeVersion.DB_15_4.value,
    tasks=tasks_extract + [
        task_rollup_data,
    ],
)

training_task_workflows = {
    model_type:
    _create_spark_workflow_on_databricks(
        job_name=f"training_{model_type}",
        # Let's downsize the worker to save money
        instance_type=G4.g4dn_xlarge(),
        # Ideally we would run on single node, the worker is basically redundant
        # Can we reduce the driver size without impacting training time?
        # Or perhaps we can speed up training by increasing batch size (might make the model overfit)
        driver_instance_type=G5.g5_48xlarge(),
        databricks_runtime=DatabricksRuntimeVersion.ML_GPU_DB_15_4.value,
        tasks=[task_model_training],
    )
    for model_type, task_model_training in tasks_model_training.items()
}

packing_task_workflows = {
    model_type:
    _create_spark_workflow_on_databricks(
        job_name=f"packing_{model_type}",
        # Ideally we would run on single node
        instance_type=G4.g4dn_xlarge(),
        # We need this specific runtime, as we rely on its specific preinstalled torch version
        # (torch==2.0.1+cu118) not to segfault when packing the model.
        databricks_runtime=DatabricksRuntimeVersion.ML_GPU_DB_14_3.value,
        tasks=[task_model_packing],
    )
    for model_type, task_model_packing in tasks_model_packing.items()
}


def _create_model_serving_tasks(model_type: str) -> ModelServingTask:
    task_model_packing_push_xcom_task_id = get_push_xcom_task_id(tasks_model_packing[model_type].task_name)
    return ModelServingTask(
        task_id=f"serve_model_{model_type}",
        branch_name=f"edn-FORECAST-3321-{model_type}-{{{{ ds_nodash }}}}",
        merge_request_token="{{ var.value.model_serving_project_access_token }}",
        approval_token="{{ var.value.model_serving_approval_token }}",
        reviewer_ids=[],  # No reviewers needed, the DAG failing should be warning enough
        model_serving_endpoint_details=ModelServingEndpointDetails(
            team_name=PSR.team.jira_team.lower(),
            endpoint_group="adgroup-embedding",
            endpoint=model_type,
            model_format=get_xcom_pull_jinja_string(
                task_ids=task_model_packing_push_xcom_task_id,
                key="model_format",
            ),
            model_source=get_xcom_pull_jinja_string(
                task_ids=task_model_packing_push_xcom_task_id,
                key="source_uri",
            ),
            allow_endpoint_overwrite=True,
        )
    )


adgroup_embeddings_training_dag >> data_extraction_workflow >> list(training_task_workflows.values())
for model_type in _MODEL_TYPES:
    training_task_workflows[model_type] >> tasks_model_validation[model_type] >> packing_task_workflows[model_type]

# Only serve the model if we are running in prod
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    tasks_model_serving = {model_type: _create_model_serving_tasks(model_type) for model_type in _MODEL_TYPES}
    for model_type in _MODEL_TYPES:
        packing_task_workflows[model_type] >> tasks_model_serving[model_type]
