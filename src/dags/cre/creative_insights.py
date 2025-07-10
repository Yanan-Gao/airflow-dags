from datetime import datetime, timedelta

from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python import PythonOperator

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.ec2.emr_instance_types.graphics_optimized.g4 import G4

from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.databricks_runtime import DatabricksRuntimeVersion
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.python_versions import PythonVersions
from ttd.spark import SparkWorkflow, SparkVersionSpec, Databricks, CustomBackends, EbsConfiguration, AwsEmr
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.spark_workflow.tasks.pyspark_task import PySparkTask
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "creative-insights"

###############################################################################
# DAG
###############################################################################

# The top-level dag
creative_insights_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 10, 30),
    schedule_interval='30 09 * * *',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel="#dev-creatives-alerts",
    slack_alert_only_for_prod=True,
    dag_tsg="https://thetradedesk.atlassian.net/wiki/spaces/EN/pages/364511255/Creative+Insights+DAG+Failure",
    tags=["CRE"],
)

dag = creative_insights_dag.airflow_dag

env = TtdEnvFactory.get_from_system().execution_env

job_datetime_format: str = "%Y-%m-%dT%H:00:00"
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
logworkflow_prod_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
logworkflow_connection = logworkflow_prod_connection if env == "prod" else logworkflow_sandbox_connection
gating_type_ids = [2000494, 2000495]
whls_to_install = "s3://thetradedesk-mlplatform-us-east-1/libs/creative-insights/pyspark-jobs/dist/mergerequests/airflow/latest/creative_insights-0.0.0-py3-none-any.whl"
bucket_name = "ttd-creative-insights"
s3_folder_jobs = f"s3://{bucket_name}/env={env}/jobs"
date = '{{ data_interval_end.strftime("%Y%m%d") }}'

preprocessing_instance_count = 30
processing_instance_count = 10
embedding_instance_count = 10
generate_external_dataset_instance_count = 1

image_loader_concurrency = processing_instance_count / 10 * 6
image_processor_concurrency = processing_instance_count
ray_worker_nodes = processing_instance_count

ebs_root_volume_size = 64
spark_config = [("conf", "spark.sql.parquet.int96RebaseModeInRead=LEGACY")]
additional_application = ["Hadoop"]
embedding_bootstrap_actions = [
    ScriptBootstrapAction(
        path=f"s3://{bucket_name}/env={env}/bootstrap/download_model.sh", args=[f"s3://{bucket_name}/env={env}/models/Onnx-SetFit/"]
    )
]

###############################################################################
# datasets
###############################################################################
creative_insights_dataset = DateGeneratedDataset(
    bucket="ttd-creative-insights",
    path_prefix=f"env={env}",
    data_name="datasets/creative-insight",
    version=3,
    date_format="date=%Y%m%d",
    env_aware=False,
)

creative_embeddings_dataset = DateGeneratedDataset(
    bucket="ttd-creative-insights",
    path_prefix=f"env={env}",
    data_name="datasets/creative-embeddings",
    version=4,
    date_format="date=%Y%m%d",
    env_aware=False,
)


###############################################################################
# functions - DataMover
###############################################################################
def check_incoming_data_exists(**context):
    dt = _get_time_slot(context['data_interval_end'])
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    if not creative_insights_dataset.check_data_exist(cloud_storage, dt):
        raise AirflowNotFoundException("S3 _SUCCESS file not found for CreativeInsights!")
    if not creative_embeddings_dataset.check_data_exist(cloud_storage, dt):
        raise AirflowNotFoundException("S3 _SUCCESS file not found for CreativeEmbeddings!")
    return True


def _open_lwdb_gate(**context):
    dt = _get_time_slot(context['data_interval_end'])
    log_start_time = dt.strftime('%Y-%m-%d %H:00:00')
    for gating_type_id in gating_type_ids:
        ExternalGateOpen(
            mssql_conn_id=logworkflow_connection,
            sproc_arguments={
                'gatingType': gating_type_id,
                'grain': TaskBatchGrain_Daily,
                'dateTimeToOpen': log_start_time
            }
        )


def _get_time_slot(dt: datetime):
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    return dt


###############################################################################
# operators - DataMover
###############################################################################
check_datasets_are_not_empty = OpTask(
    op=PythonOperator(
        task_id='check_datasets_are_not_empty',
        python_callable=check_incoming_data_exists,
        provide_context=True,
        dag=dag,
    )
)

open_lwdb_gate_task = OpTask(
    op=PythonOperator(
        task_id='open_lwdb_gate_task',
        python_callable=_open_lwdb_gate,
        provide_context=True,
        dag=dag,
    )
)

###############################################################################
# operators - databricks
###############################################################################

image_preprocessing_task = PySparkTask(
    task_name="image_preprocessing_task",
    python_entrypoint_location=f"{s3_folder_jobs}/ImagePreprocessingJob.py",
    additional_command_line_arguments=
    ["--date", date, "--env", env, "--bucket_name", bucket_name, "--full_run", "False", "--max_lookback", "64"],
)

image_processing_task = PySparkTask(
    task_name="image_processing_task",
    python_entrypoint_location=f"{s3_folder_jobs}/ImageProcessingJob.py",
    additional_command_line_arguments=[
        "--date", date, "--env", env, "--bucket_name", bucket_name, "--image_loader_concurrency",
        str(int(image_loader_concurrency)), "--image_processor_concurrency",
        str(image_processor_concurrency), "--ray_worker_nodes",
        str(ray_worker_nodes), "--model_path", f"s3://{bucket_name}/env={env}/models/InternVL2-2B/", "--local_model_path",
        "/dbfs/tmp/InternVL2-2B", "--batch_size", "16"
    ]
)

embeddings_task = PySparkTask(
    task_name="embeddings_task",
    python_entrypoint_location=f"{s3_folder_jobs}/EmbeddingsJob.py",
    additional_command_line_arguments=
    ["--date", date, "--env", env, "--bucket_name", bucket_name, "--process_all_creative_types", "False", "--use-databricks", "True"]
)

creative_result_sync_task = PySparkTask(
    task_name="creative_result_sync_task",
    python_entrypoint_location=f"{s3_folder_jobs}/CreativeResultSyncJob.py",
    additional_command_line_arguments=
    ["--date", date, "--env", env, "--bucket_name", bucket_name, "--full_run", "False", "--reset", "False", "--max_lookback", "64"],
)

generate_external_datasets_task = PySparkTask(
    task_name="generate_external_datasets_task",
    python_entrypoint_location=f"{s3_folder_jobs}/GenerateExternalDatasets.py",
    additional_command_line_arguments=["--date", date, "--env", env, "--bucket_name", bucket_name],
)

preprocessing_workflow = SparkWorkflow(
    job_name="CRE-Insights-ImagePreprocessing",
    instance_type=I3.i3_2xlarge(),
    instance_count=preprocessing_instance_count,
    driver_instance_type=I3.i3_2xlarge(),
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_10.value,
    whls_to_install=[whls_to_install],
    ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
    tasks=[image_preprocessing_task],
    backend_chooser=CustomBackends(runtimes=[AwsEmr()]),
    spark_configurations=spark_config,
    ebs_root_volume_size=ebs_root_volume_size,
    additional_application=additional_application,
    deploy_mode="client",
    tags={
        "Process": "databricks-creative-insights-CRE",
        "Team": "CRE"
    }
)

processing_workflow = SparkWorkflow(
    job_name="CRE-Insights-ImageProcessing",
    instance_type=G5.g5_8xlarge(),
    instance_count=processing_instance_count,
    driver_instance_type=G5.g5_4xlarge(),
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_10.value,
    whls_to_install=[whls_to_install],
    tasks=[image_processing_task],
    backend_chooser=CustomBackends(runtimes=[Databricks()]),
    candidate_databricks_runtimes=[DatabricksRuntimeVersion.ML_GPU_DB_15_4.value],
    tags={
        "Process": "databricks-creative-insights-CRE",
        "Team": "CRE"
    }
)

embeddings_workflow = SparkWorkflow(
    job_name="CRE-Insights-Embddings",
    instance_type=G4.g4dn_4xlarge(),
    instance_count=10,
    driver_instance_type=G4.g4dn_4xlarge(),
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_10.value,
    whls_to_install=[whls_to_install],
    tasks=[embeddings_task],
    backend_chooser=CustomBackends(runtimes=[Databricks()]),
    candidate_databricks_runtimes=[DatabricksRuntimeVersion.ML_GPU_DB_14_3.value],
    tags={
        "Process": "databricks-creative-insights-CRE",
        "Team": "CRE"
    }
)

creative_result_sync_task >> generate_external_datasets_task

syncing_workflow = SparkWorkflow(
    job_name="CRE-Insights-Syncing",
    instance_type=M5.m5_4xlarge(),
    instance_count=1,
    driver_instance_type=M5.m5_4xlarge(),
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_10.value,
    whls_to_install=[whls_to_install],
    ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
    tasks=[creative_result_sync_task, generate_external_datasets_task],
    backend_chooser=CustomBackends(runtimes=[AwsEmr()]),
    spark_configurations=spark_config,
    ebs_root_volume_size=ebs_root_volume_size,
    additional_application=additional_application,
    deploy_mode="client",
    tags={
        "Process": "databricks-creative-insights-CRE",
        "Team": "CRE"
    }
)

# DAG dependencies
creative_insights_dag >> preprocessing_workflow >> processing_workflow >> embeddings_workflow >> syncing_workflow >> check_datasets_are_not_empty >> open_lwdb_gate_task
