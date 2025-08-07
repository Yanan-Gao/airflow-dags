"""DAG for main analysis Job."""
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State
from ttd.el_dorado.v2.base import TtdDag
from datetime import datetime
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
import json
import logging
from ttd.tasks.op import OpTask
from ttd.python_versions import PythonVersions
from ttd.spark import (
    EbsConfiguration,
    SparkVersionSpec,
    SparkWorkflow,
    CustomBackends,
    Databricks,
)
from ttd.eldorado.databricks.databricks_runtime import DatabricksRuntimeVersion
from ttd.spark_workflow.tasks.pyspark_task import PySparkTask
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator

env = TtdEnvFactory.get_from_system().execution_env
env_prefix = "env=prod" if env == "prod" else "env=test"
instance_id = "CSOL-12422"

dag_name = "campaign_overlap_analysis_main"
file_key = "combinations_input.csv"
s3_bucket = "ttd-csol-12422-campaign-reach-overlap-useast"
record_list = "record_list"
max_runs = 150
worker_instance_count = 8
spark_config = [("conf", "spark.sql.shuffle.partitions=auto")]
code_base_path = "s3://ttd-build-artefacts/csol/csol-12422-reach-overlap/"
code_sub_dir = "release/latest" if env == "prod" else "mergerequests/zhy-CSOL-13233-ftr-dag-test/latest"
python_wheel_name = "campaign_reach_overlap-1.0.0-py3-none-any.whl"
python_wheel_path = f"{code_base_path}{code_sub_dir}/{python_wheel_name}"
file_check_entry_point = f"{code_base_path}{code_sub_dir}/code/file_check.py"
analysis_job_entry_point = f"{code_base_path}{code_sub_dir}/code/analysis_subtasks.py"
consolidate_reports_entry_point = f"{code_base_path}{code_sub_dir}/code/consolidate_report.py"

# DAG

# The top-level dag
campaign_overlap_analysis_main_dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 7, 22),
    schedule_interval="*/30 * * * *" if env == "prod" else None,
    max_active_runs=1,
    slack_channel=f"#csol-12422-campaign-reach-overlap-notification-{'prod' if env == 'prod' else 'qa'}",
    slack_alert_only_for_prod=False,
    tags=["CSOL"],
)

adag: DAG = campaign_overlap_analysis_main_dag.airflow_dag


def starter_task():
    """Starter Python Task."""
    logging.info("Starting of Sub tasks..")


# Spark Job for file check
file_check_pyspark_test = PySparkTask(
    task_name="file_check_pyspark_test",
    python_entrypoint_location=file_check_entry_point,
    additional_command_line_arguments=
    ["--env", env, "--instance-id", instance_id, "--run-id", "{{ run_id | replace(':', '_') | replace('-', '_') }}"],
)

file_check_process_task = SparkWorkflow(
    job_name="file_check_process_task",
    instance_type=M5.m5_large(),
    driver_instance_type=M5.m5_large(),
    instance_count=2,
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_11.value,
    whls_to_install=[python_wheel_path],
    ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
    tasks=[file_check_pyspark_test],
    retries=0,
    backend_chooser=CustomBackends(runtimes=[Databricks()]),
    candidate_databricks_runtimes=[DatabricksRuntimeVersion.DB_15_4.value],
    tags={
        "Process": "Elililly-campaign-overlap-analysis",
        "Team": "CSOL"
    },
)


def fetch_sub_task_combinations(file_key, **kwargs):
    """Fetch sub task combinations from S3 and push it to XCOM."""
    try:
        logging.info(f"file key is = {file_key}")
        aws_storage = AwsCloudStorage(conn_id='aws_default')
        # Check if file exists else skip all processing.
        if aws_storage.check_for_key(file_key, s3_bucket):
            logging.info(f"File {file_key} found in {s3_bucket}.")
        else:
            logging.info(f"File {file_key} not found in {s3_bucket}. Skipping downstream tasks.")
            raise AirflowSkipException("Skipping downstream tasks because file is missing.")

        file_content = aws_storage.read_key(file_key, s3_bucket)
        # Parse JSON content
        data = json.loads(file_content)
        # Validate: list of lists of dicts
        if not isinstance(data, list) or not all(isinstance(sublist, list) for sublist in data):
            raise ValueError("Expected a list of lists")

        if not all(isinstance(rec, dict) for sublist in data for rec in sublist):
            raise ValueError("Expected dictionaries inside each sub list")
        data_as_str = [json.dumps(sublist) for sublist in data]
        logging.info(data_as_str)
        if len(data) > max_runs:
            raise ValueError("Number of subtasks are more than defined max limit.")
        kwargs["ti"].xcom_push(key="record_list", value=data_as_str)
        kwargs['ti'].xcom_push(key='record_count', value=len(data))
        return len(data)
    except Exception as e:
        raise


def check_trigger_branch(index, **kwargs):
    """Check record index and retrun valid processing or emoty task."""
    record_count = kwargs['ti'].xcom_pull(key='record_count', task_ids='fetch_sub_task_combinations_task')
    if index < record_count:
        return f"process_task_alias_{index}"
    else:
        return f"empty_task_{index}"


def subtasks_finish_signal(**context):
    """Check if fetch_sub_task_combinations_task skipped or success and skip/trigger downstream as applicable."""
    dag_run = context['dag_run']
    task_id_to_check = 'fetch_sub_task_combinations_task'
    task_instance = dag_run.get_task_instance(task_id=task_id_to_check)
    if task_instance is None or task_instance.state == State.SKIPPED or task_instance.state == State.FAILED:
        raise AirflowSkipException(f"Task instance {task_id_to_check} not found or skipped/failed, skipping final task")
    else:
        logging.info("All subtaks have finished, starting consolidation report task.")


start_task = OpTask(op=PythonOperator(dag=adag, task_id="start_task", python_callable=starter_task))

# Fetch task Python operator
fetch_sub_task_combinations_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id="fetch_sub_task_combinations_task",
        python_callable=fetch_sub_task_combinations,
        provide_context=True,
        op_kwargs={"file_key": f"{env_prefix}/pair_files/{instance_id}/{{{{ run_id | replace(':', '_') | replace('-', '_') }}}}.json"},
    )
)

# Dummy Task to manage dependecies and apply trigger rules

subtasks_finish_signal_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id="subtasks_finish_signal_task",
        python_callable=subtasks_finish_signal,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )
)

# Spark task for report consolidation.
consolidate_reports_pyspark_test = PySparkTask(
    task_name="consolidate_reports_pyspark_test",
    python_entrypoint_location=consolidate_reports_entry_point,
    additional_command_line_arguments=
    ["--env", env, "--instance-id", instance_id, "--run-id", "{{ run_id | replace(':', '_') | replace('-', '_') }}"],
)

consolidate_reports_process_task = SparkWorkflow(
    job_name="consolidate_reports_process_task",
    instance_type=M5.m5_large(),
    driver_instance_type=M5.m5_large(),
    instance_count=2,
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_11.value,
    whls_to_install=[python_wheel_path],
    ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
    tasks=[consolidate_reports_pyspark_test],
    retries=0,
    backend_chooser=CustomBackends(runtimes=[Databricks()]),
    candidate_databricks_runtimes=[DatabricksRuntimeVersion.DB_15_4.value],
    tags={
        "Process": "Elililly-campaign-overlap-analysis",
        "Team": "CSOL"
    },
)


def create_dynamic_tasks():
    """Function to create dyanmic task based on input S3 file, It uses branch python operator to decide the path."""
    for index in range(max_runs):
        # Branch Python operator to check if index is valid and decide DAG workflow path.
        check_task = OpTask(
            op=BranchPythonOperator(
                task_id=f"check_trigger_branch_{index}",
                python_callable=check_trigger_branch,
                provide_context=True,
                op_kwargs={"index": index},
                dag=adag,
            )
        )

        # Main spark job for analysis processing.
        task_start = PySparkTask(
            task_name=f"campaign_overlap_analysis_subtask_{index}",
            python_entrypoint_location=analysis_job_entry_point,
            additional_command_line_arguments=[
                "--env", env, "--instance-id", instance_id, "--run-id", "{{ run_id | replace(':', '_') | replace('-', '_') }}", "--record",
                "{{ ti.xcom_pull(task_ids='fetch_sub_task_combinations_task', key='record_list')[" + str(index) + "] }}"
            ],
        )

        process_task = SparkWorkflow(
            job_name=f"process_task_{index}",
            instance_type=I3.i3_2xlarge(),
            driver_instance_type=I3.i3_2xlarge(),
            instance_count=worker_instance_count,
            spark_version=SparkVersionSpec.SPARK_3_5_0.value,
            python_version=PythonVersions.PYTHON_3_11.value,
            whls_to_install=[python_wheel_path],
            ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
            tasks=[task_start],
            retries=0,
            backend_chooser=CustomBackends(runtimes=[Databricks()]),
            candidate_databricks_runtimes=[DatabricksRuntimeVersion.DB_15_4.value],
            tags={
                "Process": "Elililly-campaign-overlap-analysis",
                "Team": "CSOL"
            },
        )

        # Empty task for invalid index
        process_task_alias = OpTask(op=EmptyOperator(task_id=f"process_task_alias_{index}", dag=adag))
        empty_task = OpTask(op=EmptyOperator(task_id=f"empty_task_{index}", dag=adag))
        # Set dependencies
        fetch_sub_task_combinations_task >> check_task
        check_task >> [process_task_alias, empty_task]
        process_task_alias >> process_task
        process_task >> subtasks_finish_signal_task
        empty_task >> subtasks_finish_signal_task


# Set task dependencies
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag, trigger_rule=TriggerRule.NONE_FAILED))
campaign_overlap_analysis_main_dag >> start_task >> file_check_process_task >> fetch_sub_task_combinations_task
create_dynamic_tasks()
subtasks_finish_signal_task >> consolidate_reports_process_task >> final_dag_status_step
