from datetime import datetime, timedelta

import logging

from airflow.operators.python import PythonOperator

from dags.forecast.validation.validation_helpers.aws_helper import FORECASTING_BUCKET
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.slack.slack_groups import FORECAST
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

dag_name = "activity-stable-runner-ts"

# Environment
env = TtdEnvFactory.get_from_system()
run_date_key = "run_date"
calculate_run_date_task_taskid = "calculate_run_date_activity_stable"
batch_size = '200'
read_env = env.dataset_read_env
write_env = env.dataset_write_env
adgroup_truth_prefix = f"env={read_env}/forecast-validation/raw-stable-adgroups-spend-metrics/"

###############################################################################
# DAG
###############################################################################


def xcom_template_to_get_value(taskid: str, key: str) -> str:
    global dag_name
    return f'{{{{ ' \
           f'task_instance.xcom_pull(dag_id="{dag_name}", ' \
           f'task_ids="{taskid}", ' \
           f'key="{key}") ' \
           f'}}}}'


def extract_date_from_path(path):
    path_components = path.split("/")
    date_str = path_components[len(path_components) - 2]
    if "=" in date_str:
        date_components = date_str.split("=")
        return date_components[len(date_components) - 1]
    else:
        return date_str


def get_latest_date_from_s3_path(
    s3_bucket, s3_prefix, s3_hook=None, should_check_success_file=False, success_file_key=None, date_format="%Y-%m-%d"
):
    if s3_hook is None:
        s3_hook = AwsCloudStorage(conn_id='aws_default')

    s3_prefix_list = s3_hook.list_prefixes(bucket_name=s3_bucket, prefix=s3_prefix, delimiter="/")
    if s3_prefix_list is None:
        return None
    logging.info(f's3 keys are {s3_prefix_list}')

    filtered_s3_prefix_list = filter(lambda x: s3_hook.check_for_key(key=f"{x}{success_file_key}", bucket_name=s3_bucket), s3_prefix_list) \
        if should_check_success_file \
        else s3_prefix_list

    today = datetime.today()
    processed_dates = list(
        filter(
            lambda exec_date: exec_date <= today,
            (map(lambda date_str: datetime.strptime(date_str, "%Y-%m-%d"), map(extract_date_from_path, filtered_s3_prefix_list)))
        )
    )

    if not processed_dates:
        return None
    return max(processed_dates)


def calculate_run_date(**context):
    # Check the last processed dataset in the output folder
    hook = AwsCloudStorage(conn_id='aws_default')
    max_processed_date = get_latest_date_from_s3_path(
        FORECASTING_BUCKET, adgroup_truth_prefix, s3_hook=hook, should_check_success_file=False, date_format="%Y-%m-%d"
    )
    logging.info(f'max_processed_date= {max_processed_date}')

    if max_processed_date is not None:
        # Need to run DAG
        logging.info("Executing DAG")
        # Set the rundate
        run_date_str = max_processed_date.strftime("%Y-%m-%d")
        logging.info(f"Set rundate to {run_date_str}")
        context['ti'].xcom_push(key=run_date_key, value=run_date_str)
        logging.info(
            f"Rundate pushed is {context['ti'].xcom_pull(dag_id=dag_name, task_ids=calculate_run_date_task_taskid, key=run_date_key)}"
        )


# The top-level dag
live_adgroup_validation_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime.now() - timedelta(hours=3),
    schedule_interval="*/10 * * * *",  # Every 10 minutes
    retries=0,
    retry_delay=timedelta(minutes=1),
    max_active_runs=1,
    slack_alert_only_for_prod=True,
    tags=[FORECAST.team.jira_team],
    slack_tags=FORECAST.team.jira_team,
    slack_channel="#dev-forecasting-validation-alerts",
    enable_slack_alert=True
)

dag = live_adgroup_validation_dag.airflow_dag

calculate_run_date_task = OpTask(
    op=PythonOperator(task_id=calculate_run_date_task_taskid, python_callable=calculate_run_date, dag=dag, provide_context=True)
)

live_adgroup_validation_task = OpTask(
    op=TaskServiceOperator(
        task_name="ActivityStableValidationTask",
        task_config_name="ActivityStableValidationTaskConfig",
        scrum_team=FORECAST.team,
        retries=3,
        resources=TaskServicePodResources.large(),
        telnet_commands=[],
        configuration_overrides={
            "ActivityStableValidationTask.RunDate": xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key),
            "ActivityStableValidationTask.AdGroupsBatchSize": batch_size,
            "ActivityStableValidationTask.ReadEnv": read_env,
            "ActivityStableValidationTask.WriteEnv": write_env,
        },
        # branch_name="release-2025.05.06",  # Uncomment this line and change branch name for testing branch deployment code
    )
)

# DAG dependencies
live_adgroup_validation_dag >> calculate_run_date_task >> live_adgroup_validation_task
