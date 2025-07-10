# The top-level dag
import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator, BranchPythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.docker import DockerEmrClusterTask, PySparkEmrTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "adpb-two-tower-serving"
owner = ADPB.team

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
model_download_script_path = "s3://ttd-build-artefacts/user-embedding/galamere/scripts/release/download.sh"

job_environment = TtdEnvFactory.get_from_system()
env_path = "prod" if job_environment == TtdEnvFactory.prod else "test"

run_date_key = "run_date"
calculate_run_date_task_taskid = "calculate_run_date"

base_prod_path = "s3://ttd-identity/datapipeline/prod/models/user-embedding"
base_env_path = f"s3://ttd-identity/datapipeline/{env_path}/models/user-embedding"

# Read sources (ok to read from production data)
model_source_bucket = "ttd-identity"
model_source_prefix = "datapipeline/prod/models/user-embedding/twindom/model_save/"
model_input_path = f"{base_prod_path}/dataforgein/public/all_positive_label_with_feature/date="
user_weekly_collection_path = f"{base_prod_path}/dataforgein/backstage/user_weekly_collection/date="

iav2_path_bucket = "thetradedesk-useast-data-import"
iav2_graph_base_path = "sxd-etl/universal/iav2graph/"
fetch_latest_graph_path_taskid = "fetch_latest_graph_path"
iav2_latest_graph_path_key = "iav2_latest_graph_path"
iav_graph_lookback_days = 11

country_path_bucket = "thetradedesk-useast-qubole"
country_base_path = "warehouse.external/thetradedesk.db/provisioning/country/v=1/"
fetch_latest_country_path_taskid = "fetch_latest_country_path"
country_path_key = "country_latest_path"
country_lookback_days = 3

# Write sources (this cases on job environment)
spark_logs_base_path = f"{base_env_path}/galamere/logs"
inferencing_output_path = f"{base_env_path}/galamere/staging/inference-user/date="

embeddings_base_path = f"s3://ttd-user-embeddings{'' if env_path == 'prod' else '/env=test'}"
xd_join_output_path = f"{embeddings_base_path}/dataexport/type=101/date="
aerospike_load_suffix = "/hour=00/batch=01"

base_staging_location_bucket = "ttd-identity"
base_staging_location_prefix = f"datapipeline/{env_path}/models/user-embedding/galamere/staging/inference-user/"

# Docker information
docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
docker_image_name = "ttd-base/scrum-adpb/userembedding_modelserving"
docker_image_tag = "release"

inferencing_executor_cores = 64
inferencing_instances_count = 100

xd_cores = 4  # Although the machine has 96 cores, given we join with the graph it throws a OOM. So keeping it to 4
xd_join_instances_count = 100

job_start_date = datetime(2024, 8, 15, 18, 0)
job_schedule_interval = "@daily"

dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=1,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    enable_slack_alert=True,
    run_only_latest=True
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_16xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_16xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=inferencing_instances_count
)

nogpu_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

nogpu_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5d.m5d_12xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=xd_join_instances_count
)


def extract_date_from_path(path):
    path_components = path.split("/")
    date_str = path_components[len(path_components) - 2]
    if "=" in date_str:
        date_components = date_str.split("=")
        return date_components[len(date_components) - 1]
    else:
        return date_str


def xcom_template_to_get_value(taskid: str, key: str) -> str:
    global dag_name
    return f'{{{{ ' \
           f'task_instance.xcom_pull(dag_id="{dag_name}", ' \
           f'task_ids="{taskid}", ' \
           f'key="{key}") ' \
           f'}}}}'


def calculate_run_date(**context):
    # Check the last processed dataset in the output folder
    hook = AwsCloudStorage(conn_id='aws_default')
    max_processed_date = get_latest_date_from_s3_path(
        base_staging_location_bucket, base_staging_location_prefix, s3_hook=hook, should_check_success_file=False, date_format="%Y%m%d"
    )
    logging.info(f'max_processed_date= {max_processed_date}')

    # Now pick out the last date when model was generated
    max_model_generation_date = get_latest_date_from_s3_path(
        model_source_bucket,
        model_source_prefix,
        s3_hook=hook,
        should_check_success_file=True,
        success_file_key="user_model/saved_model.pb"
    )
    logging.info(f'max_model_generation_date= {max_model_generation_date}')

    if max_processed_date is None or max_model_generation_date > max_processed_date:
        # Need to run DAG
        logging.info("Executing DAG")
        # Set the rundate
        run_date_str = max_model_generation_date.strftime("%Y%m%d")
        logging.info(f"Set rundate to {run_date_str}")
        context['ti'].xcom_push(key=run_date_key, value=run_date_str)
        logging.info(
            f"Rundate pushed is {context['ti'].xcom_pull(dag_id=dag_name, task_ids=calculate_run_date_task_taskid, key=run_date_key)}"
        )


calculate_run_date_task = OpTask(
    op=PythonOperator(task_id='calculate_run_date', python_callable=calculate_run_date, dag=adag, provide_context=True)
)


def should_run_dag(**context):
    run_date = context['ti'].xcom_pull(dag_id=dag_name, task_ids=calculate_run_date_task_taskid, key=run_date_key)
    logging.info(f"rundate rcvd is {run_date}")
    if run_date is not None:
        return "branching_task"
    else:
        return "final_dag_status"


should_run_dag_task = OpTask(
    op=BranchPythonOperator(task_id='should_run_dag', python_callable=should_run_dag, dag=adag, provide_context=True)
)


def branching_task_method(**context):
    logging.info(f"Rundate received is {context['ti'].xcom_pull(dag_id=dag_name, task_ids='calculate_run_date', key=run_date_key)}")


branching_task = OpTask(op=PythonOperator(task_id='branching_task', python_callable=branching_task_method, dag=adag, provide_context=True))


def fetch_latest_graph_path(**context):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    exec_date = datetime.strptime(
        context['ti'].xcom_pull(dag_id=dag_name, task_ids=calculate_run_date_task_taskid, key=run_date_key), '%Y%m%d'
    )
    lookback_count = 0
    while lookback_count < iav_graph_lookback_days:
        date_to_check_str = (exec_date + timedelta(-lookback_count)).strftime("%Y-%m-%d")
        path_to_check = iav2_graph_base_path + date_to_check_str + '/success/'
        logging.info(f'checking for path {path_to_check}')
        if cloud_storage.check_for_key(path_to_check + '_SUCCESS', iav2_path_bucket):
            iav2_graph_latest_path = f"s3://{iav2_path_bucket}/{path_to_check}"
            logging.info(f'found path...returning {iav2_graph_latest_path}')
            context['ti'].xcom_push(key=iav2_latest_graph_path_key, value=iav2_graph_latest_path)
            break
        lookback_count = lookback_count + 1
    if lookback_count >= iav_graph_lookback_days:
        raise AirflowFailException(f'IAV2 path not found. Checked back from {exec_date}')


fetch_latest_iav2_graph_task = OpTask(
    op=PythonOperator(task_id='fetch_latest_graph_path', python_callable=fetch_latest_graph_path, dag=adag, provide_context=True)
)


def fetch_latest_country_path(**context):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    exec_date = datetime.strptime(
        context['ti'].xcom_pull(dag_id=dag_name, task_ids=calculate_run_date_task_taskid, key=run_date_key), '%Y%m%d'
    )
    lookback_count = 0
    while lookback_count < country_lookback_days:
        date_to_check_str = (exec_date + timedelta(-lookback_count)).strftime("%Y%m%d")
        path_to_check = f'{country_base_path}date={date_to_check_str}/'
        logging.info(f'checking for path {path_to_check}')
        keys = cloud_storage.list_keys(path_to_check, country_path_bucket)
        if keys:
            country_latest_path = f"s3://{country_path_bucket}/{path_to_check}"
            logging.info(f'found path...returning {country_latest_path}')
            context['ti'].xcom_push(key=country_path_key, value=country_latest_path)
            break
        lookback_count = lookback_count + 1
    if lookback_count >= country_lookback_days:
        raise AirflowFailException(f'Country path not found. Checked back from {exec_date}')


fetch_latest_country_task = OpTask(
    op=PythonOperator(task_id='fetch_latest_country_path', python_callable=fetch_latest_country_path, dag=adag, provide_context=True)
)


def get_latest_date_from_s3_path(
    s3_bucket, s3_prefix, s3_hook=None, should_check_success_file=False, success_file_key=None, date_format="%Y%m%d"
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
            (map(lambda date_str: datetime.strptime(date_str, "%Y%m%d"), map(extract_date_from_path, filtered_s3_prefix_list)))
        )
    )

    if not processed_dates:
        return None
    return max(processed_dates)


inference_cluster_options = [
    ("executor-cores", f"{inferencing_executor_cores}"),
    ("executor-memory", "409G"),
    ("conf", f"num-executors={inferencing_instances_count}"),
    ('conf', 'spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/home/hadoop:/home/hadoop:ro'),
    ('conf', 'spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/home/hadoop:/home/hadoop:ro'),
    ('conf', 'spark.sql.shuffle.partitions=3000'),
    ('conf', 'spark.hadoop.fs.s3a.buffer.dir=/tmp'),
    ('conf', 'spark.sql.execution.arrow.pyspark.enabled=true'),
    ('conf', 'spark.sql.execution.arrow.maxRecordsPerBatch=8192'),
    ('conf', 'spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901'),
    ('conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'),
    ('conf', 'spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads=4'),
]

inferencing_spark_step = PySparkEmrTask(
    name=dag_name,
    entry_point_path="/home/hadoop/app/main.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=inference_cluster_options,
    command_line_arguments=[
        f"--input_path={model_input_path}{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}",
        f"--inference_path={inferencing_output_path}{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}",
        "--run_mode=inference", f"--environment={job_environment}",
        f"--run_date={xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}"
    ],
    timeout_timedelta=timedelta(hours=24)
)

xd_cluster_options = [
    ("executor-cores", f"{xd_cores}"),
    ("conf", f"num-executors={xd_join_instances_count}"),
    ('conf', 'spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/home/hadoop:/home/hadoop:ro'),
    ('conf', 'spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/home/hadoop:/home/hadoop:ro'),
    ('conf', 'spark.sql.shuffle.partitions=960'),
    ('conf', 'spark.hadoop.fs.s3a.buffer.dir=/tmp'),
    ('conf', 'spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901'),
    ('conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'),
    ('conf', 'spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads=4'),
]

xd_join_spark_step = PySparkEmrTask(
    name=dag_name,
    entry_point_path="/home/hadoop/app/main.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=xd_cluster_options,
    command_line_arguments=[
        f"--inference_path={inferencing_output_path}{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}",
        f"--iav2_path={xcom_template_to_get_value(fetch_latest_graph_path_taskid, iav2_latest_graph_path_key)}",
        f"--xd_output_path={xd_join_output_path}{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}{aerospike_load_suffix}",
        "--run_mode=xd", f"--environment={job_environment}",
        f"--run_date={xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}",
        f"--user_weekly_collection_path={user_weekly_collection_path}{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}",
        f"--country_mapping_path={xcom_template_to_get_value(fetch_latest_country_path_taskid, country_path_key)}"
    ],
    timeout_timedelta=timedelta(hours=5)
)

inferencing_cluster = DockerEmrClusterTask(
    name='inferencing',
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="opt/application/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": owner.jira_team},
    environment=job_environment,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri=
    f"{spark_logs_base_path}/{job_environment.dataset_write_env}/inf/{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}/log",
    spark_use_gpu=False,
    bootstrap_script_actions=[
        ScriptBootstrapAction(
            f"{model_download_script_path}", [
                f"s3://{model_source_bucket}/{model_source_prefix}date={xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}/user_model"
            ]
        )
    ]
)

xd_join_cluster = DockerEmrClusterTask(
    name='xd_join',
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="opt/application/app/",
    master_fleet_instance_type_configs=nogpu_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=nogpu_core_fleet_instance_type_configs,
    cluster_tags={"Team": owner.jira_team},
    environment=job_environment,
    enable_prometheus_monitoring=True,
    emr_release_label="emr-6.8.0",
    log_uri=
    f"{spark_logs_base_path}/{job_environment.dataset_write_env}/xd/{xcom_template_to_get_value(calculate_run_date_task_taskid, run_date_key)}/log",
    spark_use_gpu=False
)

inferencing_cluster.add_parallel_body_task(inferencing_spark_step)
xd_join_cluster.add_parallel_body_task(xd_join_spark_step)

final_dag_check_task = OpTask(task_id="final_dag_status", op=FinalDagStatusCheckOperator(dag=adag))

dag >> calculate_run_date_task
calculate_run_date_task >> should_run_dag_task
should_run_dag_task >> branching_task
branching_task >> inferencing_cluster
branching_task >> fetch_latest_iav2_graph_task
branching_task >> fetch_latest_country_task
inferencing_cluster >> xd_join_cluster
fetch_latest_iav2_graph_task >> xd_join_cluster
fetch_latest_country_task >> xd_join_cluster
xd_join_cluster >> final_dag_check_task
should_run_dag_task >> final_dag_check_task
