import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, PySparkEmrTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.graphics_optimized.g4 import G4
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
import logging

dag_name = "adpb-user-embedding-daily-inference"
owner = ADPB.team

# Job configuration
jar_path = "s3://ttd-build-artefacts/user-embedding/dataforgein/jars/release/dataforgein.jar"
job_start_date = datetime(2024, 11, 4)

job_environment = TtdEnvFactory.get_from_system()
job_schedule_interval_in_hours = 24
job_schedule_interval = "0 12 * * *"
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
training_days_of_week = [4]

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_tags = owner.sub_team
    enable_slack_alert = True
else:
    slack_tags = None
    enable_slack_alert = False

cluster_tags = {
    "Team": owner.jira_team,
}
cluster_idle_timeout_seconds = 30 * 60

# Execution date
run_date = "{{ ds }}"  # run date should always be yesterday
inf_run_date = "{{ ds_nodash }}"

# Compute
worker_cores = 48
num_workers = 100  # number of workers for user cluster(s)
num_partitions = 2 * worker_cores * num_workers
num_workers_ctx = 40  # number of workers for ctx cluster
num_partitions_ctx = 2 * worker_cores * num_workers_ctx
embedding_version_key = "latest_embedding_version"
check_embedding_version_task_id = "get-embedding_version_task_id"
latest_embedding_version = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='" + check_embedding_version_task_id + "', key='" + embedding_version_key + "') }}"

master_instance_types = [M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)]
worker_instance_types = [
    M5.m5_12xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M5a.m5a_12xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M6g.m6g_12xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
    M5.m5_24xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
    M5a.m5a_24xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
]

# Application settings
java_settings_list = [
    ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096"),
]

packages_tfrecord = [
    ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0"),
]

spark_options_list = [
    # ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
    # ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.driver.maxResultSize=32G"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
]

spark_options_list_user = copy.deepcopy(spark_options_list) + [
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % num_partitions),
]

spark_options_list_ctx = copy.deepcopy(spark_options_list) + [
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions_ctx),
    ("conf", "spark.default.parallelism=%s" % num_partitions_ctx),
]

eldorado_option_list = [("date", run_date)]

application_configuration = [
    {
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    },
    {
        "Classification": "emrfs-site",
        "Properties": {
            "fs.s3.maxConnections": "1000",
            "fs.s3.maxRetries": "50",
            "fs.s3.sleepTimeSeconds": "15",
        },
    },
]

# DAG
daily_inference_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    depends_on_past=False,
    slack_tags=slack_tags,
    tags=[owner.jira_team],
    enable_slack_alert=enable_slack_alert,
    retries=0,
)
airflow_dag = daily_inference_dag.airflow_dag


def xcom_template_to_get_value(taskid: str, key: str) -> str:
    global dag_name
    return (f"{{{{ "
            f'task_instance.xcom_pull(dag_id="{dag_name}", '
            f'task_ids="{taskid}", '
            f'key="{key}") '
            f"}}}}")


def check_is_not_training_day(run_date_str):
    run_date_weekday = datetime.strptime(run_date_str, "%Y-%m-%d").weekday()
    if run_date_weekday not in training_days_of_week:
        logging.info(f"Run date {run_date_str} is not training day, continue")
        return True
    else:
        logging.info(f"Run date {run_date_str} is training, skip")
        return False


# check if it is training day, no need to run daily inference on training day.
check_is_training_day_op_task = OpTask(
    op=ShortCircuitOperator(
        task_id="is_training_day",
        python_callable=check_is_not_training_day,
        op_kwargs={"run_date_str": "{{ ds }}"},
        dag=daily_inference_dag.airflow_dag,
        trigger_rule="none_failed",
    )
)


def get_latest_embedding_version(ds, **kwargs):
    given_date = datetime.strptime(ds, "%Y-%m-%d").date()
    offset = (given_date.weekday() + 3) % 7  # Calculate how many days since the last Friday
    last_friday = (given_date - timedelta(days=offset)).strftime("%Y%m%d")
    kwargs["task_instance"].xcom_push(key=embedding_version_key, value=last_friday)
    logging.info(f"Pushed to XCom: {last_friday} with key: {embedding_version_key}")


get_embeddings_version = PythonOperator(
    task_id=check_embedding_version_task_id,
    python_callable=get_latest_embedding_version,
    dag=daily_inference_dag.airflow_dag,
    templates_dict={"ds": "{{ ds }}"},
    provide_context=True,
)

get_embeddings_version_task = OpTask(op=get_embeddings_version)

daily_new_users_input_cluster = EmrClusterTask(
    name="daily-input-generation-cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=worker_instance_types,
        on_demand_weighted_capacity=num_workers,
    ),
    cluster_tags=cluster_tags,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    environment=job_environment,
)

daily_new_users = EmrJobTask(
    name="daily-user-aggregation",
    class_name="com.thetradedesk.dataforgein.jobs.DailyNewUsers",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_user,
    eldorado_config_option_pairs_list=eldorado_option_list + [
        ("cardinalityOutlierCappingThreshold", "350"),
        ("cardinalityDivide", "150"),
        ("skewFactor", "100"),
        ("embeddingsDate", latest_embedding_version),
    ],
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=daily_new_users_input_cluster.cluster_specs,
)

daily_new_users_input_cluster.add_sequential_body_task(daily_new_users)

user_feature_normalization = EmrJobTask(
    name="daily-user-feature-normalization",
    class_name="com.thetradedesk.dataforgein.jobs.UserFeatureNormalization",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list_user,
    eldorado_config_option_pairs_list=eldorado_option_list + [
        ("isNewUsersRun", "true"),
        ("ttd.ds.DailyNewUsersAggregationDataSet.isInChain", "true"),
    ],
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=daily_new_users_input_cluster.cluster_specs,
)

daily_new_users_input_cluster.add_sequential_body_task(user_feature_normalization)

generate_all_positive = EmrJobTask(
    name="daily-generate-all-positive",
    class_name="com.thetradedesk.dataforgein.jobs.AllPositiveGeneration",
    executable_path=jar_path,
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list_user) + packages_tfrecord,
    eldorado_config_option_pairs_list=eldorado_option_list + [
        ("isNewUsersRun", "true"),
        ("ttd.ds.DailyNewUserFeatureRawDataSet.isInChain", "true"),
    ],
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=daily_new_users_input_cluster.cluster_specs,
    configure_cluster_automatically=False,
)
daily_new_users_input_cluster.add_sequential_body_task(generate_all_positive)

# ###  Inference Cluster ####

env_path = "prod" if job_environment == TtdEnvFactory.prod else "test"
model_download_script_path = "s3://ttd-build-artefacts/user-embedding/galamere/scripts/release/download.sh"
base_prod_path = "s3://ttd-identity/datapipeline/prod/models/user-embedding"
base_env_path = f"s3://ttd-identity/datapipeline/{env_path}/models/user-embedding"

# Input
model_source_bucket = "ttd-identity"
model_source_prefix = "datapipeline/prod/models/user-embedding/twindom/model_save/"
model_input_path = f"{base_env_path}/dataforgein/public/daily_new_user_all_positive_label_with_feature/date="

# Output
spark_logs_base_path = f"{base_env_path}/galamere/logs/daily_inference"

inferencing_env_path = "env=test/" if job_environment != TtdEnvFactory.prod else ""
inferencing_base_path = f"s3://ttd-user-embeddings/{inferencing_env_path}dataexport/type=101/date="
inferencing_prefix = "/hour=00/batch="

# Batch numbers 11 to 16, 11 for Saturday and 16 for Thursday.
batch_number = "{{ 10 + ((data_interval_start.weekday() + 3) % 7)}}"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-adpb/userembedding_modelserving"
docker_image_tag = "release"

gpu_executor_cores = 4  # We are using g4dn_12xlarge nodes that have 4 GPU cores
inferencing_instances_count = 50

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[G4.g4dn_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[G4.g4dn_12xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=inferencing_instances_count
)

inference_cluster_options = [
    ("executor-cores", f"{gpu_executor_cores}"),
    ("conf", f"num-executors={inferencing_instances_count}"),
    ('conf', 'spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/home/hadoop:/home/hadoop:ro'),
    ('conf', 'spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/home/hadoop:/home/hadoop:ro'),
    ('conf', 'spark.executorEnv.NVIDIA_VISIBLE_DEVICES=all'),
    ('conf', 'spark.executor.resource.gpu.amount=1'),
    ('conf', 'spark.driver.resource.gpu.amount=1'),
    ('conf', 'spark.task.resource.gpu.amount=0.25'),
    ('conf', 'spark.sql.shuffle.partitions=3000'),
    ('conf', 'spark.hadoop.fs.s3a.buffer.dir=/tmp'),
    ('conf', 'spark.sql.execution.arrow.pyspark.enabled=true'),
    ('conf', 'spark.sql.execution.arrow.maxRecordsPerBatch=100000'),
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
        f"--input_path={model_input_path}{inf_run_date}",
        f"--inference_path={inferencing_base_path}{latest_embedding_version}{inferencing_prefix}{batch_number}", "--run_mode=inference",
        f"--environment={job_environment}", f"--run_date={inf_run_date}"
    ],
    timeout_timedelta=timedelta(hours=7)
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
    log_uri=f"{spark_logs_base_path}/{job_environment.dataset_write_env}/inf/{run_date}/log",
    spark_use_gpu=True,
    path_to_spark_gpu_discovery_script="/opt/application/scripts/getGpusResources.sh",
    bootstrap_script_actions=[
        ScriptBootstrapAction(
            f"{model_download_script_path}",
            [f"s3://{model_source_bucket}/{model_source_prefix}date=" + latest_embedding_version + "/user_model"]
        )
    ]
)

inferencing_cluster.add_parallel_body_task(inferencing_spark_step)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=daily_inference_dag.airflow_dag))

daily_inference_dag >> check_is_training_day_op_task >> get_embeddings_version_task >> daily_new_users_input_cluster
daily_new_users_input_cluster >> inferencing_cluster >> final_dag_check
