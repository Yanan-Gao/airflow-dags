import time
from datetime import timedelta, datetime
import copy
import re
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask, DockerSetupBootstrapScripts
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator

from ttd.operators.variable_file_check_sensor import VariableFileCheckSensor
from ttd.ops_api_hook import OpsApiHook
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.eldorado.aws.emr_pyspark import S3PysparkEmrTask
from dags.audauto.utils import utils
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
num_executors = 100
num_partitions = int(round(3 * num_executors)) * 30
spark_options_list = [("executor-memory", "204G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=100G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
                      ("conf", "spark.default.parallelism=%s" % num_partitions), ("conf", "spark.driver.maxResultSize=50G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25"), ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0")]

model_prediction_spark_options_list = [("executor-memory", "204G"), ("executor-cores", "16"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=100G"), ("conf", "spark.driver.cores=15"),
                                       ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
                                       ("conf", "spark.default.parallelism=%s" % num_partitions),
                                       ("conf", "spark.driver.maxResultSize=50G"), ("conf", "spark.dynamicAllocation.enabled=true"),
                                       ("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25"),
                                       ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0"),
                                       ("conf", "spark.yarn.maxAppAttempts=1")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    }
}]

# Docker config
docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
docker_image_name = "ttd-base/scrum-audauto/audience_models"
docker_image_tag = "ycp-AUDAUTO-3143-new-density-feature-training"  # change this
docker_image_prod_tag = "ycp-AUDAUTO-2952-add-model-prediction-avg-score"

test_exp_name = "RSMv2SensitiveDensityTest"
control_exp_name = "RSMv2SensitiveDensityControl"

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = "#dev-perf-auto-alerts-rsm"
    slack_tags = AUDAUTO.team.sub_team
else:
    slack_channel = "#scrum-perf-automation-alerts-testing"
    slack_tags = None

environment = TtdEnvFactory.get_from_system()
env = environment.execution_env
model = "RSM"
docker_install_script = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/install_docker_gpu_new.sh"
bootstrap_script_configuration = DockerSetupBootstrapScripts(install_docker_gpu_location=docker_install_script)
run_date = "{{ data_interval_start.to_date_string() }}"
short_date_str = '{{ data_interval_start.strftime("%Y%m%d") }}'
date_str = '{{ data_interval_start.strftime("%Y%m%d000000") }}'

AUDIENCE_JAR = ("s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar")

rsm_training_dag = TtdDag(
    dag_id="perf-automation-rsm-training-v2-new-density-ab-testing",
    start_date=datetime(2025, 5, 17),
    schedule_interval=timedelta(days=1),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO", "RSM", "RSMV2"],
)

adag = rsm_training_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
seed_none_rsmv2_test_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/test",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_ETL_SUCCESS",
)

seed_none_rsmv2_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_ETL_SUCCESS",
)

seed_none_rsmv2_test_oos_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/test",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_OOS_SUCCESS",
)

seed_none_rsmv2_oos_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_OOS_SUCCESS",
)

policy_commit_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="configdata/prod/audience",
    data_name="policyTable/RSM/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_COMMIT",
)

seed_none_calibration_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_CALIBRATION_SUCCESS"
)

seed_none_population_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_POPULATION_SUCCESS"
)

seed_none_test_calibration_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/test",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_CALIBRATION_SUCCESS"
)

seed_none_test_population_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/test",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_POPULATION_SUCCESS"
)


###############################################################################
# Sensors
###############################################################################
def init_commit_files(task_instance: TaskInstance, **kwargs):
    conn_id = kwargs.get('conn_id')
    hook = OpsApiHook(conn_id=conn_id)

    active_releases = hook.get_active_releases()
    commit_files = []
    for release_string in active_releases:
        commit_files.append("_COMMIT_" + release_string.replace(".", "-"))

    task_instance.xcom_push("files", commit_files)


init_commit_files = OpTask(
    op=PythonOperator(
        task_id="init_commit_files", python_callable=init_commit_files, provide_context=True, op_kwargs={'conn_id': 'ttdopsapi'}, dag=adag
    )
)

policy_commit_file_sensor = OpTask(
    op=VariableFileCheckSensor(
        task_id='policy_commit_file_sensor',
        poke_interval=60 * 10,
        timeout=60 * 60 * 5,
        bucket='thetradedesk-mlplatform-us-east-1',
        path=f'configdata/prod/audience/policyTable/RSM/v=1/{date_str}',
        data_task_ids=['init_commit_files']
    )
)

emb_prod_commit_file_sensor = OpTask(
    op=VariableFileCheckSensor(
        task_id='prod_emb_commit_file_sensor',
        poke_interval=60 * 10,
        timeout=60 * 60 * 5,
        bucket='thetradedesk-mlplatform-us-east-1',
        path=f'configdata/test/audience/embedding/RSMV2/{control_exp_name}/v=1/{date_str}',
        data_task_ids=['init_commit_files']
    )
)

emb_commit_file_sensor = OpTask(
    op=VariableFileCheckSensor(
        task_id='emb_commit_file_sensor',
        poke_interval=60 * 10,
        timeout=60 * 60 * 5,
        bucket='thetradedesk-mlplatform-us-east-1',
        path=f'configdata/test/audience/embedding/RSMV2/{test_exp_name}/v=1/{date_str}',
        data_task_ids=['init_commit_files']
    )
)

etl_file_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="etl_success_file_found",
        datasets=[
            seed_none_rsmv2_test_etl_success_file, seed_none_rsmv2_test_oos_etl_success_file, seed_none_rsmv2_etl_success_file,
            seed_none_rsmv2_oos_etl_success_file
        ],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 5,
        timeout=60 * 60 * 23,
    )
)

calibration_data_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='calibration_etl_success_file_available',
        datasets=[
            seed_none_test_calibration_etl_success_file, seed_none_calibration_etl_success_file, seed_none_population_etl_success_file,
            seed_none_test_population_etl_success_file
        ],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

###############################################################################
# cluster
###############################################################################
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[G5.g5_24xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[G5.g5_24xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

rsm_training_cluster_task = DockerEmrClusterTask(
    name="RelevanceScoreModelTraining",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    environment=environment,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=2 * 60 * 60,
    entrypoint_in_image="/opt/application/jobs/",
    builtin_bootstrap_script_configuration=bootstrap_script_configuration,
)

rsm_training_prod_cluster_task = DockerEmrClusterTask(
    name="RelevanceScoreModelProdTraining",
    image_name=docker_image_name,
    image_tag=docker_image_prod_tag,
    docker_registry=docker_registry,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    environment=environment,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=2 * 60 * 60,
    entrypoint_in_image="/opt/application/jobs/",
    builtin_bootstrap_script_configuration=bootstrap_script_configuration,
)

rsm_registry_cluster_task = DockerEmrClusterTask(
    name="RelevanceScoreModelRegistry",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G5.g5_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G5.g5_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": AUDAUTO.team.jira_team,
    },
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    environment=environment,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=2 * 60 * 60,
    entrypoint_in_image="/opt/application/jobs/",
    builtin_bootstrap_script_configuration=bootstrap_script_configuration,
)

###############################################################################
# steps
###############################################################################
policy_table_path = "./input/policy_table/"
model_output_path = "s3://thetradedesk-mlplatform-us-east-1/models/"
holdout_prediction_output_prefix = (f"s3://thetradedesk-mlplatform-us-east-1/data/{env}/audience/RSM/prediction")
holdout_prediction_output_path = f"{holdout_prediction_output_prefix}/v=1/{date_str}"
embedding_output_path = "s3://thetradedesk-mlplatform-us-east-1/configdata/"
embedding_type = "embedding_temp"

thresholds_path_s3 = "s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/thresholds/"
ial_path = f"s3://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date={short_date_str}"
policy_s3_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/{date_str}"
campaign_seed_mapping_path = f"s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignseed/v=1/date={short_date_str}"
incremental_training_enabled = "true"
feature_json = "configdata/test/audience/schema/RSMV2/v=1/20250319000000/feature.json"  # to remove that when in prod
full_train_start_date = 20240517
incremental_training_cadence = 100000

training_argument_list = [
    "--run_train",
    f"--date={date_str}",
    # "--model_type=RSM",
    f'--policy_table_path="{policy_table_path}"',
    # "--exp_name=/RSMv2TestTreatment",
    "--register_model=True",
    f'--model_output_path_s3="{model_output_path}"',
    f'--embedding_output_path_s3="{embedding_output_path}"',
    f'--thresholds_path_s3="{thresholds_path_s3}"',
    f"--incremental_training_enabled={incremental_training_enabled}",
    # f"--full_train_start_date={full_train_start_date}",
    f"--incremental_training_cadence={incremental_training_cadence}",
    f"--prediction_output_path_s3={holdout_prediction_output_prefix}",
    f"--embedding_type={embedding_type}"
]
# todo turn on registry when ab is ok
control_argument_list = ["--env=prodTest", f"--exp_name=/{control_exp_name}", "--model_type=RSMV2", "--full_train_start_date=20250517"]

test_argument_list = [
    "--env=test", f"--exp_name=/{test_exp_name}", f"--features_path_cloud={feature_json}", "--model_type=RSMV2",
    "--full_train_start_date=20250517"
]

training_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/main.py",
    additional_parameters=["--gpus all", "-e TF_GPU_THREAD_MODE=gpu_private"],
    additional_execution_parameters=training_argument_list + test_argument_list,
)

training_docker_prod_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_prod_tag,
    path_to_app="/opt/application/jobs/main.py",
    additional_parameters=["--gpus all", "-e TF_GPU_THREAD_MODE=gpu_private"],
    additional_execution_parameters=training_argument_list + control_argument_list,
)

model_training_step = DockerRunEmrTask(
    name="ModelTraining",
    docker_run_command=training_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)
model_training_prod_step = DockerRunEmrTask(
    name="ModelProdTraining",
    docker_run_command=training_docker_prod_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)

rsm_training_cluster_task.add_parallel_body_task(model_training_step)

rsm_training_prod_cluster_task.add_parallel_body_task(model_training_prod_step)

# model registry

registry_argument_list = [f"--test_names='RSM-{test_exp_name},RSM-{control_exp_name}'"]

Registry_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/model_register.py",
    additional_execution_parameters=registry_argument_list,
)

model_registry_step = DockerRunEmrTask(
    name="ModelTestRegistry",
    docker_run_command=Registry_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)

rsm_registry_cluster_task.add_parallel_body_task(model_registry_step)

###############################################################################
# Modify S3 file operators
###############################################################################

update_embedding_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_embedding_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/test/audience/embedding/RSMV2/{test_exp_name}/v=1/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_prod_embedding_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_prod_embedding_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/test/audience/embedding/RSMV2/{control_exp_name}/v=1/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/test/audience/embedding/RSMV2/{test_exp_name}/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_prod_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_prod_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/test/audience/embedding/RSMV2/{control_exp_name}/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_tmp_test_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_tmp_test_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/test/audience/embedding_temp/RSMV2/{test_exp_name}/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_tmp_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_tmp_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/test/audience/embedding_temp/RSMV2/{control_exp_name}/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_model_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_model_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/test/RSMV2/{test_exp_name}/bidrequest_model/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_prod_model_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_prod_model_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/test/RSMV2/{control_exp_name}/bidrequest_model/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/test/RSMV2/{test_exp_name}/bidrequest_model/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_prod_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_prod_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/test/RSMV2/{control_exp_name}/bidrequest_model/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)
# for incremental model training
update_full_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/test/RSMV2/{test_exp_name}/full_model/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_prod_full_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_prod_full_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/test/RSMV2/{control_exp_name}/full_model/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)


###############################################################################
# helpers
###############################################################################
def copy_s3_object(**kwargs):
    # Extract the parameters from the context
    source_bucket, source_key = extract_bucket_and_key(kwargs['source'])
    destination_bucket, destination_key = extract_bucket_and_key(kwargs['destination'])

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    try:
        # Copy the object
        s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key}, Bucket=destination_bucket, Key=destination_key)
        print(f"Copied {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error in credentials: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def copy_s3_dir(**kwargs):

    # Extract bucket and prefix from each using your helper function.
    source_bucket, source_prefix = extract_bucket_and_key(kwargs['source'])
    destination_bucket, destination_prefix = extract_bucket_and_key(kwargs['destination'])

    # Initialize the S3 client.
    s3_client = boto3.client('s3')

    # Use a paginator to list all objects under the source prefix.
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Compute the relative key by stripping the source prefix.
                relative_key = key[len(source_prefix):] if key.startswith(source_prefix) else key
                # Create the new destination key.
                new_key = destination_prefix + relative_key
                s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': key}, Bucket=destination_bucket, Key=new_key)
                print(f"Copied {source_bucket}/{key} to {destination_bucket}/{new_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error in credentials: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def extract_bucket_and_key(s3_path):
    match = re.match(r'^s3://([^/]+)/(.*)$', s3_path)
    if match:
        bucket_name = match.group(1)
        object_key = match.group(2)
        return bucket_name, object_key
    else:
        raise ValueError("Invalid S3 path format")


def extract_file_date_version(task_instance: TaskInstance, **kwargs):

    cutoff_date_str = kwargs["cutoff_date"]

    cutoff_date = datetime.strptime(cutoff_date_str, "%Y%m%d").date()
    prefix = kwargs["prefix"]
    source_bucket, source_key = extract_bucket_and_key(kwargs['source'])

    # Initialize boto3 S3 client and get the file contents.
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    content = response["Body"].read().decode("utf-8")

    # Split the file into lines and filter out empty ones.
    date_lines = [line.strip() for line in content.splitlines() if line.strip()]

    # Parse each line into a datetime using the known format.
    valid_dates = []
    for line in date_lines:
        try:
            file_dt = datetime.strptime(line, "%Y%m%d%H%M%S")
            if file_dt.date() < cutoff_date:
                valid_dates.append((file_dt, line))
        except Exception as e:
            # Skip any lines that do not parse.
            continue

    if not valid_dates:
        raise ValueError(f"No date in the file is smaller than cutoff_date {cutoff_date_str}")

    selected = max(valid_dates, key=lambda x: x[0])
    print("Selected date string:", selected[1])
    task_instance.xcom_push(key=f"{prefix}previous_version", value=selected[1])


embedding_version_cloud_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/embedding_temp/RSMV2/{control_exp_name}/v=1/_CURRENT"
update_previous_version = OpTask(
    op=PythonOperator(
        task_id="detect_previous_version",
        python_callable=extract_file_date_version,
        provide_context=True,
        op_kwargs={
            'cutoff_date': short_date_str,
            'source': embedding_version_cloud_path,
            'prefix': ""
        },
        dag=adag
    )
)

embedding_test_version_cloud_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/embedding_temp/RSMV2/{test_exp_name}/v=1/_CURRENT"
update_test_previous_version = OpTask(
    op=PythonOperator(
        task_id="detect_test_previous_version",
        python_callable=extract_file_date_version,
        provide_context=True,
        op_kwargs={
            'cutoff_date': short_date_str,
            'source': embedding_test_version_cloud_path,
            'prefix': 'test_'
        },
        dag=adag
    )
)
previous_version_key = 'previous_version'
previous_version = f'{{{{ task_instance.xcom_pull(key="{previous_version_key}") }}}}'

test_previous_version_key = 'test_previous_version'
test_previous_version = f'{{{{ task_instance.xcom_pull(key="{test_previous_version_key}") }}}}'

###############################################################################
# prediction cluster setup
###############################################################################
bootstrap_script_actions = [
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/node_exporter_bootstrap.sh", []
    ),
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/graphite_exporter_bootstrap.sh",
        ["s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts"]
    ),
    ScriptBootstrapAction("s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/bootstrap/import-ttd-certs.sh", []),
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/bootstrap_stat_collector.sh",
        ["s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/spark_stat_collect.py"]
    ),
    ScriptBootstrapAction("s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/tensorflow_init.sh", []),
]

population_emr_cluster = utils.create_emr_cluster(
    name="population-prod-prediction-cluster", capacity=64, bootstrap_script_actions=bootstrap_script_actions
)
previous_calibration_emr_cluster = utils.create_emr_cluster(
    name="previous-prod-calibration-prediction-cluster", capacity=64, bootstrap_script_actions=bootstrap_script_actions
)
current_calibration_emr_cluster = utils.create_emr_cluster(
    name="current-prod-calibration-prediction-cluster", capacity=64, bootstrap_script_actions=bootstrap_script_actions
)

population_test_emr_cluster = utils.create_emr_cluster(
    name="population-test-prediction-cluster", capacity=64, bootstrap_script_actions=bootstrap_script_actions
)
previous_test_calibration_emr_cluster = utils.create_emr_cluster(
    name="previous-test-calibration-prediction-cluster", capacity=64, bootstrap_script_actions=bootstrap_script_actions
)
current_test_calibration_emr_cluster = utils.create_emr_cluster(
    name="current-test-calibration-prediction-cluster", capacity=64, bootstrap_script_actions=bootstrap_script_actions
)

###############################################################################
# prediction steps
###############################################################################
# for control
previous_feature_path_origin = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/schema/RSMV2/v=1/{previous_version}/features.json"
previous_feature_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/schema/RSMV2/v=1/{previous_version}/features_json"
current_feature_path_origin = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/schema/RSMV2/v=1/{date_str}/features.json"
current_feature_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/schema/RSMV2/v=1/{date_str}/features_json"
calibration_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/RSMV2/Seed_None/v=1/{date_str}/mixedForward=Calibration/"
population_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/RSMV2/Seed_None/v=1/{date_str}/split=Population/"
previous_prod_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/test/RSMV2/{control_exp_name}/full_two_output_model/{previous_version}"
current_prod_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/test/RSMV2/{control_exp_name}/full_two_output_model/{date_str}"
previous_calibration_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/{control_exp_name}/prediction/calibration_data/v=1/model_version={previous_version}/{date_str}"
current_calibration_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/{control_exp_name}/prediction/calibration_data/v=1/model_version={date_str}/{date_str}"
population_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/{control_exp_name}/prediction/population_data/v=1/model_version={date_str}/{date_str}"

copy_previous_feature_json = OpTask(
    op=PythonOperator(
        task_id="copy_previous_feature_json",
        provide_context=True,
        python_callable=copy_s3_object,
        op_kwargs={
            'source': previous_feature_path_origin,
            'destination': previous_feature_path,
        },
        dag=adag
    )
)

copy_current_feature_json = OpTask(
    op=PythonOperator(
        task_id="copy_current_feature_json",
        provide_context=True,
        python_callable=copy_s3_object,
        op_kwargs={
            'source': current_feature_path_origin,
            'destination': current_feature_path,
        },
        dag=adag
    )
)

previous_calibration_job_arguments = [
    f"--feature_path={previous_feature_path}",
    f"--data_path={calibration_data_path}",
    f"--model_path={previous_prod_model_path}",
    f"--out_path={previous_calibration_output_path}",
]
current_calibration_job_arguments = [
    f"--feature_path={current_feature_path}", f"--data_path={calibration_data_path}", f"--model_path={current_prod_model_path}",
    f"--out_path={current_calibration_output_path}"
]
population_job_arguments = [
    f"--feature_path={current_feature_path}", f"--data_path={population_data_path}", f"--model_path={current_prod_model_path}",
    f"--out_path={population_output_path}"
]

current_calibration_prediction_job = S3PysparkEmrTask(
    name="CurrentCalibrationPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/full_two_output_model_prediction_spark.py",
    additional_args_option_pairs_list=model_prediction_spark_options_list,
    cluster_specs=current_calibration_emr_cluster.cluster_specs,
    command_line_arguments=current_calibration_job_arguments,
)

previous_calibration_prediction_job = S3PysparkEmrTask(
    name="PreviousCalibrationPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/full_two_output_model_prediction_spark.py",
    additional_args_option_pairs_list=model_prediction_spark_options_list,
    cluster_specs=previous_calibration_emr_cluster.cluster_specs,
    command_line_arguments=previous_calibration_job_arguments,
)

current_calibration_emr_cluster.add_parallel_body_task(current_calibration_prediction_job)
previous_calibration_emr_cluster.add_parallel_body_task(previous_calibration_prediction_job)

population_prediction_job = S3PysparkEmrTask(
    name="PopulationPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/full_two_output_model_prediction_spark.py",
    additional_args_option_pairs_list=model_prediction_spark_options_list,
    cluster_specs=population_emr_cluster.cluster_specs,
    command_line_arguments=population_job_arguments,
)

population_emr_cluster.add_parallel_body_task(population_prediction_job)

# for test
"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/schema/RSMV2/v=1/20250322000000/feature.json"
previous_test_feature_path_origin = f"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/schema/RSMV2/v=1/{test_previous_version}/features.json"
previous_test_feature_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/schema/RSMV2/v=1/{test_previous_version}/features_json"
current_test_feature_path_origin = f"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/schema/RSMV2/v=1/{date_str}/features.json"
current_test_feature_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/test/audience/schema/RSMV2/v=1/{date_str}/features_json"
calibration_test_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/Seed_None/v=1/{date_str}/mixedForward=Calibration/"
population_test_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/Seed_None/v=1/{date_str}/split=Population/"
previous_test_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/test/RSMV2/{test_exp_name}/full_two_output_model/{test_previous_version}"
current_test_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/test/RSMV2/{test_exp_name}/full_two_output_model/{date_str}"
previous_test_calibration_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/{test_exp_name}/prediction/calibration_data/v=1/model_version={test_previous_version}/{date_str}"
current_test_calibration_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/{test_exp_name}/prediction/calibration_data/v=1/model_version={date_str}/{date_str}"
population_test_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/{test_exp_name}/prediction/population_data/v=1/model_version={date_str}/{date_str}"

copy_previous_test_feature_json = OpTask(
    op=PythonOperator(
        task_id="copy_previous_test_feature_json",
        provide_context=True,
        python_callable=copy_s3_object,
        op_kwargs={
            'source': previous_test_feature_path_origin,
            'destination': previous_test_feature_path,
        },
        dag=adag
    )
)

copy_current_test_feature_json = OpTask(
    op=PythonOperator(
        task_id="copy_current_test_feature_json",
        provide_context=True,
        python_callable=copy_s3_object,
        op_kwargs={
            'source': current_test_feature_path_origin,
            'destination': current_test_feature_path,
        },
        dag=adag
    )
)

previous_test_calibration_job_arguments = [
    f"--feature_path={previous_test_feature_path}", f"--data_path={calibration_test_data_path}", f"--model_path={previous_test_model_path}",
    f"--out_path={previous_test_calibration_output_path}"
]
current_test_calibration_job_arguments = [
    f"--feature_path={current_test_feature_path}", f"--data_path={calibration_test_data_path}", f"--model_path={current_test_model_path}",
    f"--out_path={current_test_calibration_output_path}"
]
population_test_job_arguments = [
    f"--feature_path={current_test_feature_path}", f"--data_path={population_test_data_path}", f"--model_path={current_test_model_path}",
    f"--out_path={population_test_output_path}"
]

current_test_calibration_prediction_job = S3PysparkEmrTask(
    name="TestCurrentCalibrationPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/full_two_output_model_prediction_spark.py",
    additional_args_option_pairs_list=model_prediction_spark_options_list,
    cluster_specs=current_test_calibration_emr_cluster.cluster_specs,
    command_line_arguments=current_test_calibration_job_arguments,
)

previous_test_calibration_prediction_job = S3PysparkEmrTask(
    name="TestPreviousCalibrationPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/full_two_output_model_prediction_spark.py",
    additional_args_option_pairs_list=model_prediction_spark_options_list,
    cluster_specs=previous_test_calibration_emr_cluster.cluster_specs,
    command_line_arguments=previous_test_calibration_job_arguments,
)

current_test_calibration_emr_cluster.add_parallel_body_task(current_test_calibration_prediction_job)
previous_test_calibration_emr_cluster.add_parallel_body_task(previous_test_calibration_prediction_job)

population_test_prediction_job = S3PysparkEmrTask(
    name="TestPopulationPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/full_two_output_model_prediction_spark.py",
    additional_args_option_pairs_list=model_prediction_spark_options_list,
    cluster_specs=population_test_emr_cluster.cluster_specs,
    command_line_arguments=population_test_job_arguments,
)

population_test_emr_cluster.add_parallel_body_task(population_test_prediction_job)

###############################################################################
# emb merge steps
###############################################################################
audience_embedding_merge_cluster_task = EmrClusterTask(
    name="AudienceEmbeddingMergeCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(512).with_ebs_iops(10000).with_ebs_throughput(400).with_max_ondemand_price()
            .with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=1920
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300
)
# todo config path args
audience_audience_embedding_merge_step = EmrJobTask(
    name="embeddingMergeJob",
    class_name="com.thetradedesk.audience.jobs.AudienceCalibrationAndMergeJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[('date', run_date), ('anchorStartDate', '2025-03-24'), ('syntheticIdLength', '2000'),
                                       ('ttd.env', 'test'),
                                       ('tmpEmbeddingDataS3Path', f'configdata/test/audience/embedding_temp/RSMV2/{control_exp_name}/v=1'),
                                       ('embeddingDataS3Path', f'configdata/test/audience/embedding/RSMV2/{control_exp_name}/v=1'),
                                       ('inferenceDataS3Path', f'data/test/audience/RSMV2/{control_exp_name}/prediction/'),
                                       ('AudienceModelPolicyReadableDatasetReadEnv', 'prod')],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=4)
)
# todo config path args
audience_test_embedding_merge_cluster_task = EmrClusterTask(
    name="AudienceTestEmbeddingMergeCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': AUDAUTO.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(512).with_ebs_iops(10000).with_ebs_throughput(400).with_max_ondemand_price()
            .with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=1920
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300
)

audience_test_audience_embedding_merge_step = EmrJobTask(
    name="testEmbeddingMergeJob",
    class_name="com.thetradedesk.audience.jobs.AudienceCalibrationAndMergeJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[('date', run_date), ('anchorStartDate', '2025-03-24'), ('syntheticIdLength', '2000'),
                                       ('ttd.env', 'test'),
                                       ('tmpEmbeddingDataS3Path', f'configdata/test/audience/embedding_temp/RSMV2/{test_exp_name}/v=1'),
                                       ('embeddingDataS3Path', f'configdata/test/audience/embedding/RSMV2/{test_exp_name}/v=1'),
                                       ('inferenceDataS3Path', f'data/test/audience/RSMV2/{test_exp_name}/prediction/'),
                                       ('AudienceModelPolicyReadableDatasetReadEnv', 'prod')],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=4)
)

audience_test_embedding_merge_cluster_task.add_parallel_body_task(audience_test_audience_embedding_merge_step)
audience_embedding_merge_cluster_task.add_parallel_body_task(audience_audience_embedding_merge_step)

delay_task = OpTask(op=PythonOperator(task_id="delay_task", python_callable=lambda: time.sleep(1800), dag=adag))
delay_task_prod = OpTask(op=PythonOperator(task_id="delay_task_prod", python_callable=lambda: time.sleep(1800), dag=adag))

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
rsm_training_dag >> etl_file_sensor >> rsm_training_cluster_task
rsm_training_dag >> etl_file_sensor >> rsm_training_prod_cluster_task

rsm_training_cluster_task >> calibration_data_sensor >> update_test_previous_version
update_test_previous_version >> copy_previous_test_feature_json >> previous_test_calibration_emr_cluster >> audience_test_embedding_merge_cluster_task
update_test_previous_version >> copy_current_test_feature_json >> current_test_calibration_emr_cluster >> audience_test_embedding_merge_cluster_task
update_test_previous_version >> population_test_emr_cluster >> audience_test_embedding_merge_cluster_task

(
    audience_test_embedding_merge_cluster_task >> update_embedding_success_file_task >> update_embedding_current_file_task >>
    update_tmp_test_embedding_current_file_task >> init_commit_files >> policy_commit_file_sensor >> rsm_registry_cluster_task >>
    emb_commit_file_sensor >> delay_task >> update_model_success_file_task >> update_model_current_file_task >>
    update_full_model_current_file_task >> final_dag_status_step
)

rsm_training_prod_cluster_task >> calibration_data_sensor >> update_previous_version
update_previous_version >> copy_previous_feature_json >> previous_calibration_emr_cluster >> audience_embedding_merge_cluster_task
update_previous_version >> copy_current_feature_json >> current_calibration_emr_cluster >> audience_embedding_merge_cluster_task
update_previous_version >> population_emr_cluster >> audience_embedding_merge_cluster_task

(
    audience_embedding_merge_cluster_task >> update_prod_embedding_success_file_task >> update_prod_embedding_current_file_task >>
    update_tmp_embedding_current_file_task >> init_commit_files >> policy_commit_file_sensor >> rsm_registry_cluster_task >>
    emb_prod_commit_file_sensor >> delay_task_prod >> update_prod_model_success_file_task >> update_prod_model_current_file_task >>
    update_prod_full_model_current_file_task >> final_dag_status_step
)
