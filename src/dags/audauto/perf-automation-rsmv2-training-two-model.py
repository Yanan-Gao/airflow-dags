import copy
import time
from datetime import timedelta, datetime

from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from dags.audauto.utils.mlflow import rsm_mlflow_operator
from dags.audauto.utils.mlflow.rsm_config import RSMConfig

from dags.audauto.utils import utils
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask, DockerSetupBootstrapScripts
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.variable_file_check_sensor import VariableFileCheckSensor
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.ops_api_hook import OpsApiHook
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
num_executors = 100
num_partitions = int(round(3 * num_executors)) * 30
spark_options_list = [("executor-memory", "204G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=100G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
                      ("conf", "spark.default.parallelism=%s" % num_partitions), ("conf", "spark.driver.maxResultSize=50G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25"), ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0"),
                      ("conf", "spark.yarn.maxAppAttempts=1")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
    }
}]

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3_2
# Docker config
docker_registry = "production.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-audauto/audience_models"
docker_image_tag = "latest"
test_exp_name = ""
sensitive = "sensitive"
non_sensitive = "nonsensitive"

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
override_env = "test" if env == "prodTest" else env

AUDIENCE_JAR = ("s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar")

rsm_training_dag = TtdDag(
    dag_id="perf-automation-rsmv2-training-two-model",
    start_date=datetime(2025, 5, 20),
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
seed_none_rsmv2_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_ETL_SUCCESS",
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

etl_file_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="etl_success_file_found",
        datasets=[
            seed_none_rsmv2_etl_success_file,
            seed_none_rsmv2_oos_etl_success_file,
        ],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 5,
        timeout=60 * 60 * 23,
    )
)

calibration_data_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='calibration_etl_success_file_available',
        datasets=[seed_none_calibration_etl_success_file, seed_none_population_etl_success_file],
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

rsm_training_non_sensitive_cluster_task = DockerEmrClusterTask(
    name="RelevanceScoreModelNonSensitiveTraining",
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

rsm_training_sensitive_cluster_task = DockerEmrClusterTask(
    name="RelevanceScoreModelSensitiveTraining",
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

model_merge_task = DockerEmrClusterTask(
    name="RelevanceScoreModelMerge",
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
holdout_prediction_output_prefix = (f"s3://thetradedesk-mlplatform-us-east-1/data/{override_env}/audience/RSM/prediction/")
holdout_prediction_output_path = f"{holdout_prediction_output_prefix}/v=1/{date_str}"
embedding_output_path = "s3://thetradedesk-mlplatform-us-east-1/configdata/"
embedding_type = "embedding_temp"

thresholds_path_s3 = "s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/thresholds/"
ial_path = f"s3://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date={short_date_str}"
policy_s3_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/{date_str}"
policy_key = f"configdata/prod/audience/policyTable/RSM/v=1/{date_str}/"
campaign_seed_mapping_path = f"s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignseed/v=1/date={short_date_str}"
incremental_training_enabled = "true"
full_train_start_date = 20250520

incremental_training_cadence = 100000

training_argument_list = [
    "--run_train",
    f"--date={date_str}",
    f'--policy_table_path="{policy_table_path}"',
    f'--model_output_path_s3="{model_output_path}"',
    f'--embedding_output_path_s3="{embedding_output_path}"',
    f'--thresholds_path_s3="{thresholds_path_s3}"',
    f"--incremental_training_enabled={incremental_training_enabled}",
    f"--full_train_start_date={full_train_start_date}",
    f"--incremental_training_cadence={incremental_training_cadence}",
    f"--prediction_output_path_s3={holdout_prediction_output_prefix}",
    f"--embedding_type={embedding_type}",
    "--model_type=RSMV2",
]

sensitive_argument_list = [f"--env={env}", "--model_type=RSMV2", "--model_output_type=full_model_sensitive"]
non_sensitive_argument_list = [
    f"--env={env}", "--model_type=RSMV2", "--sensitive=nonsensitive", "--model_output_type=full_model_nonsensitive"
]

sensitive_training_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/main.py",
    additional_parameters=["--gpus all", "-e TF_GPU_THREAD_MODE=gpu_private"],
    additional_execution_parameters=training_argument_list + sensitive_argument_list,
)

nonsensitive_training_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/main.py",
    additional_parameters=["--gpus all", "-e TF_GPU_THREAD_MODE=gpu_private"],
    additional_execution_parameters=training_argument_list + non_sensitive_argument_list,
)

sensitive_model_training_step = DockerRunEmrTask(
    name="SensitiveModelTraining",
    docker_run_command=sensitive_training_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)
nonsensitive_model_training_step = DockerRunEmrTask(
    name="NonSensitiveModelTraining",
    docker_run_command=nonsensitive_training_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)

rsm_training_sensitive_cluster_task.add_parallel_body_task(sensitive_model_training_step)
rsm_training_non_sensitive_cluster_task.add_parallel_body_task(nonsensitive_model_training_step)

# model merge

merge_argument_list = [
    f"--date={date_str}",
    "--model_type=RSMV2",
    f'--model_output_path_s3="{model_output_path}"',
    "--register_model=False",
    f"--env={override_env}",
    f'--embedding_output_path_s3="{embedding_output_path}"',
]

Registry_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    path_to_app="/opt/application/jobs/model_merge.py",
    additional_execution_parameters=merge_argument_list,
)

model_merge_step = DockerRunEmrTask(
    name="ModelTestMerge",
    docker_run_command=Registry_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=12),
)

model_merge_task.add_parallel_body_task(model_merge_step)

# model registry
path = test_exp_name.lstrip("/")
registry_argument_list = [f"--test_names='RSM-{path}'"]

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

###############################################################################
# Modify S3 file operators
###############################################################################

update_embedding_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_embedding_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{override_env}/audience/embedding/RSMV2/v=1/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_policy_table_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_policy_table_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{override_env}/audience/policyTable/RSM/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{override_env}/audience/embedding/RSMV2/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_sensitive_tmp_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_sensitive_tmp_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{override_env}/audience/embedding_temp/RSMV2/{sensitive}/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_non_sensitive_tmp_embedding_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_non_sensitive_tmp_embedding_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"configdata/{override_env}/audience/embedding_temp/RSMV2/{non_sensitive}/v=1/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_br_model_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_br_model_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/bidrequest_model/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_full_model_success_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_success_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/full_two_output_model/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_br_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_br_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/bidrequest_model/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

# for incremental model training
update_full_model_sensitive_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_sensitive_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/full_model_sensitive/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_full_model_non_sensitive_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_nonsensitive_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/full_model_nonsensitive/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)
update_br_model_sensitive_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_br_model_sensitive_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/bidrequest_model_sensitive/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)
update_br_model_nonsensitive_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_br_model_nonsensitive_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/bidrequest_model_nonsensitive/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

update_full_model_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_full_model_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"models/{override_env}/RSMV2/full_two_output_model/_CURRENT",
        date=date_str,
        append_file=True,
        dag=adag,
    )
)

###############################################################################
# helpers
###############################################################################

embedding_version_cloud_path = "s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/embedding/RSMV2/v=1/_CURRENT"
detect_previous_version = OpTask(
    op=PythonOperator(
        task_id="detect_previous_version",
        python_callable=utils.extract_file_date_version,
        provide_context=True,
        op_kwargs={
            'cutoff_date': short_date_str,
            'source': embedding_version_cloud_path,
            # 'prefix': 'test_'
        },
        dag=adag
    )
)

# previous_version_key = 'previous_version'
# previous_version = f'{{{{ task_instance.xcom_pull(key="{previous_version_key}") }}}}'

###############################################################################
# prediction cluster setup
###############################################################################

# previous_calibration_prediction_path = f"audience/RSMV2/prediction/calibration_data/v=1/model_version={previous_version}"

# previous_calibration_prediction_success_file = DateGeneratedDataset(
#     bucket="thetradedesk-mlplatform-us-east-1",
#     path_prefix="data/prod",
#     data_name=previous_calibration_prediction_path,
#     date_format="%Y%m%d000000",
#     version=None,
#     env_aware=False,
#     success_file="_SUCCESS"
# )
# current_calibration_prediction_success_file = DateGeneratedDataset(
#     bucket="thetradedesk-mlplatform-us-east-1",
#     path_prefix="data/prod",
#     data_name=f"audience/RSMV2/prediction/calibration_data/v=1/model_version={date_str}",
#     date_format="%Y%m%d000000",
#     version=None,
#     env_aware=False,
#     success_file="_SUCCESS"
# )

# population_prediction_success_file = DateGeneratedDataset(
#     bucket="thetradedesk-mlplatform-us-east-1",
#     path_prefix="data/prod",
#     data_name=f"audience/RSMV2/prediction/population_data/v=1/model_version={date_str}",
#     date_format="%Y%m%d000000",
#     version=None,
#     env_aware=False,
#     success_file="_SUCCESS"
# )

# tmp solution detect success file from parent data folder
previous_calibration_prediction_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_PREVIOUS_CALIBRATION_INFERENCE_SUCCESS",
)

current_calibration_prediction_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_CURRENT_CALIBRATION_INFERENCE_SUCCESS",
)

population_prediction_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod",
    data_name="audience/RSMV2/Seed_None/v=1",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,
    success_file="_POPULATION_INFERENCE_SUCCESS",
)

prediction_data_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='predictions_success_file_available',
        datasets=[
            current_calibration_prediction_success_file, population_prediction_success_file, previous_calibration_prediction_success_file
        ],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

###############################################################################
# emb merge steps
###############################################################################
# todo config path args
audience_embedding_merge_cluster_task = EmrClusterTask(
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
    emr_release_label=emr_release_label,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300
)

audience_embedding_merge_step = EmrJobTask(
    name="testEmbeddingMergeJob",
    class_name="com.thetradedesk.audience.jobs.AudienceCalibrationAndMergeJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        ("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.3.4"),
    ],
    eldorado_config_option_pairs_list=[
        ('date', run_date), ('anchorStartDate', '2025-04-14'), ('syntheticIdLength', '2000'), ('ttd.env', f'{override_env}'),
        ('tmpSenEmbeddingDataS3Path', f'configdata/{override_env}/audience/embedding_temp/RSMV2/{sensitive}/v=1'),
        ('tmpNonSenEmbeddingDataS3Path', f'configdata/{override_env}/audience/embedding_temp/RSMV2/{non_sensitive}/v=1'),
        ('embeddingDataS3Path', f'configdata/{override_env}/audience/embedding/RSMV2/v=1'),
        ('inferenceDataS3Path', f'data/{override_env}/audience/RSMV2/prediction/'), ('AudienceModelPolicyReadableDatasetReadEnv', 'prod')
    ],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=4)
)

audience_embedding_merge_cluster_task.add_parallel_body_task(audience_embedding_merge_step)
# audience_embedding_merge_cluster_task.add_parallel_body_task(audience_audience_embedding_merge_step)

delay_task = OpTask(op=PythonOperator(task_id="delay_task", python_callable=lambda: time.sleep(1800), dag=adag))

# promote_model_task = rsm_mlflow_operator.get_register_new_prod_version_operator(
#     tags={
#         RSMConfig.POLICY_TABLE_PATH_TAG_NAME: policy_key,
#         RSMConfig.SEED_EMBEDDING_PATH_TAG_NAME: v2_embedding_key,
#         RSMConfig.BID_REQUEST_MODEL_PATH_TAG_NAME: v2_br_model_path
#     },
# )
v2_embedding_key = f"configdata/{override_env}/audience/embedding/RSMV2/v=1/{date_str}/"
v2_br_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/{override_env}/RSMV2/bidrequest_model/{date_str}/"

if override_env == "prod":
    # promote RSM prod model version
    promote_model_task = rsm_mlflow_operator.get_register_new_prod_version_operator(
        tags={
            RSMConfig.POLICY_TABLE_PATH_TAG_NAME: policy_key,
            RSMConfig.SEED_EMBEDDING_PATH_TAG_NAME: v2_embedding_key,
            RSMConfig.BID_REQUEST_MODEL_PATH_TAG_NAME: v2_br_model_path
        },
    )
else:
    # todo add exp model promotion
    promote_model_task = OpTask(op=EmptyOperator(task_id="noop_promote_prod_model"))

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
rsm_training_dag >> etl_file_sensor >> rsm_training_non_sensitive_cluster_task
rsm_training_dag >> etl_file_sensor >> rsm_training_sensitive_cluster_task

[
    rsm_training_non_sensitive_cluster_task, rsm_training_sensitive_cluster_task
] >> update_full_model_non_sensitive_current_file_task >> update_full_model_sensitive_current_file_task >> update_full_model_current_file_task >> update_br_model_sensitive_current_file_task >> update_br_model_nonsensitive_current_file_task >> model_merge_task >> update_full_model_success_file_task >> calibration_data_sensor

calibration_data_sensor >> detect_previous_version >> prediction_data_sensor >> audience_embedding_merge_cluster_task

(
    audience_embedding_merge_cluster_task >> update_policy_table_current_file_task >> update_embedding_success_file_task >>
    update_embedding_current_file_task >> update_sensitive_tmp_embedding_current_file_task >>
    update_non_sensitive_tmp_embedding_current_file_task >> init_commit_files >> policy_commit_file_sensor >> promote_model_task >>
    delay_task >> update_br_model_success_file_task >> update_br_model_current_file_task >> final_dag_status_step
)
