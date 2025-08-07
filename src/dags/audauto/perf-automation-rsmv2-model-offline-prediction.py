import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.docker import (
    DockerEmrClusterTask,
    DockerSetupBootstrapScripts,
)
from ttd.openlineage import OpenlineageConfig
from ttd.docker import PySparkEmrTask
from airflow.operators.python import PythonOperator
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from dags.audauto.utils import utils

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

# generic spark settings list we'll add to each step.
spark_options_list = [
    ("executor-memory", "200G"),
    ("executor-cores", "32"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=128G"),
    ("conf", "spark.driver.cores=20"),
    ("conf", "spark.sql.shuffle.partitions=4096"),
    ("conf", "spark.default.parallelism=4096"),
    ("conf", "spark.driver.maxResultSize=80G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.memory.storageFraction=0.25"),
    ("packages", "io.openlineage:openlineage-spark_2.13:1.34.0,com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0"),
]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

run_date = "{{ data_interval_start.to_date_string() }}"
date_str = "{{ data_interval_start.strftime(\"%Y%m%d000000\") }}"
short_date_str = '{{ data_interval_start.strftime("%Y%m%d") }}'
date_str_prev = "{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y%m%d000000') }}"
environment = TtdEnvFactory.get_from_system()
env = environment.execution_env
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"
policy_table_read_env = "prod"
model_read_env = "prod"

docker_registry = "docker.pkgs.adsrvr.org"
docker_image_name = "ttd-base-prod/ttd-base/scrum-audauto/audience_models"
docker_image_tag = "latest"
docker_install_script = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/audience-models/prod/latest/install_docker_gpu_new_with_ray.sh"
bootstrap_script_configuration = DockerSetupBootstrapScripts(install_docker_gpu_location=docker_install_script)

model_offline_prediction_etl_dag = TtdDag(
    dag_id="perf-automation-rsmv2-model-offline-prediction-etl",
    start_date=datetime(2025, 7, 2, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#dev-perf-auto-alerts-rsm",
    run_only_latest=False,
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "RSMV2"]
)

dag = model_offline_prediction_etl_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################
override_env = "test" if env == "prodTest" else env
seed_none_oos_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"data/{policy_table_read_env}",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_OOS_SUCCESS"
)

# br_model_success_file = DateGeneratedDataset(
#     bucket="thetradedesk-mlplatform-us-east-1",
#     path_prefix="models/prod",
#     data_name="RSMV2/bidrequest_model/",
#     date_format="%Y%m%d000000",
#     version=None,
#     env_aware=False,  # always read from prod
#     success_file="_SUCCESS"
# )

full_model_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"models/{model_read_env}",
    data_name="RSMV2/full_two_output_model",
    date_format="%Y%m%d000000",
    version=None,
    env_aware=False,  # always read from prod
    success_file="_SUCCESS"
)

seed_none_calibration_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"data/{policy_table_read_env}",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_CALIBRATION_SUCCESS"
)

seed_none_population_etl_success_file = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"data/{policy_table_read_env}",
    data_name="audience/RSMV2/Seed_None",
    date_format="%Y%m%d000000",
    version=1,
    env_aware=False,
    success_file="_POPULATION_SUCCESS"
)

###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[
            seed_none_oos_etl_success_file, seed_none_calibration_etl_success_file, seed_none_population_etl_success_file,
            full_model_success_file
        ],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 18,
    )
)

# prev_model_sensor = OpTask(
#     op=DatasetCheckSensor(
#         task_id='previous_data_available',
#         datasets=[br_model_success_file],
#         ds_date='{{data_interval_start.subtract(days=1).to_datetime_string()}}',
#         poke_interval=60 * 10,
#         timeout=60 * 60 * 12,
#     )
# )

###############################################################################
# helper functions
###############################################################################


def setup_prediction_cluster(common_cluster_kwargs, name, **overrides):
    kwargs = common_cluster_kwargs.copy()
    kwargs.update({
        "name": name,
    })
    kwargs.update(overrides)
    return DockerEmrClusterTask(**kwargs)


def setup_prediction_step(common_pyspark_kwargs, name, command_line_arguments, **overrides):

    kwargs = common_pyspark_kwargs.copy()
    kwargs.update({
        "name": name,
        "command_line_arguments": command_line_arguments,
    })
    kwargs.update(overrides)

    return PySparkEmrTask(**kwargs)


###############################################################################
# clusters
###############################################################################
audience_model_offline_metrics_etl_cluster_task = EmrClusterTask(
    name="AudienceModelOfflineMetricsGenerationCluster",
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
        on_demand_weighted_capacity=3840
    ),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=300
)

common_cluster_kwargs = {
    "image_name":
    docker_image_name,
    "image_tag":
    docker_image_tag,
    "docker_registry":
    docker_registry,
    "cluster_tags": {
        "Team": AUDAUTO.team.jira_team,
    },
    "emr_release_label":
    AwsEmrVersions.AWS_EMR_SPARK_3_5,
    "environment":
    environment,
    "additional_application_configurations":
    copy.deepcopy(application_configuration),
    "enable_prometheus_monitoring":
    False,
    "cluster_auto_termination_idle_timeout_seconds":
    2 * 60 * 60,
    "entrypoint_in_image":
    "/opt/application/jobs/",
    "builtin_bootstrap_script_configuration":
    bootstrap_script_configuration,
    "master_fleet_instance_type_configs":
    EmrFleetInstanceTypes(
        instance_types=[R5d.r5d_16xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    "core_fleet_instance_type_configs":
    EmrFleetInstanceTypes(
        instance_types=[R5d.r5d_16xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=50,
    ),
}

rsmv2_oos_prediction_cluster_task = setup_prediction_cluster(common_cluster_kwargs, "RelevanceScoreOOSPrediction")
rsmv2_previous_calibration_prediction_cluster_task = setup_prediction_cluster(
    common_cluster_kwargs, "RelevanceScorePreviousCalibrationPrediction"
)
rsmv2_current_calibration_prediction_cluster_task = setup_prediction_cluster(
    common_cluster_kwargs, "RelevanceScoreCurrentCalibrationPrediciton"
)
rsmv2_population_prediction_cluster_task = setup_prediction_cluster(common_cluster_kwargs, "RelevanceScorePopulationPrediction")

###############################################################################
# steps
###############################################################################

full_model_version_cloud_path = f"s3://thetradedesk-mlplatform-us-east-1/models/{model_read_env}/RSMV2/full_two_output_model/_CURRENT"
detect_previous_version = OpTask(
    op=PythonOperator(
        task_id="detect_previous_version",
        python_callable=utils.extract_file_date_version,
        provide_context=True,
        op_kwargs={
            'cutoff_date': short_date_str,
            'source': full_model_version_cloud_path,
            # 'prefix': 'test_'
        },
        dag=dag
    )
)
previous_version_key = 'previous_version'
previous_version = f'{{{{ task_instance.xcom_pull(key="{previous_version_key}") }}}}'

model_type = "RSMV2"
storage_source = "s3"
model_output_type = "full_two_output_model"
# pretrained_model_path_cloud = f"s3://thetradedesk-mlplatform-us-east-1/models/test/RSMV2/RSMv2TestTreatment/{model_output_type}/{date_str_prev}/"
policy_table_path = "./input/policy_table/"
# oos_prediction_output_path_cloud = f"s3://thetradedesk-mlplatform-us-east-1/data/test/audience/RSMV2/prediction/population_data/v=1/model_version={date_str_prev}/{date_str}/"
extra_feature_path = "s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/extraFeatures/extraFeatures.json"
partition_field_names = 'split'

calibration_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{model_read_env}/audience/RSMV2/Seed_None/v=1/{date_str}/mixedForward=Calibration/"
population_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{model_read_env}/audience/RSMV2/Seed_None/v=1/{date_str}/split=Population/"
oos_data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{model_read_env}/audience/RSMV2/Seed_None/v=1/{date_str}/split=OOS/"

previous_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/{model_read_env}/RSMV2/full_two_output_model/{previous_version}/"
current_model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/{model_read_env}/RSMV2/full_two_output_model/{date_str}/"

previous_calibration_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{override_env}/audience/RSMV2/prediction/calibration_data/v=1/model_version={previous_version}/{date_str}/"
current_calibration_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{override_env}/audience/RSMV2/prediction/calibration_data/v=1/model_version={date_str}/{date_str}/"
population_output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{override_env}/audience/RSMV2/prediction/population_data/v=1/model_version={date_str}/{date_str}/"
oos_output_path_cloud = f"s3://thetradedesk-mlplatform-us-east-1/data/{override_env}/audience/RSMV2/prediction/oos_data/v=1/model_version={previous_version}/{date_str}/"

measurement_common_argument_list = [
    f"--env={model_read_env}",
    f"--date={date_str}",
    f"--model_type={model_type}",
    f"--storage_source={storage_source}",
    # f"--pretrained_model_path_cloud={pretrained_model_path_cloud}",
    f"--model_output_type={model_output_type}",
    f"--policy_table_path={policy_table_path}",
    # f"--prediction_output_path_cloud={oos_prediction_output_path_cloud}",
    f"--partition_field_names={partition_field_names}",
    # f"--custom_cloud_path={oos_input_path}",
]

measurement_common_argument_list.append("{% raw %}--extraFeatureCloudPath=" + extra_feature_path + "{% endraw %}")

previous_calibration_job_arguments = [
    f"--custom_inference_data_cloud_path={calibration_data_path}", f"--saved_model_cloud_path={previous_model_path}",
    f"--prediction_output_path_cloud={previous_calibration_output_path}"
]
current_calibration_job_arguments = [
    f"--custom_inference_data_cloud_path={calibration_data_path}", f"--saved_model_cloud_path={current_model_path}",
    f"--prediction_output_path_cloud={current_calibration_output_path}"
]
population_job_arguments = [
    f"--custom_inference_data_cloud_path={population_data_path}", f"--saved_model_cloud_path={current_model_path}",
    f"--prediction_output_path_cloud={population_output_path}"
]
oos_job_arguments = [
    f"--custom_inference_data_cloud_path={oos_data_path}", f"--saved_model_cloud_path={previous_model_path}",
    f"--prediction_output_path_cloud={oos_output_path_cloud}"
]

common_step_kwargs = {
    "entry_point_path": "/home/hadoop/app/model_evaluation_pyspark.py",
    "docker_registry": docker_registry,
    "image_name": docker_image_name,
    "image_tag": docker_image_tag,
    "additional_args_option_pairs_list": spark_options_list,
    "timeout_timedelta": timedelta(hours=12),
    "openlineage_config": OpenlineageConfig(enabled=True),
}

oos_prediction_step = setup_prediction_step(common_step_kwargs, "OOSPrediction", measurement_common_argument_list + oos_job_arguments)
previous_calibration_prediction_step = setup_prediction_step(
    common_step_kwargs, "PreviousCalibrationPrediction", measurement_common_argument_list + previous_calibration_job_arguments
)
current_calibration_prediciton_step = setup_prediction_step(
    common_step_kwargs, "CurrentCalibrationPrediction", measurement_common_argument_list + current_calibration_job_arguments
)
population_prediction_step = setup_prediction_step(
    common_step_kwargs, "PopulationPrediction", measurement_common_argument_list + population_job_arguments
)

rsmv2_oos_prediction_cluster_task.add_parallel_body_task(oos_prediction_step)
rsmv2_previous_calibration_prediction_cluster_task.add_parallel_body_task(previous_calibration_prediction_step)
rsmv2_current_calibration_prediction_cluster_task.add_parallel_body_task(current_calibration_prediciton_step)
rsmv2_population_prediction_cluster_task.add_parallel_body_task(population_prediction_step)

audience_rsm_model_offline_metrics_generation_step = EmrJobTask(
    name="populationDataGenerator",
    class_name="com.thetradedesk.audience.jobs.AudienceModelOfflineMetricsGeneratorJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list) + [
        (
            "packages",
            "io.openlineage:openlineage-spark_2.13:1.34.0,com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.7.0",
        ),
    ],
    eldorado_config_option_pairs_list=[
        ("date", run_date),
        ("prThreshold", "0.5"),
        ("numAUCBuckets", 100000),
        ("ttdReadEnv", policy_table_read_env),
        ("AudienceModelPolicyReadableDatasetReadEnv", "prod"),
        ("embeddingDataS3Path", "configdata/prod/audience/embedding/RSMV2/v=1"),
    ],
    executable_path=AUDIENCE_JAR,
    timeout_timedelta=timedelta(hours=4),
)
audience_model_offline_metrics_etl_cluster_task.add_parallel_body_task(audience_rsm_model_offline_metrics_generation_step)

###############################################################################
# Update S3 success files
###############################################################################

update_previous_calibration_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_previous_calibration_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/prediction/calibration_data/v=1/model_version={previous_version}/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=dag,
    )
)
update_current_calibration_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_current_calibration_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/prediction/calibration_data/v=1/model_version={date_str}/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=dag,
    )
)

update_population_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_population_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/prediction/population_data/v=1/model_version={date_str}/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=dag,
    )
)

update_oos_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_oos_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/prediction/oos_data/v=1/model_version={previous_version}/{date_str}/_SUCCESS",
        date=date_str,
        append_file=True,
        dag=dag,
    )
)

# tmp solution, write success file to main data folder for previous version calibration data
tmp_update_previous_calibration_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="tmp_update_previous_calibration_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_PREVIOUS_CALIBRATION_INFERENCE_SUCCESS",
        date="",
        append_file=False,
        dag=dag,
    )
)

tmp_update_current_calibration_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="tmp_update_current_calibration_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_CURRENT_CALIBRATION_INFERENCE_SUCCESS",
        date="",
        append_file=False,
        dag=dag,
    )
)

tmp_update_population_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="tmp_update_population_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_POPULATION_INFERENCE_SUCCESS",
        date="",
        append_file=False,
        dag=dag,
    )
)

tmp_update_oos_sucess_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="tmp_update_oos_sucess_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"data/{override_env}/audience/RSMV2/Seed_None/v=1/{date_str}/_OOS_INFERENCE_SUCCESS",
        date="",
        append_file=False,
        dag=dag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

model_offline_prediction_etl_dag >> dataset_sensor >> detect_previous_version
detect_previous_version >> rsmv2_previous_calibration_prediction_cluster_task >> update_previous_calibration_sucess_file_task >> tmp_update_previous_calibration_sucess_file_task >> final_dag_status_step
detect_previous_version >> rsmv2_current_calibration_prediction_cluster_task >> update_current_calibration_sucess_file_task >> tmp_update_current_calibration_sucess_file_task >> final_dag_status_step
detect_previous_version >> rsmv2_population_prediction_cluster_task >> update_population_sucess_file_task >> tmp_update_population_sucess_file_task
detect_previous_version >> rsmv2_oos_prediction_cluster_task >> update_oos_sucess_file_task >> tmp_update_oos_sucess_file_task
[
    update_population_sucess_file_task, update_oos_sucess_file_task
] >> audience_model_offline_metrics_etl_cluster_task >> final_dag_status_step
