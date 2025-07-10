import copy
import calendar
from datetime import datetime, timedelta
from typing import List

from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from dags.audauto.utils.test_verification import create_wait_operator, create_test_verification_cluster
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.graphics_optimized.g4 import G4
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO, DATPERF
from ttd.ttdenv import TtdEnvFactory
from dags.audauto.utils import utils

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
promote_model_to_rollout = True

# Set to `True` when the current model is a test.
# Ensure that the `promote_model_to_rollout` flag is set to `False` and that the
# `METRIC_VERIFICATION_WAIT_TIME` and `TEST_NAME` config values are specified.
test_verification = False

env_specific_argument_lantern: List[str] = [] if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else [
    "train_set.mlflow_registry_test_name=kongming_prodtest_make_mlflow_happy",
]

# Rollout configurables
DEFAULT_ROLLOUT_IMAGE_TAG = "3.0.2"

# Rollout flag defaults
DEFAULT_ROLLOUT_STRATEGY = "custom"
DEFAULT_SUCCESS_THRESHOLD = 99.5
DEFAULT_ROLLOUT_FEATURE_FLAG_NAME = "conversion-model-rollout"
DEFAULT_SAMPLING_KEY = "conversionaggregateid"
DEFAULT_STAGING_TO_PROD_DELAY_MINUTES = 20.0
DEFAULT_CUSTOM_PERCENTAGES = "1.0,10.0,50.0,100.0"
DEFAULT_CUSTOM_INTERVALS_IN_MINUTES = "40.0,20.0,20.0"

# Test Model Version Verification Defaults
DEFAULT_METRIC_VERIFICATION_WAIT_TIME = 40
DEFAULT_TEST_NAME = ""

# Airflow inputs
ROLLOUT_STRATEGY = f'{{{{ dag_run.conf.get("rollout_strategy") if dag_run.conf is not none and dag_run.conf.get("rollout_strategy") is not none else "{DEFAULT_ROLLOUT_STRATEGY}" }}}}'

SUCCESS_THRESHOLD = f'{{{{ dag_run.conf.get("success_threshold") if dag_run.conf is not none and dag_run.conf.get("success_threshold") is not none else "{DEFAULT_SUCCESS_THRESHOLD}" }}}}'

ROLLOUT_FEATURE_FLAG_NAME = f'{{{{ dag_run.conf.get("rollout_feature_flag_name") if dag_run.conf is not none and dag_run.conf.get("rollout_feature_flag_name") is not none else "{DEFAULT_ROLLOUT_FEATURE_FLAG_NAME}" }}}}'

SAMPLING_KEY = f'{{{{ dag_run.conf.get("sampling_key") if dag_run.conf is not none and dag_run.conf.get("sampling_key") is not none else "{DEFAULT_SAMPLING_KEY}" }}}}'

STAGING_TO_PROD_DELAY_MINUTES = f'{{{{ dag_run.conf.get("staging_to_prod_delay_minutes") if dag_run.conf is not none and dag_run.conf.get("staging_to_prod_delay_minutes") is not none else "{DEFAULT_STAGING_TO_PROD_DELAY_MINUTES}" }}}}'

CUSTOM_PERCENTAGES = f'{{{{ dag_run.conf.get("custom_percentages") if dag_run.conf is not none and dag_run.conf.get("custom_percentages") is not none else "{DEFAULT_CUSTOM_PERCENTAGES}" }}}}'

CUSTOM_INTERVALS_IN_MINUTES = f'{{{{ dag_run.conf.get("custom_intervals_in_minutes") if dag_run.conf is not none and dag_run.conf.get("custom_intervals_in_minutes") is not none else "{DEFAULT_CUSTOM_INTERVALS_IN_MINUTES}" }}}}'

ROLLOUT_IMAGE_TAG = f'{{{{ dag_run.conf.get("rollout_image_tag") if dag_run.conf is not none and dag_run.conf.get("rollout_image_tag") is not none else "{DEFAULT_ROLLOUT_IMAGE_TAG}" }}}}'

METRIC_VERIFICATION_WAIT_TIME = f'{{{{ dag_run.conf.get("metric_verification_wait_time") if dag_run.conf is not none and dag_run.conf.get("metric_verification_wait_time") is not none else "{DEFAULT_METRIC_VERIFICATION_WAIT_TIME}" }}}}'

TEST_NAME = f'{{{{ dag_run.conf.get("test_name") if dag_run.conf is not none and dag_run.conf.get("test_name") is not none else "{DEFAULT_TEST_NAME}" }}}}'


# generic spark settings list we'll add to each step.
def spark_options_list(num_executors):
    num_partitions = int(round(3 * num_executors)) * 30
    spark_options_list = [("executor-memory", "100G"), ("executor-cores", "16"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=100G"),
                          ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
                          ("conf", "spark.default.parallelism=%s" % num_partitions), ("conf", "spark.driver.maxResultSize=50G"),
                          ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                          ("conf", "spark.memory.storageFraction=0.25")]
    return spark_options_list


# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
environment = TtdEnvFactory.get_from_system()
python_date_format = '%Y%m%d'

# Jar
kongming_jar = 's3://thetradedesk-mlplatform-us-east-1/libs/kongming/jars/prod/kongming_production.jar'

docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
docker_image_name = "ttd-base/audauto/kongming"
docker_image_tag = "release"
pytorch_image_name = "ttd-base/audauto/kongming_lantern"
pytorch_image_tag = "release"

# Training Configs
train_set = "full_kongming_two_stage_userdata"
trainset_version = 2
instance_type = "full_prod"
model_date = "{{ ds_nodash }}"
full_train_weekday = calendar.SUNDAY
base_model_date_lookback_in_days = 1
incremental_train_key = 'incremental_train'
base_model_date_key = 'base_model_date'
incremental_train_template_scala = f'{{{{ task_instance.xcom_pull(key="{incremental_train_key}") }}}}'
base_model_date_template = f'{{{{ task_instance.xcom_pull(key="{base_model_date_key}") }}}}'
extended_features: List[str] = []
extended_features_flag = ','.join(extended_features)

# Route errors to test channel in test environment
slack_channel, slack_tags, enable_slack_alert = utils.get_env_vars()

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
kongming_training_dag = TtdDag(
    dag_id="perf-automation-kongming-training",
    start_date=datetime(2025, 6, 8),
    schedule_interval=utils.schedule_interval,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=2,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    run_only_latest=False,
    teams_allowed_to_access=[AUDAUTO.team.jira_team, DATPERF.team.jira_team]
)
dag = kongming_training_dag.airflow_dag

# need all the raw data to complete before training
kongming_etl_dag_sensor = ExternalTaskSensor(
    task_id='kongming_etl_sensor',
    external_dag_id="perf-automation-kongming-etl-v2",
    external_task_id=None,  # wait for the entire DAG to complete
    allowed_states=["success"],
    check_existence=False,
    poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
    timeout=18 * 60 * 60,  # timeout in seconds - wait 18 hours
    mode='reschedule',  # release the worker slot between pokes
    dag=dag
)


def update_xcom_incremental_train_info(execution_date_str, **kwargs):
    task_instance = kwargs['task_instance']

    execution_date = datetime.strptime(execution_date_str, python_date_format)
    incremental_train = execution_date.weekday() != full_train_weekday
    task_instance.xcom_push(key=incremental_train_key, value=str(incremental_train).lower())

    base_model_date_str = ""
    if incremental_train:
        base_model_date = execution_date - timedelta(days=base_model_date_lookback_in_days)
        base_model_date_str = base_model_date.strftime(python_date_format)

    task_instance.xcom_push(key=base_model_date_key, value=base_model_date_str)


update_xcom_incremental_train_task = PythonOperator(
    task_id=update_xcom_incremental_train_info.__name__,
    python_callable=update_xcom_incremental_train_info,
    op_kwargs={'execution_date_str': model_date},
    provide_context=True
)

kongming_model_training_cluster = utils.create_single_master_instance_docker_cluster(
    cluster_name="KongmingLanternModelTraining",
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    instance_type=G5.g5_24xlarge(),
    entrypoint_in_image="opt/application/app/",
)

kongming_model_training_sensitive_cluster = utils.create_single_master_instance_docker_cluster(
    cluster_name="KongmingLanternModelTrainingSensitive",
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    instance_type=G5.g5_24xlarge(),
    entrypoint_in_image="opt/application/app/",
)

kongming_model_stack_cluster = utils.create_single_master_instance_docker_cluster(
    cluster_name="KongmingLanternModelStack",
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    instance_type=G5.g5_4xlarge(),
    entrypoint_in_image="opt/application/app/",
)

kongming_model_scoring_sampled = utils.create_single_master_instance_docker_cluster(
    cluster_name="KongmingLanternModelScoringSampled",
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    instance_type=G5.g5_16xlarge(),
    entrypoint_in_image="opt/application/app/",
)

kongming_model_scoring_last_day = utils.create_single_master_instance_docker_cluster(
    cluster_name="KongmingLanternModelScoringLastDay",
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    instance_type=G5.g5_16xlarge(),
    entrypoint_in_image="opt/application/app/",
)


def get_training_commands():
    return [
        "--nproc-per-node=4",
        '/opt/application/app/training_kongming_isolate_userdata_two_stage.py',
        f"train_set={train_set}",
        "instance=emr_kongming",
        f"instance.env={environment}",
        f"instance.model_creation_date={model_date}",
        f"train_set.base_model_date={base_model_date_template}",
        "training.extended_features=[" + ",".join(['"' + f + '"' for f in extended_features]) + "]" if len(extended_features) > 1 else "",
    ] + env_specific_argument_lantern


def get_scoring_commands():
    return [
        "instance=emr_kongming",
        f"instance.env={environment}",
        f"instance.model_creation_date={model_date}",
        "train_set=[]",
        "training.eval_batch_size=65536",
        "training.num_workers=8",
        '''training.excluded_features=["UserDataOptIn","BidrequestId","BidrequestIdStr"]''',
        "training.model_choice=isolate_userdata",
        "training.phase=2",
        "instance.feature_json='userdata/feature.json'",
        "hyperparameter.sensitive_fields='[Zip,Site,ContextualCategoriesTier1,HasContextualCategoryTier1,ContextualCategoryLengthTier1]'",
        "training.train_sensitive_model=true",
    ]


def get_calibration_commands():
    return [
        f"--env={environment}",
        f"--date={model_date}",
        f"--instance={instance_type}",
        "--handle_sensitive_advertiser=true",
        f"--training_cluster_id={kongming_model_stack_cluster.cluster_id}",
        "--use_historical_lookup=false",
    ]


pytorch_pretrain_phase1 = utils.create_docker_job(
    "PyTorchModelTraining_Pretrain_Phase1",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="torchrun",
    app_path="--nnodes=1",
    exec_args=get_training_commands() + ["training.run_pretrain_phase1=true"],
    emr=kongming_model_training_cluster
)

pytorch_pretrain_phase2 = utils.create_docker_job(
    "PyTorchModelTraining_Pretrain_Phase2",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="torchrun",
    app_path="--nnodes=1",
    exec_args=get_training_commands() + ["training.run_pretrain_phase2=true"],
    emr=kongming_model_training_cluster
)

pytorch_retrain_phase1 = utils.create_docker_job(
    "PyTorchModelTraining_Retrain_Phase1",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="torchrun",
    app_path="--nnodes=1",
    exec_args=get_training_commands() + ["training.run_retrain_phase1=true"],
    emr=kongming_model_training_cluster
)

pytorch_retrain_phase2 = utils.create_docker_job(
    "PyTorchModelTraining_Retrain_Phase2",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="torchrun",
    app_path="--nnodes=1",
    exec_args=get_training_commands() + ["training.run_retrain_phase2=true"],
    emr=kongming_model_training_cluster,
    timeout=timedelta(hours=4)
)

pytorch_pretrain_sensitive = utils.create_docker_job(
    "PyTorchModelTraining_Pretrain_Sensitive",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="torchrun",
    app_path="--nnodes=1",
    exec_args=get_training_commands() + ["training.run_pretrain_phase1=true", "training.train_sensitive_model=true"],
    emr=kongming_model_training_sensitive_cluster
)

pytorch_retrain_sensitive = utils.create_docker_job(
    "PyTorchModelTraining_Retrain_Sensitive",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="torchrun",
    app_path="--nnodes=1",
    exec_args=get_training_commands() + ["training.run_retrain_phase1=true", "training.train_sensitive_model=true"],
    emr=kongming_model_training_sensitive_cluster
)

pytorch_stack_model = utils.create_docker_job(
    "PyTorchModelStacking",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="python3.10",
    app_path="/opt/application/app/concat_model.py",
    exec_args=[
        "train_set=full_kongming_two_stage_userdata",
        "instance=emr_kongming",
        f"instance.env={environment}",
        f"instance.model_creation_date={model_date}",
        f"train_set.base_model_date={base_model_date_template}",
        "instance.bias_path=./advertiser_bias.parquet",
        "training=default",
        "train_set.trainset_pretrain.args.version=2",
        "train_set.trainset_pretrain.args.file_format=csv",
        "train_set.trainset_pretrain_phase2.args.version=2",
        "train_set.trainset_pretrain_phase2.args.file_format=csv",
        "train_set.dataset.args.version=2",
        "train_set.dataset.args.file_format=csv",
        "train_set.trainset_phase2.args.version=2",
        "train_set.trainset_phase2.args.file_format=csv",
    ],
    emr=kongming_model_stack_cluster
)

pytorch_scoring_sampled = utils.create_docker_job(
    "PyTorchModelScoringSampled",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="python3.10",
    app_path="/opt/application/app/main.py",
    exec_args=get_scoring_commands() + [
        "+calibration_set=sampled",
        "training.train_sensitive_model=true",
        "training.num_workers=32",
        "+calibration_set.auxiliary_num_fields=[Weight,Target]",
        "calibration_set.dataset.args.version=2",
    ],
    emr=kongming_model_scoring_sampled,
    timeout=timedelta(hours=4)
)

pytorch_scoring_last_day = utils.create_docker_job(
    "PyTorchModelScoringLast1Day",
    image_name=pytorch_image_name,
    image_tag=pytorch_image_tag,
    docker_registry=docker_registry,
    exec_lang="python3.10",
    app_path="/opt/application/app/main.py",
    exec_args=get_scoring_commands() + [
        "+calibration_set=full",
        "training.num_workers=32",
        "training.train_sensitive_model=true",
        "calibration_set.dataset.args.version=3",
    ],
    emr=kongming_model_scoring_last_day,
    timeout=timedelta(hours=4)
)

# # Calibration Step
calibration_weighted_capacity = 180
calibration_cluster = utils.create_calculation_docker_cluster(
    cluster_name='KongmingModelCalibration',
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    master_instance_type=G4.g4dn_8xlarge(),
    weighted_capacity=calibration_weighted_capacity,
    entrypoint_in_image="opt/application/app/",
)

calibration_task_out = utils.create_pyspark_docker_job(
    name="ModelCalibrationOptOut",
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    spark_options_list=utils.get_spark_options_list(calibration_weighted_capacity),
    exec_lang="python3.10",
    app_path="/home/hadoop/app/calibration.py",
    exec_args=get_calibration_commands() + ["--offline_score_index=0", "--create_warmup=false"],
    emr=calibration_cluster,
)

calibration_task_in = utils.create_pyspark_docker_job(
    name="ModelCalibrationOptIn",
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    spark_options_list=utils.get_spark_options_list(calibration_weighted_capacity),
    exec_lang="python3.10",
    app_path="/home/hadoop/app/calibration.py",
    exec_args=get_calibration_commands() + [
        "--offline_score_index=1",
        f"--promote_to_rollout={promote_model_to_rollout}",
    ],
    emr=calibration_cluster,
)

# Monitor Metrics Prep Step
metrics_prep_weighted_capacity = 150
metrics_prep_cluster = utils.create_calculation_docker_cluster(
    cluster_name='KongmingMonitorMetricsPreparation',
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    master_instance_type=R5.r5_4xlarge(),
    weighted_capacity=metrics_prep_weighted_capacity,
    entrypoint_in_image="opt/application/app/",
)

metrics_prep_task = utils.create_pyspark_docker_job(
    name="MetricsPreparation",
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    spark_options_list=utils.get_spark_options_list(metrics_prep_weighted_capacity),
    exec_lang="python3.10",
    app_path='/home/hadoop/app/prep_monitor_metrics.py',
    exec_args=[
        f"--env={environment}",
        f"--date={model_date}",
        "--use_csv=true",
        f"--trainset_version={trainset_version}",
        f"--incremental_train={incremental_train_template_scala}",
        "--gen_model_perf=false",
        "--gen_metadata=true",
        "--gen_calibration_log=true",
    ],
    emr=metrics_prep_cluster,
)

# DAG Dependencies
kongming_training_dag >> kongming_model_training_cluster
kongming_training_dag >> kongming_model_training_sensitive_cluster
kongming_etl_dag_sensor >> update_xcom_incremental_train_task >> kongming_model_training_cluster.first_airflow_op()
pytorch_pretrain_phase1 >> pytorch_pretrain_phase2 >> pytorch_retrain_phase1 >> pytorch_retrain_phase2
kongming_etl_dag_sensor >> update_xcom_incremental_train_task >> kongming_model_training_sensitive_cluster.first_airflow_op()
pytorch_pretrain_sensitive >> pytorch_retrain_sensitive
kongming_model_training_cluster >> kongming_model_stack_cluster
kongming_model_training_sensitive_cluster >> kongming_model_stack_cluster
kongming_model_training_cluster >> kongming_model_scoring_sampled
kongming_model_training_sensitive_cluster >> kongming_model_scoring_sampled
kongming_model_training_cluster >> kongming_model_scoring_last_day
kongming_model_training_sensitive_cluster >> kongming_model_scoring_last_day
kongming_model_scoring_sampled >> calibration_cluster
kongming_model_scoring_last_day >> calibration_cluster
calibration_task_out >> calibration_task_in
kongming_model_training_cluster >> calibration_cluster >> metrics_prep_cluster

final_dag_check = FinalDagStatusCheckOperator(dag=dag)

if environment == TtdEnvFactory.prod and promote_model_to_rollout:
    docker_rollout_image_name = "ttd-base/datperf/dalgo_utils"
    docker_rollout_registry = "production.docker.adsrvr.org"
    core_and_master_fleet_rollout_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[C5.c5_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    model_rollout_command_line_arguments = [
        f'--rollout_strategy={ROLLOUT_STRATEGY}', f'--model_name={"kongming"}', f'--success_threshold={SUCCESS_THRESHOLD}',
        f'--rollout_feature_flag_name={ROLLOUT_FEATURE_FLAG_NAME}', f'--sampling_key={SAMPLING_KEY}', f'--environment={environment}',
        f'--staging_to_prod_delay_minutes={STAGING_TO_PROD_DELAY_MINUTES}',
        f'--training_cluster_id={kongming_model_stack_cluster.cluster_id}', f'--custom_percentages={CUSTOM_PERCENTAGES}',
        f'--custom_intervals_in_minutes={CUSTOM_INTERVALS_IN_MINUTES}'
    ]
    rollout_cluster_task = DockerEmrClusterTask(
        name="ModelRollout",
        image_name=docker_rollout_image_name,
        image_tag=ROLLOUT_IMAGE_TAG,
        docker_registry=docker_rollout_registry,
        master_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        core_fleet_instance_type_configs=core_and_master_fleet_rollout_instance_type_configs,
        cluster_tags=utils.cluster_tags,
        emr_release_label="emr-6.9.0",
        environment=environment,
        additional_application_configurations=copy.deepcopy(utils.get_application_configuration()),
        enable_prometheus_monitoring=True,
        entrypoint_in_image="/opt/application/app/",
        retries=0
    )

    rollout_docker_command = DockerCommandBuilder(
        docker_registry=docker_rollout_registry,
        docker_image_name=docker_rollout_image_name,
        docker_image_tag=ROLLOUT_IMAGE_TAG,
        execution_language="python3",
        path_to_app="/lib/dalgo_utils/janus/kickoff_rollout.py",
        additional_parameters=["--shm-size=5g", "--ulimit memlock=-1"],
        additional_execution_parameters=model_rollout_command_line_arguments,
    )

    rollout_step = DockerRunEmrTask(
        name="ModelRollout", docker_run_command=rollout_docker_command.build_command(), timeout_timedelta=timedelta(hours=3)
    )
    rollout_cluster_task.add_sequential_body_task(rollout_step)

    calibration_cluster >> rollout_cluster_task
    rollout_cluster_task.last_airflow_op() >> final_dag_check
elif test_verification:
    metric_verification_wait_task = create_wait_operator(METRIC_VERIFICATION_WAIT_TIME, dag)
    calibration_cluster.last_airflow_op() >> metric_verification_wait_task

    test_verification_cluster_task = create_test_verification_cluster(
        docker_dalgo_image_tag=ROLLOUT_IMAGE_TAG,
        cluster_tags=utils.cluster_tags,
        model_name="kongming",
        success_threshold=SUCCESS_THRESHOLD,
        training_cluster_id=kongming_model_stack_cluster.cluster_id,
        test_name=TEST_NAME,
        wait_duration_seconds=METRIC_VERIFICATION_WAIT_TIME
    )

    calibration_cluster >> test_verification_cluster_task
    metric_verification_wait_task >> test_verification_cluster_task.first_airflow_op()
    test_verification_cluster_task.last_airflow_op() >> final_dag_check
else:
    metrics_prep_cluster.last_airflow_op() >> final_dag_check
