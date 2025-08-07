import copy
from datetime import datetime, timedelta
from typing import List

from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import TaskInstanceState
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, PySparkEmrTask, DockerCommandBuilder, DockerRunEmrTask
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.compute_optimized.c4 import C4

from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m4 import M4
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
from dags.audauto.utils import utils

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]


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


application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

cluster_tags = {
    'Team': AUDAUTO.team.jira_team,
}

# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
spark_date_macro = "{{ds}}"
environment = TtdEnvFactory.get_from_system()
experiment_name = ""

env_specific_argument_kongming_python: List[str] = []

# Jar
kongming_jar = 's3://thetradedesk-mlplatform-us-east-1/libs/kongming/jars/prod/kongming_production.jar'

# Training Configs
training_dag_id = 'perf-automation-roas-training-restricted'
model_date = "{{ds_nodash}}"
docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
docker_image_name = "ttd-base/audauto/kongming"
docker_image_tag = "release"

# Metrics Configs
task = 'roas'
scoring_sensitive_model = True
trainset_version = 2
feature_json_path = "feature_roas_sensitive.json"
oos_delay = 10
oos_date = "{{macros.ds_format(macros.ds_add(ds, " + str(-oos_delay) + "), '%Y-%m-%d', '%Y%m%d')}}"

# impression + conversion lookback

# Route errors to test channel in test environment
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    slack_channel = '#dev-perf-auto-alerts-cpa-roas'  # send out alerts to testing channel for now
    slack_tags = AUDAUTO.team.sub_team
    enable_slack_alert = True
else:
    slack_channel = '#scrum-perf-automation-alerts-testing'
    slack_tags = None
    enable_slack_alert = True

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
roas_metric_dag = TtdDag(
    dag_id="perf-automation-roas-restricted-metric-etl",
    start_date=datetime(2025, 7, 20),
    schedule_interval=utils.schedule_interval,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=2,
    max_active_runs=6,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    run_only_latest=False
)
dag = roas_metric_dag.airflow_dag

# need all the raw data to complete before training
roas_training_dag_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id="roas_training_sensor",
        external_dag_id=training_dag_id,
        external_task_id=None,  # wait for the entire DAG to complete
        allowed_states=[TaskInstanceState.SUCCESS],
        check_existence=False,
        poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
        timeout=15 * 60 * 60,  # timeout in seconds - wait 15 hours
        mode="reschedule",  # release the worker slot between pokes
        dag=dag
    )
)


def create_single_master_instance_docker_cluster(
    cluster_name: str,
    docker_registry: str,
    docker_image_name: str,
    docker_image_tag: str,
    instance_type: EmrInstanceType,
):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[instance_type.with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            C4.c4_large().with_fleet_weighted_capacity(1),
            M4.m4_large().with_fleet_weighted_capacity(1),
            C5.c5_xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1
    )

    return DockerEmrClusterTask(
        name=cluster_name,
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label="emr-6.9.0",
        environment=environment,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(application_configuration),
    )


def create_calculation_docker_cluster(
    cluster_name: str,
    docker_registry: str,
    docker_image_name: str,
    docker_image_tag: str,
    weighted_capacity: int,
):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[R5.r5_4xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(768).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R5.r5_24xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(6)
        ],
        on_demand_weighted_capacity=weighted_capacity
    )

    return DockerEmrClusterTask(
        name=cluster_name,
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        entrypoint_in_image='opt/application/app/',
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        environment=environment,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(application_configuration),
    )


calculation_cluster_spark_options_list = [("executor-memory", "100G"), ("executor-cores", "16"),
                                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                          ("conf", "spark.driver.memory=110G"), ("conf", "spark.driver.cores=15"),
                                          ("conf", "spark.sql.shuffle.partitions=1400"), ("conf", "spark.default.parallelism=1400"),
                                          ("conf", "spark.driver.maxResultSize=50G"), ("conf", "spark.dynamicAllocation.enabled=true"),
                                          ("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25")]

metrics_scoring_cluster = create_single_master_instance_docker_cluster(
    cluster_name='ROASMetricsScoring',
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    instance_type=R5.r5_24xlarge(),
)


def generate_insample_scoring_task():
    scoring_docker_command = DockerCommandBuilder(
        docker_registry=docker_registry,
        docker_image_name=docker_image_name,
        execution_language="python3.10",
        docker_image_tag=docker_image_tag,
        path_to_app='/opt/application/app/main.py',
        additional_parameters=[
            "-v /mnt/tmp:/opt/application/tmp",
        ],
        additional_execution_parameters=[
            f"--env={environment}",
            "--run_score=true",
            "--scoring_mode=2",
            f"--model_creation_date={model_date}",
            # f"--experiment={experiment}",
            "--model_path=./input/model/",
            f"--score_dates={model_date}",
            f"--task={task}",
            f"--feature_json_file={feature_json_path}",
            f"--trainset_version={trainset_version}",
            "--neofy_roas_model=1",
            f"--scoring_sensitive_model={scoring_sensitive_model}"
        ] + env_specific_argument_kongming_python
    )
    return DockerRunEmrTask(
        name="Scoring_Insample",
        docker_run_command=scoring_docker_command.build_command(),
        timeout_timedelta=timedelta(hours=1),
    )


def generate_oos_scoring_task():
    scoring_docker_command = DockerCommandBuilder(
        docker_registry=docker_registry,
        docker_image_name=docker_image_name,
        execution_language="python3.10",
        docker_image_tag=docker_image_tag,
        path_to_app='/opt/application/app/main.py',
        additional_parameters=[
            "-v /mnt/tmp:/opt/application/tmp",
        ],
        additional_execution_parameters=[
            f"--env={environment}",
            "--run_score=true",
            "--scoring_mode=3",
            f"--model_creation_date={oos_date}",
            # f"--experiment={experiment}",
            "--model_path=./input/model/",
            f"--score_dates={model_date}",
            f"--task={task}",
            f"--feature_json_file={feature_json_path}",
            f"--trainset_version={trainset_version}",
            "--scoring_batch_size=65536",
            "--virtual_cores=80",
            "--neofy_roas_model=1",
            f"--scoring_sensitive_model={scoring_sensitive_model}"
        ] + env_specific_argument_kongming_python
    )
    return DockerRunEmrTask(
        name="Scoring_OOS",
        docker_run_command=scoring_docker_command.build_command(),
        timeout_timedelta=timedelta(hours=1),
    )


insample_scoring_task = generate_insample_scoring_task()
oos_scoring_task = generate_oos_scoring_task()
metrics_scoring_cluster.add_parallel_body_task(insample_scoring_task)
metrics_scoring_cluster.add_parallel_body_task(oos_scoring_task)
# Monitor Metrics Prep Step

metrics_prep_weighted_capacity = 30
metrics_prep_cluster = create_calculation_docker_cluster(
    cluster_name='ROASMonitorMetricsPreparation',
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    weighted_capacity=metrics_prep_weighted_capacity,
)
OOS_evaluation_task = PySparkEmrTask(
    name="EvaluateOOS",
    entry_point_path='/home/hadoop/app/generating_metrics.py',
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    additional_args_option_pairs_list=spark_options_list(metrics_prep_weighted_capacity),
    command_line_arguments=[
        f"--env={environment}", f"--date={model_date}", f"--task={task}", "--score_and_evaluate_val=True",
        "--score_and_evaluate_holdout=True", "--calc_overall_metrics=False", f"--trainset_version={trainset_version}",
        f"--scoring_sensitive_model={scoring_sensitive_model}"
    ] + env_specific_argument_kongming_python,
    timeout_timedelta=timedelta(hours=8),
    python_distribution="python3.10"
)
metrics_prep_cluster.add_parallel_body_task(OOS_evaluation_task)

metrics_prep_task = PySparkEmrTask(
    name="MetricsPreparation",
    entry_point_path='/home/hadoop/app/prep_monitor_metrics.py',
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    additional_args_option_pairs_list=spark_options_list(metrics_prep_weighted_capacity),
    command_line_arguments=[
        f"--env={environment}", f"--date={model_date}", "--use_csv=true", f"--task={task}", f"--trainset_version={trainset_version}",
        f"--scoring_sensitive_model={scoring_sensitive_model}"
    ],
    timeout_timedelta=timedelta(hours=1),
    python_distribution="python3.10"
)
metrics_prep_cluster.add_parallel_body_task(metrics_prep_task)
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG Dependencies
roas_metric_dag >> roas_training_dag_sensor >> metrics_scoring_cluster >> metrics_prep_cluster
insample_scoring_task >> oos_scoring_task
OOS_evaluation_task >> metrics_prep_task >> final_dag_status_step
