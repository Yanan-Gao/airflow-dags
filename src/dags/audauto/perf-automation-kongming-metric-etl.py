import copy
from datetime import datetime, timedelta

from airflow.sensors.external_task_sensor import ExternalTaskSensor

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask, PySparkEmrTask
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.ttdenv import TtdEnvFactory
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
environment = TtdEnvFactory.get_from_system()

# Jar
kongming_jar = 's3://thetradedesk-mlplatform-us-east-1/libs/kongming/jars/prod/kongming_production.jar'

# Training Configs
training_dag_id = 'perf-automation-kongming-training'
incremental_train_key = 'incremental_train'
incremental_train_push_task_id = 'update_xcom_incremental_train_info'
incremental_train_template_scala = f'{{{{ task_instance.xcom_pull(dag_id="{training_dag_id}", task_ids="{incremental_train_push_task_id}",key="{incremental_train_key}") }}}}'

docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
docker_image_name = "ttd-base/audauto/kongming"
docker_image_tag = "release"
pytorch_image_name = "ttd-base/audauto/kongming_lantern"
pytorch_image_tag = "release"

# OOS Configs
oos_lookback_timely = (1, 2)
oos_lookback_groundtruth = (3, 7)  # impression + conversion lookback
sum_lookback_timely = sum(oos_lookback_timely)
sum_lookback_groundtruth = sum(oos_lookback_groundtruth)

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
kongming_metric_dag = TtdDag(
    dag_id="perf-automation-kongming-metric-etl",
    start_date=datetime(2025, 5, 10),
    schedule_interval=utils.schedule_interval,
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=2,
    max_active_runs=5,
    retry_delay=timedelta(minutes=10),
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    enable_slack_alert=enable_slack_alert,
    dagrun_timeout=timedelta(hours=36)
)
dag = kongming_metric_dag.airflow_dag

# need all the raw data to complete before training
kongming_training_dag_sensor = ExternalTaskSensor(
    task_id='kongming_training_sensor',
    external_dag_id=training_dag_id,
    external_task_id=None,  # wait for the entire DAG to complete
    allowed_states=["success"],
    check_existence=False,
    poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
    timeout=24 * 60 * 60,  # timeout in seconds - wait 12 hours
    mode='reschedule',  # release the worker slot between pokes
    dag=dag
)


# todo: revisit disk configs
def create_calculation_cluster(cluster_name: str, weighted_capacity: int):
    return EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[R5.r5_4xlarge().with_ebs_size_gb(300).with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        cluster_tags=cluster_tags,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R5.r5_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(1),
                R5.r5_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(2),
                R5.r5_24xlarge().with_ebs_size_gb(6144).with_max_ondemand_price().with_fleet_weighted_capacity(3)
            ],
            on_demand_weighted_capacity=weighted_capacity
        ),
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        environment=environment,
        additional_application_configurations=copy.deepcopy(application_configuration),
        enable_prometheus_monitoring=True,
        cluster_auto_termination_idle_timeout_seconds=60 * 60,
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
            C5.c5_xlarge().with_fleet_weighted_capacity(1),
        ], on_demand_weighted_capacity=1
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
    master_instance_type: EmrInstanceType,
    weighted_capacity: int,
):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[master_instance_type.with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
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
        emr_release_label="emr-6.9.0",
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

pytorch_scorer_cluster = create_single_master_instance_docker_cluster(
    cluster_name='KongmingLanternPerformanceMetricsGeneration',
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    instance_type=G5.g5_24xlarge(),
)
pytorch_measurement_timely_two_score_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    execution_language="python3.10",
    path_to_app='/opt/application/app/main.py',
    # path_to_app='/opt/application/app/main_userdata.py',
    additional_parameters=[
        "--gpus all",
        "--ipc=host",
        "-e TF_GPU_THREAD_MODE=gpu_private",
        "-e NVIDIA_DISABLE_REQUIRE=1",
    ],
    additional_execution_parameters=[
        "instance=emr_kongming",
        f"instance.env={environment}",
        "instance.model_creation_date={{ macros.ds_format(macros.ds_add(ds, -" + str(sum_lookback_timely) + "), '%Y-%m-%d', '%Y%m%d')}}",
        "train_set=[]",
        "+test_set=default",
        "training.eval_batch_size=65536",
        "training.num_workers=16",
        "training.prefetch_factor=1",
        "+data_set@test_set.datasets.delay_3d=full_oos_timely",
        "test_set.datasets.delay_3d.args.version=2",
        "test_set.apply_calibration=true",
        "training.model_choice=isolate_userdata",
        "training.phase=2",
        "training.pretrain=false",
        '''instance.feature_json='userdata/feature.json\'''',
    ]
)
pytorch_timely_two_score_measurement_task = DockerRunEmrTask(
    name="MetricsGenerationTimelyTwoScore",
    docker_run_command=pytorch_measurement_timely_two_score_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=6),
)
pytorch_scorer_cluster.add_parallel_body_task(pytorch_timely_two_score_measurement_task)

pytorch_measurement_delayed_two_score_docker_command = DockerCommandBuilder(
    docker_registry=docker_registry,
    docker_image_name=pytorch_image_name,
    docker_image_tag=pytorch_image_tag,
    execution_language="python3.10",
    path_to_app='/opt/application/app/main.py',
    # path_to_app='/opt/application/app/main_userdata.py',
    additional_parameters=[
        "--gpus all",
        "--ipc=host",
        "-e TF_GPU_THREAD_MODE=gpu_private",
        "-e NVIDIA_DISABLE_REQUIRE=1",
    ],
    additional_execution_parameters=[
        "instance=emr_kongming",
        f"instance.env={environment}",
        "instance.model_creation_date={{ macros.ds_format(macros.ds_add(ds, -" + str(sum_lookback_groundtruth) +
        "), '%Y-%m-%d', '%Y%m%d')}}",
        "train_set=[]",
        "+test_set=default",
        "training.eval_batch_size=65536",
        "training.num_workers=16",
        "training.prefetch_factor=1",
        "+data_set@test_set.datasets.delay_10d=full_oos",
        "test_set.datasets.delay_10d.args.version=2",
        "test_set.apply_calibration=true",
        "training.model_choice=isolate_userdata",
        "training.phase=2",
        "training.pretrain=false",
        '''instance.feature_json='userdata/feature.json\'''',
    ]
)
pytorch_delayed_two_score_measurement_task = DockerRunEmrTask(
    name="MetricsGenerationDelayedTwoScore",
    docker_run_command=pytorch_measurement_delayed_two_score_docker_command.build_command(),
    timeout_timedelta=timedelta(hours=10),
)
pytorch_scorer_cluster.add_parallel_body_task(pytorch_delayed_two_score_measurement_task)

# Monitor Metrics Prep Step
metrics_prep_weighted_capacity = 150
metrics_prep_cluster = create_calculation_docker_cluster(
    cluster_name='KongmingMonitorMetricsPreparation',
    docker_registry=docker_registry,
    docker_image_name=docker_image_name,
    docker_image_tag=docker_image_tag,
    master_instance_type=R5.r5_4xlarge(),
    weighted_capacity=metrics_prep_weighted_capacity,
)

metrics_prep_task = PySparkEmrTask(
    name="MetricsPreparation",
    entry_point_path='/home/hadoop/app/prep_monitor_metrics.py',
    docker_registry=docker_registry,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    additional_args_option_pairs_list=spark_options_list(metrics_prep_weighted_capacity),
    command_line_arguments=[
        f"--env={environment}",
        "--date={{ ds_nodash }}",
        "--use_csv=true",
        f"--incremental_train={incremental_train_template_scala}",
        "--gen_model_perf=false",
        "--gen_metadata=false",
        "--gen_calibration_log=false",
    ],
    timeout_timedelta=timedelta(hours=2),
    python_distribution="python3.10"
)
metrics_prep_cluster.add_parallel_body_task(metrics_prep_task)

# DAG Dependencies
kongming_metric_dag >> pytorch_scorer_cluster
kongming_training_dag_sensor >> pytorch_scorer_cluster.first_airflow_op()
pytorch_timely_two_score_measurement_task >> pytorch_delayed_two_score_measurement_task

pytorch_scorer_cluster >> metrics_prep_cluster

final_dag_check = FinalDagStatusCheckOperator(dag=dag)
metrics_prep_cluster.last_airflow_op() >> final_dag_check
