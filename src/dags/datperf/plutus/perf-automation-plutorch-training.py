from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DATPERF, AUDAUTO
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from dags.datperf.datasets import plutus_etl_dataset
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.datperf.utils.sleep_timer import SleepTimer
from airflow.sensors.date_time import DateTimeSensor

# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
MODELVERSION = "{{ logical_date.strftime(\"%Y%m%d%H%M\") }}"
MODELNAME = "plutorch"

# Flags to make it easier to prodtest plutorch
PRODTEST_BRANCH_NAME = ""
PRODTEST_TEST_SCRIPTS = False
PRODTEST_TRAIN_IMAGE = False

CLUSTERSETUP = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/plutorch/scripts/clustersetup.sh"
MODELRUN = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/plutorch/scripts/modelrun.sh"

airflow_env = TtdEnvFactory.get_from_system()
if airflow_env == TtdEnvFactory.prodTest and PRODTEST_TEST_SCRIPTS:
    # Using .lower() here because gitlab ci
    CLUSTERSETUP = f"s3://thetradedesk-mlplatform-us-east-1/libs/plutus/plutorch/scripts/mergerequests/{PRODTEST_BRANCH_NAME}/clustersetup.sh"
    MODELRUN = f"s3://thetradedesk-mlplatform-us-east-1/libs/plutus/plutorch/scripts/mergerequests/{PRODTEST_BRANCH_NAME}/modelrun.sh"

# MLFlow/Janus registration
EXPERIMENTATION_DOCKER_REGISTRY = "internal.docker.adsrvr.org"
EXPERIMENTATION_DOCKER_IMAGE_NAME = "plutorch-training"
EXPERIMENTATION_DOCKER_IMAGE_TAG = "release"
EXPERIMENTATION_ENTRYPOINT_PATH = "/opt/application/experiment.py"


def generate_experiment_key(task_instance: TaskInstance, **_):
    execution_date = task_instance.execution_date
    experiment_key = f"plutus-training-{execution_date.strftime('%Y%m%d%H')}"
    task_instance.xcom_push(key="experiment_key", value=experiment_key)


# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
plutorch_train = TtdDag(
    dag_id="perf-automation-plutorch-training",
    start_date=datetime(2024, 7, 16, 12, 0),
    schedule_interval=timedelta(hours=24),
    retries=0,
    retry_delay=timedelta(hours=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/yrMMCQ',
    default_args={"owner": "DATPERF"},
    enable_slack_alert=False,
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=36),
    tags=["DATPERF", "Plutus"],
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutorch_train.airflow_dag

plutus_etl_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='plutus_etl_available',
        poke_interval=60 * 30,
        timeout=60 * 60 * 20,  # wait up to 20 hours
        ds_date="{{ data_interval_start.to_datetime_string() }}",
        datasets=[plutus_etl_dataset]
    )
)

plutorch_train_cluster = EmrClusterTask(
    name="plutorchModelTrain",
    # only a master node for this training, but cant define a single node cluster with runjobflow operator
    # would usually just use an ec2 instance but ongoing battle with secops et. al. has left us unable to provision
    # from ec2 at the moment so picking single cheapest instance for this cluster for now
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G5.g5_16xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            M5.m5_xlarge().with_fleet_weighted_capacity(1),
            M5.m5_2xlarge().with_fleet_weighted_capacity(1),
            C5.c5_xlarge().with_fleet_weighted_capacity(1),
            C5.c5_2xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1
    ),
    emr_release_label='emr-6.9.0',
    cluster_auto_termination_idle_timeout_seconds=-1,
)

cluster_setup = EmrJobTask(
    name="ClusterSetup",
    job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
    executable_path=CLUSTERSETUP,
    class_name=None
)

training_args = ['-v', MODELVERSION, '-s', 'True', '-r', 'True', '-e', airflow_env.execution_env.lower()]

if airflow_env == TtdEnvFactory.prodTest:
    training_args.append('-m')
    training_args.append('training-prod-test')
    if PRODTEST_TRAIN_IMAGE:
        training_args.append('-t')
        training_args.append(PRODTEST_BRANCH_NAME)

model_train = EmrJobTask(
    name="ModelTraining",
    job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
    executable_path=MODELRUN,
    timeout_timedelta=timedelta(hours=16),
    command_line_arguments=training_args,
    class_name=None
)

plutorch_train_cluster.add_sequential_body_task(cluster_setup)
plutorch_train_cluster.add_sequential_body_task(model_train)

plutorch_train >> plutus_etl_dataset_sensor >> plutorch_train_cluster

# -------------------
# MODEL PROMOTION
# -------------------

# Add a timer for start_experiment_delay to use to wake up
start_experiment_timer_task = OpTask(op=SleepTimer(task_id='start_experiment_timer', duration_mins=15, dag=dag))

# Experiment start is delayed to give some time for MLFlow update to propagate to BidderCache nodes
start_experiment_delay_task = OpTask(
    op=DateTimeSensor(
        task_id='start_experiment_delay',
        target_time="{{ ti.xcom_pull(task_ids='start_experiment_timer', key='time_to_wake')}}",
        mode='reschedule'
    )
)

# This task generates the experiment key that will be used to reference the experiment in Janus. The
# start_experiment task will create the experiment using this key and then the end_experiment task will use the key
# to retrieve the KPIs and conclude the experiment.
generate_experiment_key_task = OpTask(
    op=PythonOperator(task_id='generate_experiment_key', provide_context=True, python_callable=generate_experiment_key, dag=dag)
)

experiment_instance_types = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)
experiment_cluster_tags = {"Team": DATPERF.team.jira_team, "Job": plutorch_train.airflow_dag.dag_id}

# Start Experiment:
# An EMR cluster is started and Docker container is run on the cluster to start the experiment in Janus
start_experiment_docker_command = DockerCommandBuilder(
    docker_image_name=EXPERIMENTATION_DOCKER_IMAGE_NAME,
    docker_image_tag=EXPERIMENTATION_DOCKER_IMAGE_TAG,
    docker_registry=EXPERIMENTATION_DOCKER_REGISTRY,
    path_to_app=EXPERIMENTATION_ENTRYPOINT_PATH,
    additional_execution_parameters=[
        "start_experiment",
        f"--janus_experiment_key={{{{task_instance.xcom_pull(task_ids='{generate_experiment_key_task.task_id}', key='experiment_key')}}}}",
        f"--model_training_date={MODELVERSION}",
        f"--job_cluster_id={{{{task_instance.xcom_pull(task_ids='{model_train.add_job_task.task_id}', key='job_flow_id')}}}}"
    ],
)

start_experiment_docker_step = DockerRunEmrTask(name="StartExperiment", docker_run_command=start_experiment_docker_command.build_command())

start_experiment_cluster = DockerEmrClusterTask(
    name="plutorch_start_experiment",
    image_name=EXPERIMENTATION_DOCKER_IMAGE_NAME,
    image_tag=EXPERIMENTATION_DOCKER_IMAGE_TAG,
    docker_registry=EXPERIMENTATION_DOCKER_REGISTRY,
    entrypoint_in_image="/opt/application",
    master_fleet_instance_type_configs=experiment_instance_types,
    cluster_tags=experiment_cluster_tags,
    core_fleet_instance_type_configs=experiment_instance_types,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri=f"s3://ttd-identity/datapipeline/logs/{airflow_env.execution_env}/plutorch-start-janus-experiment"
)

start_experiment_cluster.add_parallel_body_task(start_experiment_docker_step)

# Add a timer for end_experiment_delay to use to wake up
end_experiment_timer_task = OpTask(op=SleepTimer(task_id='end_experiment_timer', duration_mins=6 * 60, dag=dag))

# Experiment will run for ~6 hours to give time for KPI pipeline to run
end_experiment_delay_task = OpTask(
    op=DateTimeSensor(
        task_id='end_experiment_delay',
        target_time="{{ ti.xcom_pull(task_ids='end_experiment_timer', key='time_to_wake')}}",
        mode='reschedule'
    )
)

# End Experiment:
# An EMR cluster is started and Docker container is run to end the experiment in Janus, retrieve the metrics,
# and promote the staging model version in MLFlow based on the metrics.
end_experiment_docker_command = DockerCommandBuilder(
    docker_image_name=EXPERIMENTATION_DOCKER_IMAGE_NAME,
    docker_image_tag=EXPERIMENTATION_DOCKER_IMAGE_TAG,
    docker_registry=EXPERIMENTATION_DOCKER_REGISTRY,
    path_to_app=EXPERIMENTATION_ENTRYPOINT_PATH,
    additional_execution_parameters=[
        "end_experiment", f"--model_training_date={MODELVERSION}",
        f"--janus_experiment_key={{{{task_instance.xcom_pull(task_ids='{generate_experiment_key_task.task_id}', key='experiment_key')}}}}",
        f"--job_cluster_id={{{{task_instance.xcom_pull(task_ids='{model_train.add_job_task.task_id}', key='job_flow_id')}}}}",
        "--promotion_metric=savings_rate"
    ],
)

end_experiment_docker_step = DockerRunEmrTask(name="EndExperiment", docker_run_command=end_experiment_docker_command.build_command())

end_experiment_cluster = DockerEmrClusterTask(
    name="plutorch_end_experiment",
    image_name=EXPERIMENTATION_DOCKER_IMAGE_NAME,
    image_tag=EXPERIMENTATION_DOCKER_IMAGE_TAG,
    docker_registry=EXPERIMENTATION_DOCKER_REGISTRY,
    entrypoint_in_image="/opt/application",
    master_fleet_instance_type_configs=experiment_instance_types,
    cluster_tags=experiment_cluster_tags,
    core_fleet_instance_type_configs=experiment_instance_types,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri=f"s3://ttd-identity/datapipeline/logs/{airflow_env.execution_env}/plutorch-end-janus-experiment"
)

end_experiment_cluster.add_parallel_body_task(end_experiment_docker_step)
plutorch_train_cluster >> start_experiment_timer_task >> start_experiment_delay_task >> generate_experiment_key_task >> \
    start_experiment_cluster >> end_experiment_timer_task >> end_experiment_delay_task >> end_experiment_cluster
