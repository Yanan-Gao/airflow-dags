import copy
import logging
from datetime import datetime, timedelta

from airflow import macros
from airflow.exceptions import AirflowFailException
from airflow.operators.python import BranchPythonOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.docker import DockerEmrClusterTask, DockerCommandBuilder, DockerRunEmrTask
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = 'adpb-user-embedding-training'
owner = ADPB.team

# Job configuration
job_environment = TtdEnvFactory.get_from_system()
env_str = "prod"
job_start_date = datetime(2024, 8, 1, 5, 0)  # Upstream Input ETL starts at 00:00 and takes ~ 5 hours to complete
job_schedule_interval = "0 5 * * 6"  # runs every saturday.
run_date = "{{ ds }}"
delta_days = 0
run_date_str = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y%m%d') }}"
training_days_of_week = [4]  # week days to train and require input data to be ready, other week days optional
cluster_idle_timeout_seconds = 240 * 60  # 4 hour idle timeout, without termination protection this should be longer than data sync job run time.

docker_registry = "nexustheturd-docker-14d-retention.gen.adsrvr.org"
docker_image_name = "twindom-training"
docker_image_tag = "release"

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5

NVIDIA_TOOLKIT_INSTALL_SCRIPT = "s3://ttd-build-artefacts/mlops/scripts/docker/gpu/v1/install_nvidia_container_toolkit.sh"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

cluster_tags = {
    'Team': owner.jira_team,
}

# DAG
training_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    depends_on_past=False,
    run_only_latest=False,
    max_active_runs=1,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    enable_slack_alert=True,
    retries=0
)

dag = training_dag.airflow_dag


class S3KeySensorPatched(DatasetCheckSensor):
    """
    Patched S3KeySensor that won't skip all down streams unconditionally, this allows down stream trigger to work properly
    https://github.com/apache/airflow/issues/8696
    """

    def _do_skip_downstream_tasks(self, context):
        logging.info("Skip sensor only.")
        pass


modeling_pos_neg_label_with_feature_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    env_aware=True,
    data_name="models/user-embedding/dataforgein/public/modeling_pos_neg_label_with_feature",
    version=None,
    date_format="date=%Y%m%d",
)

input_etl_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[modeling_pos_neg_label_with_feature_dataset],
        ds_date="{{ (data_interval_end - macros.timedelta(days=1)).strftime(\"%Y-%m-%d 00:00:00\") }}",
        task_id='training_data_available',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 12,  # wait up to 6 hours
    )
)


def model_train_branch(**kwargs):
    week_days = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    run_date_str_arg = kwargs['run_date_str']
    run_date_weekday = macros.datetime.strptime(run_date_str_arg, '%Y%m%d').weekday()

    input_etl_sensor_task_state = kwargs["dag_run"].get_task_instance(task_id=input_etl_sensor_task.task_id).current_state()

    logging.info(f"Input etl sensor task state: {input_etl_sensor_task_state}")
    sensor_success = input_etl_sensor_task_state == State.SUCCESS

    if run_date_weekday in training_days_of_week:
        logging.info(f"Run date {run_date_str_arg} is {week_days[run_date_weekday]}, do model training.")
        if not sensor_success:
            raise AirflowFailException(f"Input data not ready: {run_date_str_arg}")
        return model_training_start.task_id
    else:
        logging.info(f"Run date {run_date_str_arg} is {week_days[run_date_weekday]}, not training day.")
        if not sensor_success:
            return final_dag_check.task_id
        logging.info(f"Input {run_date_str_arg} is somehow ready so let's train anyway.")
        return model_training_start.task_id


model_train_branch_op_task = OpTask(
    op=BranchPythonOperator(
        task_id='model_train_branch',
        provide_context=True,
        op_kwargs={'run_date_str': run_date_str},
        python_callable=model_train_branch,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )
)

model_training_start = OpTask(op=DummyOperator(task_id="model_training_start", dag=dag), )

train_cluster = DockerEmrClusterTask(
    name="train_cluster",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[G5.g5_24xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            C5.c5_xlarge().with_fleet_weighted_capacity(1),
            C5.c5_2xlarge().with_fleet_weighted_capacity(1),
            M5.m5_xlarge().with_fleet_weighted_capacity(1),
            M6g.m6g_xlarge().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=1  # "Minimal capacity for fleet instance is 1 or greater"
    ),
    cluster_tags=cluster_tags,
    emr_release_label=emr_release_label,
    environment=job_environment,
    enable_prometheus_monitoring=True,
    additional_application_configurations=copy.deepcopy(application_configuration),
    cluster_auto_termination_idle_timeout_seconds=cluster_idle_timeout_seconds,
    bootstrap_script_actions=[
        ScriptBootstrapAction(NVIDIA_TOOLKIT_INSTALL_SCRIPT, name='install-nvidia-container-toolkit'),
    ]
)

# Step 1. Copy input ETL data into EMR instance
week_names = ['week1'][::-1]  # reverse as we create task started from the last (current) week
week_sample_ratios = {  # sample spark dataframe files according to partition index (part-XXXXX)
    'week1': 0.5
}

data_sync_tasks = []
for i, week in enumerate(week_names):
    data_sync_delta_days = -7 * i
    sample_ratio = week_sample_ratios[week]

    data_sync_task = EmrJobTask(
        name=f"data-sync-{week}",
        class_name=None,  # shell no class
        executable_path="s3://ttd-build-artefacts/user-embedding/twinDom/scripts/release/datasync.sh",
        command_line_arguments=[
            "--date", "{{ (data_interval_end - macros.timedelta(days=1)).add(days=$days_interval$).strftime('%Y%m%d')}}"
            .replace("$days_interval$",
                     str(data_sync_delta_days)), "--env", env_str, "--target", f"/mnt/dataforgein/{week}/tfrecords", "--sample",
            str(sample_ratio), "--verbose"
        ],
        job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
        timeout_timedelta=timedelta(hours=8),
        cluster_specs=train_cluster.cluster_specs
    )
    data_sync_tasks.append(data_sync_task)

# Step 2. Model training
model_train = DockerRunEmrTask(
    name="model-training",
    docker_run_command=DockerCommandBuilder(
        docker_registry=docker_registry,
        docker_image_name=docker_image_name,
        docker_image_tag=docker_image_tag,
        path_to_app='/opt/application/main.py',
        additional_parameters=[  # docker run parameters
            "--gpus all",
            "-e TF_GPU_THREAD_MODE=gpu_private",
            "-v /mnt/dataforgein:/mnt/dataforgein",  # mount input ETL
            "-v /mnt/twindom:/mnt/twindom"  # mount model output
        ],
        additional_execution_parameters=[  # entrypoint parameters
            "--BATCH_SIZE=1024",
            "--ENVIRONMENT=" + env_str,
        ]
    ),
    timeout_timedelta=timedelta(hours=12),
    cluster_specs=train_cluster.cluster_specs
)

# Step 3. Publish trained model to S3
model_publish = EmrJobTask(
    name="model-publish",
    class_name=None,  # shell no class
    executable_path="s3://ttd-build-artefacts/user-embedding/twinDom/scripts/release/modelpublish.sh",
    command_line_arguments=[
        "--date", run_date_str, "--env", env_str, "--source", "/mnt/twindom", "--modelpath", "models/user-embedding/twindom/model_save",
        "--verbose"
    ],
    job_jar="s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
    configure_cluster_automatically=True,
    timeout_timedelta=timedelta(hours=8),
    cluster_specs=train_cluster.cluster_specs
)

for data_sync_task in data_sync_tasks:
    train_cluster.add_sequential_body_task(data_sync_task)
train_cluster.add_sequential_body_task(model_train)
train_cluster.add_sequential_body_task(model_publish)

final_dag_check = OpTask(
    task_id="final_dag_status", op=FinalDagStatusCheckOperator(dag=dag, trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
)

training_dag >> input_etl_sensor_task
input_etl_sensor_task >> model_train_branch_op_task

model_train_branch_op_task >> model_training_start >> train_cluster >> final_dag_check
model_train_branch_op_task >> final_dag_check
