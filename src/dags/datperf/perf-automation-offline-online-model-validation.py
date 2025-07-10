from datetime import datetime, timedelta

from airflow.sensors.s3_key_sensor import S3KeySensor

from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourExternalDataset, HourGeneratedDataset
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ttdenv import TtdEnvFactory

from dags.datperf.utils.spark_config_utils import get_spark_args

dag_name = "perf-automation-offline-online-model-validation"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/offline-online-model-validation"
docker_image_tag = "latest"

# Environment
ENV = TtdEnvFactory.get_from_system()
PREVIOUS_DATE = '{{ data_interval_start.strftime("%Y-%m-%d") }}'
PREVIOUS_DATE_FORMATTED = '{{ data_interval_start.strftime("%Y%m%d") }}'
CURRENT_DATE = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
PHILO_VERSION = 5
PHILO_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/jars/prod/philo.jar"

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
offline_online_model_validation_dag = TtdDag(
    dag_id=dag_name,
    # want job start date to be 8am UTC, run everyday
    start_date=datetime(2024, 8, 5, hour=8),
    schedule_interval='0 8 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/spaces/EN/pages/58406661/Offline+Online+Model+Validation+TSG',
    retries=1,
    max_active_runs=2,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF'],
    enable_slack_alert=False,
)

dag = offline_online_model_validation_dag.airflow_dag

####################################################################################################################
# S3 dataset sources
####################################################################################################################

# s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=5/prod/global/year=2024/month=07/day=17/
philo_data: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"features/data/philo/v={PHILO_VERSION}/prod",
    data_name="global",
    date_format="year=%Y/month=%m/day=%d",
    version=None,
    success_file=None,
    env_aware=False,
)

# s3://ttd-identity/datapipeline/prod/bidrequestadgroupsampledinternalauctionresultslog/v=1/date=20240718/hour=0/
bidrequest_ial_data = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    data_name="bidrequestadgroupsampledinternalauctionresultslog",
    hour_format="hour={hour}",
    version=1,
    env_aware=True,
    success_file=None
)

# s3://thetradedesk-useast-logs-2/avails/cleansed/2024/06/15/
avails_data: HourExternalDataset = HourExternalDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="avails",
    data_name="cleansed",
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None,
    version=None,
)

####################################################################################################################
# S3 key sensors
####################################################################################################################

# philo model
# s3://thetradedesk-mlplatform-us-east-1/models/prod/philo/v=5/global/combined/model_files/202408010453/
philo_model_sensor = OpTask(
    op=S3KeySensor(
        task_id='philo_model_available',
        poke_interval=60 * 10,
        timeout=60 * 60 * 1,  # wait up to 1 hour before failing
        bucket_key=f'models/prod/philo/v={PHILO_VERSION}/global/combined/model_files/{PREVIOUS_DATE_FORMATTED}????/_SUCCESS',
        bucket_name='thetradedesk-mlplatform-us-east-1',
        wildcard_match=True,
        dag=dag
    )
)

# philo input data
philo_data_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='philo_data_available',
        datasets=[philo_data],
        # date of datasets to check for existence
        ds_date=f'{PREVIOUS_DATE} 08:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
        raise_exception=True,
    )
)

# bidrequest adgroup sampled internal auction log data
bidrequest_ial_data_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='bidrequest_ial_data_available',
        datasets=[bidrequest_ial_data.with_check_type("hour")],
        # date of datasets to check for existence
        ds_date=f'{PREVIOUS_DATE} 23:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
        raise_exception=True,
    )
)

# avails data
avails_data_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='avails_data_available',
        datasets=[avails_data],
        # date of datasets to check for existence
        ds_date=f'{PREVIOUS_DATE} 08:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
        raise_exception=True,
    )
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(16)],
    on_demand_weighted_capacity=512,
)

cluster = DockerEmrClusterTask(
    name=dag_name,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri="s3://ttd-identity/datapipeline/logs/airflow",
)

####################################################################################################################
# steps
####################################################################################################################

# Instance configuration
instance_type = M5.m5_4xlarge()
on_demand_weighted_capacity = 512

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)
spark_args = get_spark_args(cluster_params)
spark_args.append(("jars", PHILO_JAR))

spark_options_list = spark_args

offline_online_model_validation_task = PySparkEmrTask(
    name="OfflineOnlineModelValidation",
    entry_point_path="/home/hadoop/app/main.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=spark_options_list,
    command_line_arguments=[f"--env={ENV}", f"--date={CURRENT_DATE}"],
    timeout_timedelta=timedelta(hours=8),
)
cluster.add_parallel_body_task(offline_online_model_validation_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG dependencies
offline_online_model_validation_dag >> philo_model_sensor >> cluster
offline_online_model_validation_dag >> bidrequest_ial_data_sensor >> cluster
offline_online_model_validation_dag >> philo_data_sensor >> cluster
offline_online_model_validation_dag >> avails_data_sensor >> cluster
offline_online_model_validation_dag >> cluster >> final_dag_status_step
