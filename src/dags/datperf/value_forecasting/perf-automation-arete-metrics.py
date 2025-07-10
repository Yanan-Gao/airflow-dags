from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory, TestEnv

from dags.datperf.value_forecasting.perf_automation_arete_datasets import build_datasets

dag_name = "perf-automation-value-forecasting-arete-metrics"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/datperf/value_pacing_forecasting_model"

docker_image_tag = "latest"

# default values
ENV = TtdEnvFactory.get_from_system()
ARETE_ENV = TtdEnvFactory.get_from_system()
ARETE_VOYAGER_ENV = TestEnv()

# job arguments
PREDICT_DATE = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
MODEL_DATE = '{{ data_interval_start.strftime("%Y-%m-%d") }}'
PUSH_PROM_METRICS_FLAG = '--push_prom_metrics'

####################################################################################################################
# Helper Functions
####################################################################################################################


def get_job_time(execute_date, hour, minute):
    return execute_date.replace(hour=hour, minute=minute, second=0)


####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
value_forecasting_arete_metrics_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 8, 12),
    schedule_interval='0 8 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/CTayDg',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = value_forecasting_arete_metrics_dag.airflow_dag

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
        M5.m5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=128,
)

additional_spark_options_list = [("executor-memory", "45G"), ("executor-cores", "16"),
                                 ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                 ("conf", "spark.driver.memory=50G"), ("conf", "spark.driver.cores=16"),
                                 ("conf", "spark.sql.shuffle.partitions=3000"), ("conf", "spark.driver.maxResultSize=50G"),
                                 ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.shuffle.service.enabled=true"),
                                 ("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25"),
                                 ("conf", "spark.rpc.message.maxSize=2047")]

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
    log_uri="s3://ttd-identity/datapipeline/logs/airflow"
)

####################################################################################################################
# S3 dataset sources
####################################################################################################################

adgroup_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="adgroup",
    success_file=None,
    env_aware=False,
)

campaign_flight_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="campaignflight",
    success_file=None,
    env_aware=False,
)

arete_data_dataset_names = ['arete_adgroups_data', 'arete_train_data']
arete_model_dataset_names = [
    # ARETE
    'arete_prediction',
    'arete_sd_model',
    'arete_avg_model',
    'arete_moving_avg_model'
]

voyager_dataset_names = [
    'arima_normalized', 'avg_normalized', 'cold_start_normalized', 'arete_voyager_output_predictions', 'cold_start_predictions'
]

arete_data_datasets = build_datasets(arete_data_dataset_names, 'data', ENV)

model_datasets = (
    build_datasets(arete_model_dataset_names, 'models', ARETE_ENV) + build_datasets(voyager_dataset_names, 'models', ARETE_VOYAGER_ENV)
)

####################################################################################################################
# S3 dataset sensors
####################################################################################################################

# AdGroup data
adgroup_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='adgroup_data_available',
        datasets=[adgroup_dataset],
        # looks for parquet file in today's folder
        ds_date=f'{PREDICT_DATE} 00:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

# Campaign data
campaign_flight_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='campaign_flight_data_available',
        datasets=[campaign_flight_dataset],
        # looks for parquet file in today's folder
        ds_date=f'{PREDICT_DATE} 00:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

model_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='arete_model_data_available',
        datasets=model_datasets,
        # looks for parquet file in today's folder
        ds_date=f'{MODEL_DATE} 00:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

data_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='arete_data_available',
        datasets=arete_data_datasets,
        # looks for parquet file in today's folder
        ds_date=f'{PREDICT_DATE} 00:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

####################################################################################################################
# steps
####################################################################################################################

arete_metrics_task = PySparkEmrTask(
    name="AreteMetrics",
    entry_point_path="/home/hadoop/app/AreteMetrics.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=additional_spark_options_list,
    command_line_arguments=[
        f"--env={ENV}", f"--arete_env={ARETE_ENV}", f"--model_date={MODEL_DATE}", f"--predict_date={PREDICT_DATE}",
        f"--arete_voyager_env={ARETE_VOYAGER_ENV}", f"{PUSH_PROM_METRICS_FLAG}"
    ],
    timeout_timedelta=timedelta(hours=2),
)

cluster.add_parallel_body_task(arete_metrics_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG dependencies
value_forecasting_arete_metrics_dag >> adgroup_sensor >> cluster
value_forecasting_arete_metrics_dag >> campaign_flight_sensor >> cluster
value_forecasting_arete_metrics_dag >> model_dataset_sensor >> cluster
value_forecasting_arete_metrics_dag >> data_dataset_sensor >> cluster
value_forecasting_arete_metrics_dag >> cluster >> final_dag_status_step
