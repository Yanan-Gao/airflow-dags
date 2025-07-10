from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "perf-automation-arete-voyager-cold-start"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/datperf/value_pacing_forecasting_model"

docker_image_tag = "latest"

# default values
ENV = 'test'
ARETE_ENV = TtdEnvFactory.get_from_system()

# job arguments
PREDICT_DATE = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
MODEL_NAME = 'cold_start'
ARETE_PREDICT_FLAG = '--no-if_arete'
COLD_START_PREDICT_FLAG = '--if_cold_start'
SHIFT_UTIL_PREDICT_FLAG = '--no-if_shift_forecast'
AION_PREDICT_FLAG = '--no-if_aion'

AION_ADGROUP_PATH = "s3://thetradedesk-mlplatform-us-east-1/models/test/value-pacing-forecasting/aion_predictions/"
####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
# Cold start forecasts are usually available around 7:00 AM UTC.
arete_voyager_cold_start_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 8, 13),
    schedule_interval='30 7 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/CTayDg',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF'],
    enable_slack_alert=False
)

dag = arete_voyager_cold_start_dag.airflow_dag

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_16xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(64),
        R5.r5_24xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(96),
    ],
    on_demand_weighted_capacity=384,
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

cold_start_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/dev",
    data_name="arete_cold_start_from_aion",
    env_aware=False,
    version=None
)

arete_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="value-pacing-forecasting/arete_adgroups_data",
    env_aware=True,
    version=None
)

budget_adgroup_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportProductionAdGroupBudgetSnapshot",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

campaign_flight_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="campaignflight",
    success_file=None,
    env_aware=False,
)

provisioning_adgroup_dataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="adgroup",
    success_file=None,
    env_aware=False,
)

####################################################################################################################
# S3 dataset sensors
####################################################################################################################

# Cold Start predictions
cold_start_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='cold_start_data_available',
        # looks for success file in hour 23 of yesterday's folder
        ds_date="{{ data_interval_end.to_datetime_string() }}",
        poke_interval=60 * 10,
        # wait up to 2 hours
        timeout=60 * 60 * 2,
        raise_exception=True,
        datasets=[cold_start_dataset],
    )
)

# Arete - AdGroup data
arete_adgroup_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='arete_adgroup_data_available',
        # looks for success file in hour 23 of yesterday's folder
        ds_date="{{ data_interval_end.to_datetime_string() }}",
        poke_interval=60 * 10,
        # wait up to 2 hours
        timeout=60 * 60 * 2,
        raise_exception=True,
        datasets=[arete_dataset],
    )
)

# Budget - AdGroup data
budget_adgroup_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_adgroup_data_available',
        datasets=[budget_adgroup_dataset],
        # looks for success file in hour 23 of yesterday's folder
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
        raise_exception=True,
    )
)

# Provisioning - Campaign data
provisioning_campaign_flight_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='provisioning_campaign_flight_data_available',
        datasets=[campaign_flight_dataset],
        # looks for parquet file in today's folder
        ds_date=f'{PREDICT_DATE} 00:00:00',
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

# Provisioning - AdGroup data
provisioning_adgroup_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='provisioning_adgroup_data_available',
        datasets=[provisioning_adgroup_dataset],
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

cold_start_task = PySparkEmrTask(
    name="AreteVoyager",
    entry_point_path="/home/hadoop/app/AreteVoyager.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=additional_spark_options_list,
    command_line_arguments=[
        f"--env={ENV}", f"--arete_env={ARETE_ENV}", f"--model_date={PREDICT_DATE}", f"{ARETE_PREDICT_FLAG}", f"{COLD_START_PREDICT_FLAG}",
        f"{SHIFT_UTIL_PREDICT_FLAG}", f"{AION_PREDICT_FLAG}", f"--aion_adgroup_path={AION_ADGROUP_PATH}", f"--model_name={MODEL_NAME}"
    ],
    timeout_timedelta=timedelta(hours=2),
)

cluster.add_parallel_body_task(cold_start_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
arete_voyager_cold_start_dag >> budget_adgroup_sensor >> cluster
arete_voyager_cold_start_dag >> provisioning_campaign_flight_sensor >> cluster
arete_voyager_cold_start_dag >> provisioning_adgroup_sensor >> cluster
arete_voyager_cold_start_dag >> arete_adgroup_sensor >> cluster
arete_voyager_cold_start_dag >> cold_start_sensor >> cluster
