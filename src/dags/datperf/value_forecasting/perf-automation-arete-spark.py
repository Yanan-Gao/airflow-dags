from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.compute_optimized.c5a import C5a
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "perf-automation-value-forecasting-arete-spark"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/datperf/value_pacing_forecasting_model"

docker_image_tag = "latest"

# Environment
ENV = TtdEnvFactory.get_from_system()
WEIGHT_METHOD = 'trapezoid'
WEIGHT_FACTOR = 10
MAG_MODEL = 'piecewise'
PREDICT_DATE = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'

# Temporary campaign exclusion list
TEMP_CAMPAIGN_EXCLUSION_LIST = 'divydey'

###############################################################################
# DAG
###############################################################################

# The top-level dag
value_forecasting_arete_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 6, 13),
    schedule_interval='0 4 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/CTayDg',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    enable_slack_alert=False,
    tags=["DATPERF"]
)

dag = value_forecasting_arete_dag.airflow_dag

###############################################################################
# S3 dataset sources
###############################################################################

vc_budget_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportProductionVolumeControlBudgetSnapshot",
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

###############################################################################
# S3 dataset sensors
###############################################################################

# Budget data
vc_budget_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='budget_data_available',
        datasets=[vc_budget_dataset],
        # looks for success file in hour 23 of yesterday's folder
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
        raise_exception=True,
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

###############################################################################
# clusters
###############################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5d.m5d_4xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C5a.c5ad_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(16),
        C5a.c5ad_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
        C5a.c5ad_16xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(64)
    ],
    on_demand_weighted_capacity=1024,
)

arete_cluster = DockerEmrClusterTask(
    name=dag_name,
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": DATPERF.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri="s3://ttd-identity/datapipeline/logs/airflow",
)

###############################################################################
# steps
###############################################################################

spark_options_list = [
    ("executor-memory", "19G"),
    ("executor-cores", "16"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=18G"),
    ("conf", "spark.sql.shuffle.partitions=3000"),
    ("conf", "spark.driver.maxResultSize=24G"),
    ("conf", "spark.memory.storageFraction=0.25"),
]

arete_model_task = PySparkEmrTask(
    name="AreteModel",
    entry_point_path="/home/hadoop/app/Arete.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=spark_options_list,
    command_line_arguments=[
        f"--env={ENV}", f"--predict_date={PREDICT_DATE}", f"--weight_method={WEIGHT_METHOD}", f"--weight_factor={WEIGHT_FACTOR}",
        f"--mag_model={MAG_MODEL}", "--num_of_bucket=1", "--bucket=0", "--spark=True",
        f"--campaign_exclusion_list={TEMP_CAMPAIGN_EXCLUSION_LIST}"
    ],
    timeout_timedelta=timedelta(hours=4),
)

arete_cluster.add_parallel_body_task(arete_model_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
value_forecasting_arete_dag >> vc_budget_sensor >> campaign_flight_sensor \
    >> arete_cluster
