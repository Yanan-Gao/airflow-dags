from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF

dag_name = "perf-automation-value-forecasting-arete"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = ("ttd-base/datperf/value_pacing_forecasting_model")

docker_image_tag = "latest"
num_of_bucket = 10

# Environment
ENV = 'test'
WEIGHT_METHOD = 'trapezoid'
WEIGHT_FACTOR = 10
MAG_MODEL = 'piecewise'
PREDICT_DATE = '{{ (data_interval_start + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
EXPERIMENT = 'arete-pandas'

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
value_forecasting_arete_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 5, 2),
    schedule_interval='0 4 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/CTayDg',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    enable_slack_alert=False,
    default_args={"owner": "DATPERF"},
    tags=["DATPERF"],
)

dag = value_forecasting_arete_dag.airflow_dag

####################################################################################################################
# S3 dataset sources
####################################################################################################################

budget_volume_control: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportProductionVolumeControlBudgetSnapshot",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

provisioning_campaign_flight: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="campaignflight",
    date_format="%Y-%m-%d",
    version=1,
    success_file=None,
    env_aware=False
)

####################################################################################################################
# S3 dataset sensors
####################################################################################################################

# Budget data
productionVolumeControlBudgetSnapshot_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='budget_data_available',
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,  # wait up to 6 hours
    raise_exception=True,
    # looks for success file in hour 23
    ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
    datasets=[budget_volume_control]
)

# Campaign data
campaignFlight_sensor = DatasetCheckSensor(
    dag=dag,
    task_id='campaign_flight_data_available',
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,  # wait up to 6 hours
    # looks for parquet file in current date folder
    ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
    datasets=[budget_volume_control]
)

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5.r5_24xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(96)],
    on_demand_weighted_capacity=96,
)

spark_options_list = [("executor-memory", "200G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=110G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=10000"),
                      ("conf", "spark.driver.maxResultSize=50G"), ("conf", "spark.dynamicAllocation.enabled=true"),
                      ("conf", "spark.memory.fraction=0.7"), ("conf", "spark.memory.storageFraction=0.25"),
                      ("conf", "spark.rpc.message.maxSize=2047")]

clusters = []
for bucket in range(num_of_bucket):
    cluster = DockerEmrClusterTask(
        name=dag_name + str(bucket),
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
    clusters.append(cluster)

####################################################################################################################
# steps
####################################################################################################################

for bucket in range(num_of_bucket):
    arete_model_task = PySparkEmrTask(
        name="AreteModel",
        entry_point_path="/home/hadoop/app/Arete.py",
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        additional_args_option_pairs_list=spark_options_list,
        command_line_arguments=[
            f"--env={ENV}",
            f"--predict_date={PREDICT_DATE}",
            f"--weight_method={WEIGHT_METHOD}",
            f"--weight_factor={WEIGHT_FACTOR}",
            f"--mag_model={MAG_MODEL}",
            f"--experiment={EXPERIMENT}",
            f"--num_of_bucket={num_of_bucket}",
            f"--bucket={bucket}",
        ],
        timeout_timedelta=timedelta(hours=2)
    )

    clusters[bucket].add_parallel_body_task(arete_model_task)

# The final combine output task uses its own cluster
combine_output_cluster = DockerEmrClusterTask(
    name=dag_name + 'CombineOutput',
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label="emr-6.7.0",
    log_uri="s3://ttd-identity/datapipeline/logs/airflow"
)

combine_output_task = PySparkEmrTask(
    name="CombineAreteBuckets",
    entry_point_path="/home/hadoop/app/CombineAreteBuckets.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=spark_options_list,
    command_line_arguments=[
        f"--env={ENV}",
        f"--predict_date={PREDICT_DATE}",
        f"--weight_method={WEIGHT_METHOD}",
        f"--weight_factor={WEIGHT_FACTOR}",
        f"--mag_model={MAG_MODEL}",
        f"--experiment={EXPERIMENT}",
        f"--num_of_bucket={num_of_bucket}",
    ],
    timeout_timedelta=timedelta(hours=2)
)

combine_output_cluster.add_parallel_body_task(combine_output_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
for i in range(num_of_bucket):
    value_forecasting_arete_dag >> clusters[i]
    [productionVolumeControlBudgetSnapshot_sensor, campaignFlight_sensor] >> clusters[i].first_airflow_op()
    clusters[i].last_airflow_op()
    clusters[i] >> combine_output_cluster

combine_output_cluster.first_airflow_op()
combine_output_cluster.last_airflow_op() >> final_dag_status_step
