from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from datetime import datetime, timedelta
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.datperf.datasets import geronimo_dataset, adgroup_dataset, kongming_oos_dataset, kongming_oos_optin_dataset
from ttd.slack.slack_groups import DATPERF
from ttd.docker import (
    DockerEmrClusterTask,
    PySparkEmrTask,
)

# Job configuration
job_schedule_interval_charging = "0 4 * * *"
job_start_date = datetime(2024, 10, 16)
exec_date = '{{ data_interval_start.strftime("%Y%m%d") }}'

# Docker configuration
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/user_data"
docker_image_tag = "latest"

# DAG definition
pricing_dag = TtdDag(
    dag_id="perf-automation-valgo-userdata-pricing",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval_charging,
    dag_tsg='',
    retries=1,
    max_active_runs=1,
    enable_slack_alert=False,
    tags=['DATPERF']
)

dag = pricing_dag.airflow_dag

upstream_etl_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="upstream_etl_available",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start - macros.timedelta(days=1)).strftime("%Y-%m-%d 23:00:00") }}',
    datasets=[geronimo_dataset, adgroup_dataset],
)

mpm_data_sensor = DatasetCheckSensor(
    dag=dag,
    task_id="mpm_data_available",
    poke_interval=60 * 10,
    timeout=60 * 60 * 6,
    ds_date='{{ (data_interval_start - macros.timedelta(days=4)).strftime("%Y-%m-%d 00:00:00") }}',
    datasets=[kongming_oos_dataset, kongming_oos_optin_dataset]
)

# Instance types
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

pricing_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
    ], on_demand_weighted_capacity=1
)

# Cluster configuration
pyspark_cluster = DockerEmrClusterTask(
    name="valgo-userdata-segment-pricing-cluster",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/app/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        "Team": DATPERF.team.jira_team,
    },
    core_fleet_instance_type_configs=pricing_core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri="s3://ttd-identity/datapipeline/logs/airflow",
)

# Pricing task
pricing_ischargeabeflag = PySparkEmrTask(
    name="pricing-ischargeableflag",
    entry_point_path="/home/hadoop/app/pricing_main.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    eldorado_config_option_pairs_list=[('date', "{{ds}}")],
    additional_args_option_pairs_list=[
        ("num-executors", "19"),
        ("executor-memory", "40G"),
        ("executor-cores", "16"),
        ("conf", "spark.driver.memory=20G"),
        ("conf", "spark.driver.maxResultSize=4G"),
    ],
    command_line_arguments=["--delay=3", f"--date={exec_date}", "--kpitype=CPA", "--lookback=14"],
    timeout_timedelta=timedelta(hours=2),
)

pyspark_cluster.add_parallel_body_task(pricing_ischargeabeflag)
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
pricing_dag >> pyspark_cluster
upstream_etl_sensor >> mpm_data_sensor >> pyspark_cluster.first_airflow_op()
pyspark_cluster.last_airflow_op() >> final_dag_status_step
