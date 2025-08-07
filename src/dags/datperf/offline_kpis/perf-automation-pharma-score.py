from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.docker import PySparkEmrTask, DockerEmrClusterTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5

from dags.datperf.utils.spark_config_utils import get_spark_args
from ttd.ttdenv import TtdEnvFactory

dag_name = "perf-automation-pharma-goal-kpi-score"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/offlinekpis"
docker_image_tag = "latest"

ENV = TtdEnvFactory.get_from_system().execution_env
TEST_PATH = "s3://thetradedesk-mlplatform-us-east-1/env=prod/metadata/pharmagoalkpiscores/campaigntests/offline-kpi-test-campaigns.csv"
CARRYOVER = True
DATA_AVAILABLE_DATE = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
DATA_AVAILABLE_DATE_FORMATTED = '{{ data_interval_end.strftime("%Y%m%d") }}'
run_only_latest = None
####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
score_dag = TtdDag(
    dag_id=dag_name,
    # job runs 0 UTC everyday, probably doesn't need to be that often but just to start?
    start_date=datetime(2025, 6, 1, hour=8),
    schedule_interval=timedelta(days=1),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/lUFZDw',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF'],
    enable_slack_alert=False,
    run_only_latest=run_only_latest
)

dag = score_dag.airflow_dag

# Instance configuration
instance_type = M5.m5_4xlarge()
on_demand_weighted_capacity = 640

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)
spark_args = get_spark_args(cluster_params)

spark_options_list = spark_args

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(16)],
    on_demand_weighted_capacity=on_demand_weighted_capacity,
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

score_task = PySparkEmrTask(
    name="PharmaScores",
    entry_point_path="/home/hadoop/app/main.py",
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
    command_line_arguments=[f"--env={ENV}", f"--cur_date={DATA_AVAILABLE_DATE_FORMATTED}", f"--carryover={CARRYOVER}"],
    timeout_timedelta=timedelta(hours=2),
)
cluster.add_parallel_body_task(score_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG dependencies
score_dag >> cluster >> final_dag_status_step
