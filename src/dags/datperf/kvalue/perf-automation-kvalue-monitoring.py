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

dag_name = "perf-automation-kvalue-monitoring"

# Docker information
docker_registry = "internal.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/kvalue"
docker_image_tag = "latest"

# Environment
ENV = "prod"
DATA_AVAILABLE_DATE = '{{ data_interval_start.strftime("%Y-%m-%d") }}'
DATA_AVAILABLE_DATE_FORMATTED = '{{ data_interval_start.strftime("%Y%m%d") }}'
IAL_DATETIME = '{{ (data_interval_start + macros.timedelta(hours=-1)).strftime("%Y%m%d %H:%M:%S") }}'
USE_EXCLUSIONS = True
TEST_BUCKETS_DA = 1001
USE_EXCLUSIONS_SOLIMAR = True
TEST_BUCKETS_SOLIMAR = 1001
USE_QA_GOALS = True
EXCLUSIONS_TEST_PATH = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/k_value/ue_test_adgroups/prism_solimar_adgroups_with_excess_potential.csv"
OUTPUT_PATH = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/k_value/experiment_adgroups/prod"
QA_TEST_PATH = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/k_value/qa_test_adgroups/qa_tests.csv"
ADG_REVERT_PATH = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/k_value/revert_adgroups/revert_adgs.csv"
ADV_REVERT_PATH = "s3://thetradedesk-mlplatform-us-east-1/libs/philo/k_value/revert_advertisers/revert_advs.csv"
####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
kvalue_dag = TtdDag(
    dag_id=dag_name,
    # want job start date to be 8am UTC, run everyday
    start_date=datetime(2024, 8, 27),
    schedule_interval='0 5-12/2,17-23/2 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/lUFZDw',
    retries=1,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    tags=['DATPERF', "KValue"],
    enable_slack_alert=False
)

dag = kvalue_dag.airflow_dag

####################################################################################################################
# clusters
####################################################################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(16)],
    on_demand_weighted_capacity=80,
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
on_demand_weighted_capacity = 80

# Spark configuration
cluster_params = instance_type.calc_cluster_params(instances=on_demand_weighted_capacity, parallelism_factor=10)
spark_args = get_spark_args(cluster_params)

spark_options_list = spark_args

kvalue_task = PySparkEmrTask(
    name="KValueMonitoring",
    entry_point_path="/home/hadoop/app/monitoring.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    additional_args_option_pairs_list=[
        ("executor-memory", "20G"),
        ("executor-cores", "4"),
        ("conf", "spark.driver.memory=20G"),
        ("conf", "spark.driver.maxResultSize=4G"),
    ],
    command_line_arguments=[
        f"--env={ENV}", f"--dist_date={DATA_AVAILABLE_DATE_FORMATTED}", f"--adgroup_date={DATA_AVAILABLE_DATE_FORMATTED}",
        f"--use_qa_goals={USE_QA_GOALS}", f"--use_exclusions={USE_EXCLUSIONS}", f"--qa_test_path={QA_TEST_PATH}",
        f"--exclusions_test_path={EXCLUSIONS_TEST_PATH}", f"--adv_revert_path={ADV_REVERT_PATH}", f"--adg_revert_path={ADG_REVERT_PATH}",
        f"--num_test_buckets={TEST_BUCKETS_DA}", f"--num_test_buckets_solimar={TEST_BUCKETS_SOLIMAR}",
        f"--use_exclusions_solimar={USE_EXCLUSIONS_SOLIMAR}"
    ],
    timeout_timedelta=timedelta(hours=2),
)
cluster.add_parallel_body_task(kvalue_task)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# DAG dependencies
kvalue_dag >> cluster >> final_dag_status_step
