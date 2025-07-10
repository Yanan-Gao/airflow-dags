from datetime import datetime, timedelta

from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.docker import DockerEmrClusterTask, PySparkEmrTask
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import DATPERF
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "perf-automation-koa-sd"

exec_date = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
koa_sd_jar = 's3://thetradedesk-mlplatform-us-east-1/libs/koa-three-dot-five/data-etl-scala/jars/release/koav3dot5.jar'

# Docker information
docker_registry = "production.docker.adsrvr.org"
docker_image_name = "ttd-base/scrum-datperf/koa_three_dot_five"
docker_image_tag = "latest"

# Environment
env = TtdEnvFactory.get_from_system()

# Rollout to all reach & incremental reach adgroups
enable_reach_adgroups = 'true'
reach_perct = 1.0

###############################################################################
# DAG
###############################################################################

koa_sd_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 12, 8),
    # This job runs everyday at 02:00 UTC
    schedule_interval="0 2 * * *",
    dag_tsg='',
    retries=1,
    max_active_runs=1,
    enable_slack_alert=False,
    tags=['DATPERF'],
)

dag = koa_sd_dag.airflow_dag

###############################################################################
# S3 sensors
###############################################################################

platform_report_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

platform_report_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='platform_report_data_available',
        datasets=[platform_report_dataset],
        # looks for success file in hour 23
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 23:00:00\") }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6,
    )
)

###############################################################################
# Clusters
###############################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_4xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

etl_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
        R5.r5_16xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(4),
    ],
    on_demand_weighted_capacity=56,
)

pyspark_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=12,
)

bidlist_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2)
    ],
    on_demand_weighted_capacity=6,
)

etl_cluster = EmrClusterTask(
    name="koa-sd-etl-cluster",
    cluster_tags={"Team": DATPERF.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=etl_core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
)

pyspark_model_cluster = DockerEmrClusterTask(
    name="koa-sd-pyspark_cluster",
    cluster_tags={"Team": DATPERF.team.jira_team},
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    entrypoint_in_image="lib/koa_job_pyspark/",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=pyspark_core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    log_uri="s3://ttd-identity/datapipeline/logs/airflow",
    cluster_auto_terminates=False,
)

bidlist_cluster = EmrClusterTask(
    name="koa-sd-output-cluster",
    cluster_tags={"Team": DATPERF.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=bidlist_core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
)

###############################################################################
# Steps
###############################################################################

etl_spark_options = [
    ("executor-memory", "100G"),
    ("executor-cores", "15"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=110G"),
    ("conf", "spark.driver.cores=15"),
    ("conf", "spark.sql.shuffle.partitions=10000"),
    ("conf", "spark.driver.maxResultSize=20G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.memory.storageFraction=0.25"),
]

koasd_spark_options = [
    ("executor-memory", "100G"),
    ("executor-cores", "16"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=100G"),
    ("conf", "spark.driver.cores=16"),
    ("conf", "spark.sql.shuffle.partitions=200"),
    ("conf", "spark.default.parallelism=200"),
    ("conf", "spark.driver.maxResultSize=6G"),
    ("conf", "spark.executor.memoryOverhead=6G"),
]

# ttd.env is automatically passed to driver-java-options

etl_step = EmrJobTask(
    name="etl-by-dimension",
    class_name="com.thetradedesk.koathreedotfive.jobs.EtlByDimension",
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=etl_spark_options,
    eldorado_config_option_pairs_list=[("date", exec_date), ("enableReachKpiCalculation", enable_reach_adgroups),
                                       ("enableReachAdGroupsPerct", reach_perct)],
    executable_path=koa_sd_jar,
)

etl_cluster.add_parallel_body_task(etl_step)

pyspark_model_step = PySparkEmrTask(
    name="koa_sd_model",
    entry_point_path="/home/hadoop/app/koa3dot5.py",
    image_name=docker_image_name,
    image_tag=docker_image_tag,
    docker_registry=docker_registry,
    # Install the KoaSD jar so pyspark can use the wrapped datasets
    additional_args_option_pairs_list=koasd_spark_options + [
        ("jars", koa_sd_jar),
    ],
    command_line_arguments=[
        f"--env={env}",
        f"--date={exec_date}",
    ],
    timeout_timedelta=timedelta(hours=2),
)

pyspark_model_cluster.add_parallel_body_task(pyspark_model_step)

bidlist_step = EmrJobTask(
    name="koa-sd-bidlist",
    class_name="com.thetradedesk.koathreedotfive.jobs.BidListifier",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ("date", exec_date),
    ],
    # BidListifier is having issues with heartbeats from workers
    additional_args_option_pairs_list=koasd_spark_options + [
        ("conf", "spark.network.timeout=1200s"),
    ],
    executable_path=koa_sd_jar,
)

bidlist_cluster.add_parallel_body_task(bidlist_step)

pacing_adjustment_step = EmrJobTask(
    name="koa-sd-v3-pacing-adjustment",
    class_name="com.thetradedesk.koathreedotfive.jobs.Koav3PacingAdjuster",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ("date", exec_date),
    ],
    additional_args_option_pairs_list=koasd_spark_options,
    executable_path=koa_sd_jar,
)

bidlist_cluster.add_parallel_body_task(pacing_adjustment_step)

version_tester_step = EmrJobTask(
    name="koa-sd-version-tester",
    class_name="com.thetradedesk.koathreedotfive.jobs.VersionTester",
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[
        ("date", exec_date),
    ],
    additional_args_option_pairs_list=koasd_spark_options,
    executable_path=koa_sd_jar,
)

bidlist_cluster.add_sequential_body_task(version_tester_step)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = FinalDagStatusCheckOperator(dag=dag)

# DAG dependencies
koa_sd_dag >> platform_report_sensor >> etl_cluster
etl_step >> pyspark_model_cluster
pyspark_model_step >> bidlist_cluster
bidlist_cluster.last_airflow_op() >> final_dag_status_step
