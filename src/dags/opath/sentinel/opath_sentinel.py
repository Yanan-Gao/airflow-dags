from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

from ttd.slack.slack_groups import OPATH
from datetime import timedelta, datetime
from ttd.ttdenv import TtdEnvFactory
from ttd.interop.vertica_import_operators import VerticaImportFromCloud, LogTypeFrequency

job_start_date = datetime(2024, 9, 27)
job_schedule_interval = "0 1 * * *"
max_retries: int = 3
retry_delay: timedelta = timedelta(minutes=20)

job_environment = TtdEnvFactory.get_from_system()
should_alert = job_environment == TtdEnvFactory.prod

dag = TtdDag(
    dag_id="opath_sentinel",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=OPATH.team.alarm_channel,
    slack_tags=OPATH.team.jira_team,
    retries=max_retries,
    retry_delay=retry_delay,
    tags=[OPATH.team.jira_team, "Sentinel"],
    enable_slack_alert=should_alert
)


def create_cluster(cluster_name: str, core_fleet_capacity: int):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            M5.m5_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
        ],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32)
        ],
        on_demand_weighted_capacity=core_fleet_capacity
    )
    additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}
    emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2_1
    aws_region = "us-east-1"

    return EmrClusterTask(
        name="OPATHSentinel-Cluster-" + cluster_name,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        cluster_tags={"Team": OPATH.team.jira_team},
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        additional_application_configurations=[additional_application_configurations],
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        environment=job_environment,
        emr_release_label=emr_release_label,
        region_name=aws_region
    )


date_macro = """{{ data_interval_start.to_date_string() }}"""
vertica_import_date = '{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}'


def create_job_task(job_name: str, job_class_name: str, core_fleet_capacity: int):
    jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-openpath-assembly.jar"

    eldorado_config_option_pairs_list = [("date", date_macro), ("env", job_environment.execution_env)]

    spark_options_list = [("executor-memory", "15G"), ("executor-cores", "10"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=40G"),
                          ("conf", "spark.driver.maxResultSize=6G"), ("conf", f"spark.sql.shuffle.partitions={core_fleet_capacity}")]

    return EmrJobTask(
        name=job_name + "-Task",
        class_name=job_class_name,
        executable_path=jar_path,
        configure_cluster_automatically=True,
        additional_args_option_pairs_list=spark_options_list,
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
    )


SINCERA_PARSE_CORE_FLEET_CAPACITY = 32
QUERY_AVAILS_CORE_FLEET_CAPACITY = 2048
COMBINE_DATASETS_CORE_FLEET_CAPACITY = 32

sincera_parse_cluster_task = create_cluster(cluster_name="SinceraParse", core_fleet_capacity=SINCERA_PARSE_CORE_FLEET_CAPACITY)
query_avails_cluster_task = create_cluster(cluster_name="QueryAvails", core_fleet_capacity=QUERY_AVAILS_CORE_FLEET_CAPACITY)
combine_data_cluster_task = create_cluster(cluster_name="CombineData", core_fleet_capacity=COMBINE_DATASETS_CORE_FLEET_CAPACITY)

job_task_sincera_parse = create_job_task(
    job_name="SinceraParse",
    job_class_name="jobs.openpath.sentinel.transformers.SentinelSinceraParse",
    core_fleet_capacity=SINCERA_PARSE_CORE_FLEET_CAPACITY
)

job_task_query_avails = create_job_task(
    job_name="QueryAvails",
    job_class_name="jobs.openpath.sentinel.transformers.UnsampledAvailsQuerying",
    core_fleet_capacity=QUERY_AVAILS_CORE_FLEET_CAPACITY
)

job_task_combine_data = create_job_task(
    job_name="CombineData",
    job_class_name="jobs.openpath.sentinel.transformers.CombineData",
    core_fleet_capacity=COMBINE_DATASETS_CORE_FLEET_CAPACITY
)

sincera_parse_cluster_task.add_sequential_body_task(job_task_sincera_parse)
query_avails_cluster_task.add_sequential_body_task(job_task_query_avails)
combine_data_cluster_task.add_sequential_body_task(job_task_combine_data)

dag >> sincera_parse_cluster_task
sincera_parse_cluster_task >> query_avails_cluster_task
query_avails_cluster_task >> combine_data_cluster_task

OPENPATH_SENTINEL_GATING_TYPE_ID = 2000520  # OpenPathSentinelImport

open_lw_gate_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="OpenPathSentinalDataImport_OpenLWGate",
    gating_type_id=OPENPATH_SENTINEL_GATING_TYPE_ID,
    log_type_id=LogTypeFrequency.DAILY.value,
    log_start_time=vertica_import_date,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

combine_data_cluster_task >> open_lw_gate_task

adag = dag.airflow_dag
