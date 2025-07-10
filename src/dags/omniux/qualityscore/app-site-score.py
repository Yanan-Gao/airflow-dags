from datetime import datetime

from airflow.operators.python import PythonOperator

from datasources.sources import ctv_datasources
from dags.omniux.utils import get_jar_file_path
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.cloud_provider import CloudProviders
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

# Dag Configs
job_name = "tvqi-site-title-score"
job_start_date = datetime(2023, 1, 22, 7, 0)
job_schedule_interval = "@weekly"
job_environment = TtdEnvFactory.get_from_system()

# LWDB configs
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"

# Date and Partition configs
run_date = '{{ data_interval_start.strftime(\"%Y-%m-%d\") }}'
log_start_time = '{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}'

env = TtdEnvFactory.get_from_system()
logworkflow_connection_open_gate = logworkflow_connection if env == TtdEnvFactory.prod \
    else logworkflow_sandbox_connection

# EMR configs
instance_type = R6g.r6g_12xlarge()
cluster_params = instance_type.calc_cluster_params(instances=10, parallelism_factor=5)

application_configuration = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.vmem-check-enabled": "false"
    }
}]

spark_options_list = [
    ("executor-memory", '65000m'),
    ("executor-cores", f'{cluster_params.executor_cores}'),
    ("conf", "num-executors={cluster_params.executor_instances}"),
    ("conf", f"spark.executor.memoryOverhead={cluster_params.executor_memory_overhead_with_unit}"),
    ("conf", f"spark.driver.memoryOverhead={cluster_params.executor_memory_overhead_with_unit}"),
    ("conf", f"spark.driver.memory={cluster_params.executor_memory_with_unit}"),
    ("conf", "spark.default.parallelism=30000"),
    ("conf", "spark.sql.shuffle.partitions=30000"),
    ("conf", "spark.speculation=false"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
    ("conf", "spark.driver.maxResultSize=0"),
    ("conf", "spark.sql.shuffle.partitions=100"),
    ("conf", "spark.default.parallelism=100"),
]

java_option_list = [("date", run_date), ("lookBackDays", 7), ("cumSiteSpendCutoff", 0.999), ("cumTitleSpendCutoff", 0.90),
                    ("rootPath", "s3://ttd-ctv/"), ("supportedIMDBRegions", "US,DE,CA,GB,IN,FR,ES,IT"), ("supportedWhipCountries", "US"),
                    ("minDistinctTitles", 100), ("recordDateRetention", 30)]

###########################################
#   DAG Setup
###########################################

# DAG

site_score_weekly_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    tags=[OMNIUX.team.name, "tvqi"],
    slack_channel=OMNIUX.omniux().alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    enable_slack_alert=True,
    run_only_latest=True
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
        R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=10
)
cluster_task = EmrClusterTask(
    name=f"{job_name}-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": OMNIUX.omniux().jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)
site_score_step = EmrJobTask(
    name=f"{job_name}-task",
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.tvqi.SiteAndTitleVideoQualityScoreWeeklyJob",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=java_option_list,
    additional_args_option_pairs_list=spark_options_list
)
cluster_task.add_parallel_body_task(site_score_step)

dag = site_score_weekly_dag.airflow_dag

###########################################
#   Check Dependencies
###########################################

# RTB Platform Reports
platformreport_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

lateplatformreport_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportLateDataPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)
data_recency_op = OpTask(
    op=DatasetRecencyOperator(
        task_id="data_recency_check",
        dag=dag,
        datasets_input=[
            ctv_datasources.CtvDatasources.app_site_map, ctv_datasources.CtvDatasources.imdb,
            ctv_datasources.CtvDatasources.whip_internal("Content")
        ],
        cloud_provider=CloudProviders.aws,
        lookback_days=14
    )
)
data_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='data_availability_check',
        poke_interval=60 * 10,
        timeout=60 * 60 * 18,  # wait up to 18 hours
        ds_date="{{ (data_interval_start - macros.timedelta(days=1)).strftime(\"%Y-%m-%d 00:00:00\") }}",
        datasets=[platformreport_dataset, lateplatformreport_dataset]
    )
)

# Opens gate for weekly DataMover task to import data into ProvDB from S3
logworkflow_open_add_site_score_gate = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'gatingType': 30003,  # dbo.fn_Enum_GatingType_ImportSiteVideoQualityScore()
                'grain': 100003,  # dbo.fn_Enum_TaskBatchGrain_Weekly()
                'dateTimeToOpen': log_start_time  # open gate for this hour
            }
        },
        task_id="logworkflow_open_add_site_score_gate",
    )
)

# Flow

site_score_weekly_dag >> cluster_task
data_sensor >> cluster_task
data_recency_op >> cluster_task
cluster_task >> logworkflow_open_add_site_score_gate
