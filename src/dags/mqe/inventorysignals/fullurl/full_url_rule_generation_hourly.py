import random
from datetime import datetime, timedelta
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.interop.vertica_import_operators import LogTypeFrequency
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack import slack_groups
from ttd.ttdenv import TtdEnvFactory

# DAG config
job_start_date = datetime(2025, 4, 20)
job_schedule_interval = "0 * * * *"
max_retries = 3
retry_delay = timedelta(minutes=random.randint(10, 60))
job_environment = TtdEnvFactory.get_from_system()

dag_id = "mqe-full-url-rule-generation"
if job_environment != TtdEnvFactory.prod:
    dag_id = "mqe-full-url-rule-generation-test"

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_groups.mqe.alarm_channel,
    slack_tags=slack_groups.mqe.name,
    retries=max_retries,
    retry_delay=retry_delay,
    tags=["MQE", "signalValidation", "fullUrl"],
    enable_slack_alert=True,
)

# need to wait on the data being ready
etl_done = DatasetCheckSensor(
    dag=dag.airflow_dag,
    ds_date="{{ data_interval_start.to_datetime_string() }}",
    datasets=[AvailsDatasources.full_url_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1")],
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    timeout=60 * 60 * 4,  # 4 hours
)

# Master node config
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R7g.r7g_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

# Core nodes config
core_fleet_capacity = 20
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R7g.r7g_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=core_fleet_capacity
)

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4

# Cluster task
cluster_task = EmrClusterTask(
    name="MQEFullUrlRuleGeneration-Cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.mqe.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    environment=job_environment,
    emr_release_label=emr_release_label
)

# Job task config
jar_path = ("s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar")
job_class_name = "jobs.inventorysignals.fullurl.FullUrlRuleGeneration"
date_macro = "{{ data_interval_start.strftime('%Y-%m-%dT%H:00:00') }}"
eldorado_config_option_pairs_list = [
    ("datetime", date_macro),
    ("env", job_environment.execution_env),
]

spark_options_list = [
    ('conf', 'spark.dynamicAllocation.enabled=true'),
    ('conf', 'spark.speculation=false'),
    ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'),
    ('conf', 'spark.sql.files.ignoreCorruptFiles=true'),
]

job_task = EmrJobTask(
    name="MQEFullUrlRuleGeneration-Task",
    class_name=job_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
)

cluster_task.add_parallel_body_task(job_task)
dag >> cluster_task

etl_done >> cluster_task.first_airflow_op()

FULL_URL_RULES_DATA_GATING_TYPE_ID = 2000626

full_url_data_open_gate_task = MsSqlImportFromCloud(
    name="FullUrlRulesValidation_OpenLWGate",
    gating_type_id=FULL_URL_RULES_DATA_GATING_TYPE_ID,
    log_type_id=LogTypeFrequency.HOURLY.value,
    log_start_time='{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}',
    mssql_import_enabled=True,
    job_environment=job_environment,
)

job_task >> full_url_data_open_gate_task

adag = dag.airflow_dag
