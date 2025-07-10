from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, ELDORADO_INVMKT_JAR
from dags.dmg.utils import get_jar_file_path
from datasources.sources.coldstorage_lookup_datasources import ColdstorageLookupDataSources
from ttd.ec2.emr_instance_types.general_purpose.m7a import M7a
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.interop.vertica_import_operators import LogTypeFrequency
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from datetime import datetime, timedelta
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DEAL_MANAGEMENT

avails_root_path = "{{ dag_run.conf.get(\"availsRoot\", \"s3://thetradedesk-useast-logs-2/avails/cleansed/\") }}"
hour_macro = "{{ dag_run.logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"

import_time = "{{ (dag_run.logical_date - macros.timedelta(hours=3)).strftime(\"%Y-%m-%d %H:00:00\") }}"
next_hour_ds = "{{ (dag_run.logical_date - macros.timedelta(hours=2)).strftime(\"%Y-%m-%d %H:00:00\") }}"

gating_type_id_qualified_avails_mssql = 2000665
gating_type_id_contributing_deal_avail_mssql = 2000684

job_jar = get_jar_file_path(ELDORADO_INVMKT_JAR)

java_options_list = [("availsRoot", avails_root_path), ("hour", hour_macro)]

spark_options_list = [('conf', 'spark.dynamicAllocation.enabled=true'), ("conf", "spark.speculation=false"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                      ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.sql.adaptive.enabled=true"),
                      ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"), ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
                      ("conf", "spark.sql.files.ignoreMissingFiles=true"), ("conf", "maximizeResourceAllocation=true")]

job_name = "dmg-qualified-avails-agg-hourly"

env = TtdEnvFactory.get_from_system()

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7a.m7a_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7a.m7a_8xlarge().with_fleet_weighted_capacity(32),
        M7a.m7a_16xlarge().with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=256
)

# Generates qualified and expanded avail counts for rollup and contributing deals
dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 5, 0),
    schedule_interval=timedelta(hours=1) if env == TtdEnvFactory.prod else None,
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    tags=[DEAL_MANAGEMENT.team.jira_team, "qualified_avails"],
    retries=1,
    retry_delay=timedelta(minutes=5),
    dagrun_timeout=timedelta(hours=6),
    run_only_latest=True
)

adag = dag.airflow_dag
cluster_task = EmrClusterTask(
    name=f"{job_name}-cluster",
    cluster_tags={"Team": DEAL_MANAGEMENT.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    cluster_auto_termination_idle_timeout_seconds=5 * 60,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }]
)

job_task_hourly_metrics = EmrJobTask(
    name=f"{job_name}-task",
    class_name="jobs.dealmetadata.QualifiedAvailsAggregation",
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=6),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_options_list,
    configure_cluster_automatically=True
)

cluster_task.add_sequential_body_task(job_task_hourly_metrics)

wait_for_random_sampled_avails = OpTask(
    op=DatasetCheckSensor(
        task_id='random_sampled_avails_available',
        datasets=[ColdstorageLookupDataSources.avails_sampled.with_check_type("hour")],
        ds_date=import_time,
        poke_interval=60 * 10,
        # wait up to 3 hours
        timeout=60 * 60 * 3,
        raise_exception=True,
    )
)

wait_for_next_random_sampled_avails = OpTask(
    op=DatasetCheckSensor(
        task_id='random_sampled_avails_next_available',
        datasets=[ColdstorageLookupDataSources.avails_sampled.with_check_type("hour")],
        # next hour of data needs to have started generation before we start reading previous hour
        ds_date=next_hour_ds,
        poke_interval=60 * 10,
        # wait up to 3 hours
        timeout=60 * 60 * 3,
        raise_exception=True,
    )
)

open_gate_task_qualified_avails_mssql = MsSqlImportFromCloud(
    name="QualifiedAvails_MSSQL_DataImport_OpenLWGate",
    gating_type_id=gating_type_id_qualified_avails_mssql,
    log_type_id=LogTypeFrequency.HOURLY.value,
    log_start_time=import_time,
    mssql_import_enabled=True,
    job_environment=env,
)

open_gate_task_contributing_deal_avail_mssql = MsSqlImportFromCloud(
    name="AvailsDealMetadata_MSSQL_DataImport_OpenLWGate_ContributingDealAvailData",
    gating_type_id=gating_type_id_contributing_deal_avail_mssql,
    log_type_id=LogTypeFrequency.HOURLY.value,
    log_start_time=import_time,
    mssql_import_enabled=True,
    job_environment=env,
)

dag >> wait_for_random_sampled_avails >> wait_for_next_random_sampled_avails >> cluster_task >> open_gate_task_qualified_avails_mssql >> open_gate_task_contributing_deal_avail_mssql
