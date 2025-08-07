from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, ELDORADO_INVMKT_JAR
from dags.dmg.utils import get_jar_file_path
from datasources.sources.coldstorage_lookup_datasources import ColdstorageLookupDataSources
from datasources.sources.invmkt_datasources import InvMktDatasources
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.interop.mssql_import_operators import MsSqlImportFromCloud
from ttd.interop.vertica_import_operators import LogTypeFrequency, VerticaImportFromCloud
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.ec2.cluster_params import Defaults
from ttd.ec2.emr_instance_types.general_purpose.m6a import M6a
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from datetime import datetime, timedelta
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator

AvailsRoot = "s3://thetradedesk-useast-logs-2/avails/cleansed/"

hour_macro = "{{ dag_run.logical_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"
import_time = "{{ (dag_run.logical_date - macros.timedelta(hours=3)).strftime(\"%Y-%m-%d %H:00:00\") }}"
next_hour_ds = "{{ (dag_run.logical_date - macros.timedelta(hours=2)).strftime(\"%Y-%m-%d %H:00:00\") }}"
job_jar = get_jar_file_path(ELDORADO_INVMKT_JAR)

java_options_list = [("availsRoot", AvailsRoot), ("hour", hour_macro)]

spark_options_list = [('conf', 'spark.dynamicAllocation.enabled=true'), ("conf", "spark.speculation=false"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                      ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.sql.adaptive.enabled=true"),
                      ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"), ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
                      ("conf", "spark.sql.files.ignoreMissingFiles=true"), ("conf", "maximizeResourceAllocation=true")]

gating_type_id_observed_signal = 2000555
gating_type_id_scope_observed_signal = 2000554
gating_type_id_signal_market_average = 2000565
gating_type_id_scope_observed_signal_mssql = 2000664

env = TtdEnvFactory.get_from_system()

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M6a.m6a_4xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6a.m6a_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(4096)
        .with_ebs_iops(Defaults.MAX_GP3_IOPS).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
        M6a.m6a_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64).with_ebs_size_gb(8192)
        .with_ebs_iops(Defaults.MAX_GP3_IOPS).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
    ],
    on_demand_weighted_capacity=512
)

# Generates data required for observed metrics and market average for deal targeting terms and signal terms
dag = TtdDag(
    dag_id="avails-deal-metadata-hourly-v2",
    start_date=datetime(2025, 2, 12, 1),
    schedule_interval=timedelta(hours=1),
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    tags=[DEAL_MANAGEMENT.team.jira_team, "avails-deal-metadata-hourly-v2", "qualified_avails"],
    run_only_latest=False,
    retries=1,
    retry_delay=timedelta(minutes=5),
    dagrun_timeout=timedelta(hours=12),
    depends_on_past=True
)

adag = dag.airflow_dag
cluster_task = EmrClusterTask(
    name="AvailsDealMetadataHourlyV2_cluster",
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
    name="AvailsDealMetadataHourlyMetrics_step",
    class_name="jobs.dealmetadata.AvailsDealMetadata",
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=12),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_options_list,
    configure_cluster_automatically=True
)

cluster_task.add_sequential_body_task(job_task_hourly_metrics)

job_task_24hour_metrics = EmrJobTask(
    name="AvailsDealMetadata24HourMetrics_step",
    class_name="jobs.dealmetadata.AvailsDealMetadata24Hours",
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=12),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_options_list,
    configure_cluster_automatically=True
)

cluster_task.add_sequential_body_task(job_task_24hour_metrics)

wait_for_deals_daily_ds = DatasetCheckSensor(
    task_id='ttd_deals_daily_dataset_check',
    dag=dag.airflow_dag,
    ds_date="{{ (logical_date - macros.timedelta(days=1)).to_datetime_string() }}",
    poke_interval=60 * 10,
    datasets=[InvMktDatasources.ttd_deals_daily],
    timeout=60 * 60 * 4
)

wait_for_bid_feedback_and_request_ds = DatasetCheckSensor(
    task_id='bid_feedback_and_request_dataset_check',
    dag=dag.airflow_dag,
    ds_date="{{ (logical_date - macros.timedelta(hours=3)).to_datetime_string() }}",
    poke_interval=60 * 10,
    datasets=[RtbDatalakeDatasource.rtb_bidrequest_v5, RtbDatalakeDatasource.rtb_bidfeedback_v5],
    timeout=60 * 60 * 4
)

wait_for_random_sampled_avails = OpTask(
    op=DatasetCheckSensor(
        task_id='random_sampled_avails_available',
        datasets=[ColdstorageLookupDataSources.avails_sampled.with_check_type("hour")],
        ds_date=import_time,
        poke_interval=60 * 10,
        # wait up to 12 hours
        timeout=60 * 60 * 12,
        raise_exception=False,
    )
)

wait_for_next_random_sampled_avails = OpTask(
    op=DatasetCheckSensor(
        task_id='random_sampled_avails_next_available',
        datasets=[ColdstorageLookupDataSources.avails_sampled.with_check_type("hour")],
        # next hour of data needs to have started generation before we start reading previous hour
        ds_date=next_hour_ds,
        poke_interval=60 * 10,
        # wait up to 12 hours
        timeout=60 * 60 * 12,
        raise_exception=False,
    )
)

final_dag_check = FinalDagStatusCheckOperator(dag=adag)


def create_vertica_import_from_cloud_task(gating_type_id: int, subdag_name: str):
    global import_time
    return VerticaImportFromCloud(
        dag=dag.airflow_dag,
        subdag_name=subdag_name,
        gating_type_id=gating_type_id,
        log_type_id=LogTypeFrequency.HOURLY.value,
        log_start_time=import_time,
        vertica_import_enabled=True,
        job_environment=env,
    )


open_gate_task_observed_signal = create_vertica_import_from_cloud_task(
    gating_type_id_observed_signal, "AvailsDealMetadata_DataImport_OpenLWGate_ObservedSignalsData"
)

open_gate_task_scope_observed_signal = create_vertica_import_from_cloud_task(
    gating_type_id_scope_observed_signal, "AvailsDealMetadata_DataImport_OpenLWGate_ScopeObservedSignalsData"
)

open_gate_task_signal_market_average = create_vertica_import_from_cloud_task(
    gating_type_id_signal_market_average, "AvailsDealMetadata_DataImport_OpenLWGate_SignalMarketAverage"
)

open_gate_task_scope_observed_signal_mssql = MsSqlImportFromCloud(
    name="AvailsDealMetadata_MSSQL_DataImport_OpenLWGate_ScopeObservedSignalsData",
    gating_type_id=gating_type_id_scope_observed_signal_mssql,
    log_type_id=LogTypeFrequency.HOURLY.value,
    log_start_time=import_time,
    mssql_import_enabled=True,
    job_environment=env,
)

dag >> wait_for_random_sampled_avails >> wait_for_next_random_sampled_avails >> cluster_task
wait_for_deals_daily_ds >> cluster_task.first_airflow_op()
wait_for_bid_feedback_and_request_ds >> cluster_task.first_airflow_op()
cluster_task >> open_gate_task_signal_market_average >> open_gate_task_scope_observed_signal >> open_gate_task_observed_signal >> open_gate_task_scope_observed_signal_mssql
