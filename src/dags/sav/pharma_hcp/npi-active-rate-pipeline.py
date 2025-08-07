"""
NPI Active Rate Pipeline

This pipeline calculates active rates.
"""
from datetime import datetime

from datasources.sources.avails_datasources import AvailsDatasources

from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import sav
from ttd.tasks.op import OpTask
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.storage_optimized.i3en import I3en

from dags.sav.pharma_hcp.npi_common_config import NpiCommonConfig
from dags.sav.pharma_hcp.npi_operator_factory import NpiOperatorFactory

dag_id = "npi-active-rate-pipeline"

# The job is scheduled to run daily at 5:00 AM to align with the availability of the "identity-agg-daily" data, which is typically ready around this time
job_schedule_interval = "0 5 * * *"
job_start_date = datetime(2024, 7, 1)

env_config = NpiCommonConfig.get_environment_config()
s3_paths = NpiCommonConfig.get_s3_paths()

file_date_path = "{{ logical_date.strftime(\"%Y%m%d\") }}"

ttd_dag = TtdDag(
    dag_id=dag_id,
    slack_channel=NpiCommonConfig.NOTIFICATION_SLACK_CHANNEL,
    enable_slack_alert=True,
    start_date=job_start_date,
    run_only_latest=True,
    schedule_interval=job_schedule_interval,
    retries=NpiCommonConfig.DEFAULT_RETRIES,
    max_active_runs=1,
    depends_on_past=True,
    retry_delay=NpiCommonConfig.DEFAULT_RETRY_DELAY,
    tags=[sav.jira_team]
)
dag = ttd_dag.airflow_dag

id_export_generator_task = NpiOperatorFactory.create_standard_npi_task(
    task_id="id_export_generator",
    task_name="id_export_generator",
    generator_task_type="IdExportGenerator",
    dag=dag,
    workload_size="standard",
    additional_env_vars=
    {"TTD_NPI_IDEXPORT_REPORTDATE": "{{ (dag_run.conf.get('REPORTDATE') if dag_run and dag_run.conf else None) or ds }}"}
)

active_id_generator_task = NpiOperatorFactory.create_standard_npi_task(
    task_id="active_id_generator",
    task_name="active_id_generator",
    generator_task_type="NpiActiveRateGenerator",
    dag=dag,
    workload_size="standard",
    additional_env_vars=
    {"TTD_NPI_ACTIVERATE_REPORTDATE": "{{ (dag_run.conf.get('REPORTDATE') if dag_run and dag_run.conf else None) or ds }}"}
)

# Ref: https://thetradedesk.gitlab-pages.adsrvr.org/teams/aifun/TTD.AvailsPipeline.Contract/consuming/prod/#airflow-orchestration
wait_for_input_data = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id="wait-for-identity-agg-daily",
        ds_date="{{ logical_date.strftime(\"%Y-%m-%d %H:%M:%S\") }}",
        lookback=0,
        poke_interval=60 * 10,
        timeout=60 * 60 * 24,
        datasets=[AvailsDatasources.identity_agg_daily_dataset]
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3en.i3en_24xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[I3en.i3en_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=30
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

spark_options = [
    ("conf", "spark.driver.maxResultSize=0"),
    ("conf", "spark.network.timeout=2400s"),
    ("conf", "spark.executor.heartbeatInterval=2000s"),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
]

java_options = [
    ("AvailsDate", "{{ ds }}"),
    ("IqviaIdExportPath", f"{s3_paths['iqvia_id_export_path']}/{file_date_path}_ttd_ids/"),
    ("LookBackDays", 7),
    ("DailyIdOutputPath", f"{s3_paths['daily_id_output_root']}/"),
    ("FullIdOutputPath", f"{s3_paths['full_id_output_root']}/date={file_date_path}/"),
]

cluster_task = EmrClusterTask(
    name="match-crosswalk-to-avails-ids",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": sav.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    additional_application_configurations=[additional_application_configurations],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
)

job_task = EmrJobTask(
    name="match-crosswalk-to-avails-ids-task",
    class_name="jobs.sav.matchrate.HcpActiveIdExporter",
    executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-sav-assembly.jar",
    configure_cluster_automatically=True,
    additional_args_option_pairs_list=spark_options,
    eldorado_config_option_pairs_list=java_options,
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> id_export_generator_task >> wait_for_input_data >> cluster_task >> active_id_generator_task
