from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

from ttd.slack import slack_groups
from datetime import timedelta, datetime
from ttd.ttdenv import TtdEnvFactory
from ttd.interop.vertica_import_operators import VerticaImportFromCloud, LogTypeFrequency

job_start_date = datetime(2024, 7, 21)
job_schedule_interval = "0 17 * * *"
max_retries: int = 3
retry_delay: timedelta = timedelta(minutes=60)

job_environment = TtdEnvFactory.get_from_system()

dag_id = "mqe-sincera-data-import"
if job_environment != TtdEnvFactory.prod:
    dag_id = "mqe-sincera-data-import-test"

dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=slack_groups.mqe.alarm_channel,
    slack_tags=slack_groups.mqe.name,
    retries=max_retries,
    retry_delay=retry_delay,
    tags=["MQE", "Sincera"],
    enable_slack_alert=True
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_4xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(128)  # Set if cluster requires additional storage
        # .with_ebs_iops(3000)
        # .with_ebs_throughput(125)
        # Default IOPS and throughput are set based on storage size. Customise to optimise for your workload
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_capacity = 20
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M5.m5_4xlarge().with_fleet_weighted_capacity(20).with_ebs_size_gb(128)  # Set if cluster requires additional storage
        # .with_ebs_iops(3000)
        # .with_ebs_throughput(125)
        # Default EBS IOPS and throughput are set based on storage size. Customise to optimise for your workload
    ],
    on_demand_weighted_capacity=core_fleet_capacity,
)

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3
aws_region = "us-east-1"

cluster_task = EmrClusterTask(
    name="MQESinceraDataImport-Cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": slack_groups.mqe.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    environment=job_environment,
    emr_release_label=emr_release_label,
    region_name=aws_region
)

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-mqe-assembly.jar"
job_class_name = "jobs.inventorysignals.sinceraimport.SinceraS3DataImport"
date_macro = """{{ data_interval_start.subtract(days=1).to_date_string() }}"""
eldorado_config_option_pairs_list = [("date", date_macro), ("env", job_environment.execution_env)]

job_task = EmrJobTask(
    name="MQESinceraDataImport-Task",
    class_name=job_class_name,
    executable_path=jar_path,
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list
)

# Vertica import
vertica_import_date = '{{ data_interval_start.subtract(days=1).strftime(\"%Y-%m-%d 00:00:00\") }}'

SINCERA_SITE_METRICS_GATING_TYPE_ID = 2000299  # ImportSinceraDailyMQSiteMetrics

site_metrics_open_gate_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="SinceraS3DataImport_OpenLWGate_SiteMetrics",
    gating_type_id=SINCERA_SITE_METRICS_GATING_TYPE_ID,
    log_type_id=LogTypeFrequency.DAILY.value,
    log_start_time=vertica_import_date,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

SINCERA_PLACEMENT_METRICS_GATING_TYPE_ID = 2000303  # ImportSinceraDailyMQPlacementMetrics

placement_metrics_open_gate_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="SinceraS3DataImport_OpenLWGate_PlacementMetrics",
    gating_type_id=SINCERA_PLACEMENT_METRICS_GATING_TYPE_ID,
    log_type_id=LogTypeFrequency.DAILY.value,
    log_start_time=vertica_import_date,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

GPID_VALIDATION_DATA_GATING_TYPE_ID = 2000349  # ImportGPIDValidationData

gpid_validation_data_open_gate_task = VerticaImportFromCloud(
    dag=dag.airflow_dag,
    subdag_name="SinceraS3DataImport_OpenLWGate_GPIDValidationData",
    gating_type_id=GPID_VALIDATION_DATA_GATING_TYPE_ID,
    log_type_id=LogTypeFrequency.DAILY.value,
    log_start_time=vertica_import_date,
    vertica_import_enabled=True,
    job_environment=job_environment,
)

job_task >> site_metrics_open_gate_task
job_task >> placement_metrics_open_gate_task
job_task >> gpid_validation_data_open_gate_task

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
