from datetime import datetime, timedelta, timezone

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m6a import M6a
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.general_purpose.m7a import M7a
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

env = "prod"
dev_user = "rehaan.mehta"

DICT_CONFIG = {
    "dev": {
        "version": "1.0.8-SNAPSHOT",
        "s3_folder": f"dev/{dev_user}/application/unifiedModel",
        "application_data_url": f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/unified_model/data",
        "slack_channel": f"@{dev_user}"
    },
    "prod": {
        "version": "1.5.2",
        "s3_folder": "application/unifiedModel",
        "application_data_url": "s3://ttd-datprd-us-east-1/application/unified_model/data",
        "slack_channel": "#scrum-data-products-alarms"
    }
}

name = "unified_model_ingestion"
group_id = "com.thetradedesk.unifiedModel"
artifact_id = "ingestion"
version = DICT_CONFIG[env]["version"]
start_date = datetime(2022, 8, 31, 23, 0)
schedule_rate = 6
schedule_interval = f"51 */{schedule_rate} * * *"
timeout_hours = 48
slack_channel = DICT_CONFIG[env]["slack_channel"]
s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]
log_level = "TRACE"
instance_timestamp = datetime.now(timezone.utc)
application_data_url = DICT_CONFIG[env]["application_data_url"]
otel_metrics_exporter_address = "https://opentelemetry-global-gateway.gen.adsrvr.org/http/v1/metrics"
origin_date_time = "2023-04-12T00:00:00+00:00"
iso_3166_url = "s3://ttd-datprd-us-east-1/data/Iso3166/v=1"
supply_vendor_url = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendor/v=1"
avail_url = "s3://thetradedesk-useast-avails/datasets/withPII/prod/identity-avails-agg-hourly-v2-delta"
bidrequest_url = "s3://thetradedesk-useast-logs-2/bidrequest/cleansed"
job_emr_cluster_name = f"DATPRD-{name}"
computationHours = "24"

dag = TtdDag(
    dag_id=name,
    tags=["DATPRD"],
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": "DATPRD",
        "retries": 0,
        'in_cluster': True,
    },
    start_date=start_date,
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=timeout_hours),
    schedule_interval=schedule_interval,
    slack_channel=slack_channel,
    slack_alert_only_for_prod=True
)

tags = {"Environment": env, "Job": name, "Resource": "EMR", "process": "unified_model", "Source": "Airflow", "Team": "DATPRD"}

cluster_configs = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.resource.cpu-vcores": "16"
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.aimd.enabled": "true",
        "fs.s3.aimd.maxAttempts": "600000",
        "fs.s3.multipart.part.attempts": "600000",
        "fs.s3.maxRetries": "600000",
        "fs.s3.maxConnections": "500",
    }
}]

ec2_key_name = "UsEastDataEngineering"

subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

cluster_bootscript = [ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + s3_folder + "/bin/boot/" + version + "/bootstrap.sh")]

instance_types = [
    M7g.m7g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128),
    M6g.m6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128),
    M7a.m7a_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128),
    M6a.m6a_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128),
]

driver_memory = "40G"
driver_core_count = 12
executor_count = 250
executor_core_count = 12
executor_memory = "40G"

additional_spark_args = [
    ("conf", "spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.retryMode=STANDARD -Dcom.amazonaws.sdk.maxAttempts=15000"),
    ("conf", "spark.driver.maxResultSize=32G"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.files.ignoreCorruptFiles=false"),
    ("conf", "spark.driver.memory=" + driver_memory),
    ("conf", "spark.driver.cores=" + str(driver_core_count)),
    ("conf", "spark.executor.instances=" + str(executor_count)),
    ("conf", "spark.executor.cores=" + str(executor_core_count)),
    ("conf", "spark.executor.memory=" + str(executor_memory)),
    ("conf", "spark.default.parallelism=" + str(executor_count * executor_core_count * 2)),
    ("conf", "spark.sql.shuffle.partitions=" + str(executor_count * executor_core_count * 2)),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("conf", "spark.jars.packages=io.delta:delta-core_2.12:2.3.0"),
]

create_cluster = EmrClusterTask(
    name=job_emr_cluster_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=executor_count),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    bootstrap_script_actions=cluster_bootscript,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_ingest_supply_vendor = EmrJobTask(
    name="ingest_supply_vendor",
    cluster_specs=create_cluster.cluster_specs,
    class_name=group_id + "." + artifact_id + ".Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=[
        "ingest-supply-vendor", "--env", env, "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate",
        "{{dag_run.start_date.strftime(\"%Y-%m-%d\")}}", "--computationDays", "30", "--supplyVendorUrl", supply_vendor_url,
        "--dagTimestamp", "{{ts}}", "--otelMetricsExporterAddress", otel_metrics_exporter_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
    "-" + version + "-all.jar",
    configure_cluster_automatically=False
)

add_ingest_avail = EmrJobTask(
    name="ingest_avail",
    cluster_specs=create_cluster.cluster_specs,
    class_name=group_id + "." + artifact_id + ".Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=[
        "ingest-avail", "--env", env, "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDateTime",
        "{{dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00+00:00\")}}", "--computationHours", computationHours, "--originDateTime",
        origin_date_time, "--availUrl", avail_url, "--iso3166Url", iso_3166_url, "--dagTimestamp", "{{ts}}", "--otelMetricsExporterAddress",
        otel_metrics_exporter_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
    "-" + version + "-all.jar",
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False
)

add_ingest_bidrequest = EmrJobTask(
    name="ingest_bidrequest",
    cluster_specs=create_cluster.cluster_specs,
    class_name=group_id + "." + artifact_id + ".Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=[
        "ingest-bidrequest", "--env", env, "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDateTime",
        "{{dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00+00:00\")}}", "--computationHours", computationHours, "--originDateTime",
        origin_date_time, "--bidRequestUrl", bidrequest_url, "--dagTimestamp", "{{ts}}", "--otelMetricsExporterAddress",
        otel_metrics_exporter_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
    "-" + version + "-all.jar",
    configure_cluster_automatically=False
)

add_merge_user_history_avail = EmrJobTask(
    name="merge_user_history_avail",
    cluster_specs=create_cluster.cluster_specs,
    class_name=group_id + "." + artifact_id + ".Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=[
        "merge-user-history", "--env", env, "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDateTime",
        "{{dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00+00:00\")}}", "--computationHours", computationHours, "--originDateTime",
        origin_date_time, "--dataType", "Avail", "--dagTimestamp", "{{ts}}", "--otelMetricsExporterAddress", otel_metrics_exporter_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
    "-" + version + "-all.jar",
    configure_cluster_automatically=False
)

add_merge_user_history_bidrequest = EmrJobTask(
    name="merge_user_history_bidrequest",
    deploy_mode="client",
    cluster_specs=create_cluster.cluster_specs,
    class_name=group_id + "." + artifact_id + ".Main",
    additional_args_option_pairs_list=additional_spark_args,
    command_line_arguments=[
        "merge-user-history", "--env", env, "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDateTime",
        "{{dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00+00:00\")}}", "--computationHours", computationHours, "--originDateTime",
        origin_date_time, "--dataType", "BidRequest", "--dagTimestamp", "{{ts}}", "--otelMetricsExporterAddress",
        otel_metrics_exporter_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
    "-" + version + "-all.jar",
    configure_cluster_automatically=False
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))

create_cluster.add_sequential_body_task(add_ingest_supply_vendor)
create_cluster.add_sequential_body_task(add_ingest_bidrequest)
create_cluster.add_sequential_body_task(add_merge_user_history_bidrequest)
create_cluster.add_sequential_body_task(add_ingest_avail)
create_cluster.add_sequential_body_task(add_merge_user_history_avail)

dag >> create_cluster >> check
adag = dag.airflow_dag
