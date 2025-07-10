from datetime import datetime, timedelta
from math import floor

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

env = "prod"
dev_user = "alex.turc"

DICT_CONFIG = {
    "dev": {
        "version": "1.0.0-SNAPSHOT",
        "s3_folder": f"dev/{dev_user}/application/dmp_export",
        "application_data_url": f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/dmp_export/data",
        "slack_channel": f"@{dev_user}",
    },
    "prod": {
        "version": "1.1.8",
        "s3_folder": "application/dmp_export",
        "application_data_url": "s3://ttd-datprd-us-east-1/application/dmp_export/data",
        "slack_channel": "#scrum-data-products-alarms",
    }
}

name = "dmp_export"
group_id = "com.thetradedesk." + name
version = DICT_CONFIG[env]["version"]

start_date = datetime(2022, 8, 31, 23, 0)
schedule_interval = "0 1 * * *"
timeout_hours = 192
slack_channel = DICT_CONFIG[env]["slack_channel"]

s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]

instance_types_collector = [
    M6g.m6g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(64),
    M7g.m7g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(64)
]

collector_master_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
collector_master_machine_vcpu = 8
collector_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
collector_core_machine_vcpu_granularity = 3
collector_core_machine_vcpu = 8 * collector_core_machine_vcpu_granularity
collector_driver_memory_overhead = 0.25
collector_executor_memory_overhead = 0.25
collector_driver_count = 1
collector_core_machine_count = 30
collector_executor_count_per_machine = 1
collector_driver_core_count = int(floor(collector_master_machine_vcpu / collector_driver_count))
collector_driver_memory = str(
    int(floor(collector_master_machine_vmem / (collector_driver_count * (1 + collector_driver_memory_overhead))))
) + "G"
collector_executor_count = collector_core_machine_count * collector_executor_count_per_machine
collector_executor_core_count = int(floor(collector_core_machine_vcpu / collector_executor_count_per_machine))
collector_executor_memory = str(
    int(floor(collector_core_machine_vmem / (collector_executor_count_per_machine * (1 + collector_executor_memory_overhead))))
) + "G"

instance_types_publisher = [
    M7g.m7g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256),
    M6g.m6g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256)
]

publisher_master_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
publisher_master_machine_vcpu = 8
publisher_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
publisher_core_machine_vcpu_granularity = 1
publisher_core_machine_vcpu = 8 * publisher_core_machine_vcpu_granularity
publisher_driver_memory_overhead = 0.25
publisher_executor_memory_overhead = 0.25
publisher_driver_count = 1
publisher_core_machine_count = 200
publisher_executor_count_per_machine = 1
publisher_driver_core_count = int(floor(publisher_master_machine_vcpu / publisher_driver_count))
publisher_driver_memory = str(
    int(floor(publisher_master_machine_vmem / (publisher_driver_count * (1 + publisher_driver_memory_overhead))))
) + "G"
publisher_executor_count = publisher_core_machine_count * publisher_executor_count_per_machine
publisher_executor_core_count = int(floor(publisher_core_machine_vcpu / publisher_executor_count_per_machine))
publisher_executor_memory = str(
    int(floor(publisher_core_machine_vmem / (publisher_executor_count_per_machine * (1 + publisher_executor_memory_overhead))))
) + "G"

log_level = "TRACE"

instance_timestamp = datetime.utcnow()
application_data_url = DICT_CONFIG[env]["application_data_url"]
prometheus_push_gateway_address = "pushgateway.gen.adsrvr.org:80"
job_instance_s3_folder = s3_folder + "/instance/" + instance_timestamp.isoformat()
job_application_version = version
job_emr_cluster_name = f"DATPRD-{name}"

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
        'in_cluster': True
    },
    start_date=start_date,
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=timeout_hours),
    schedule_interval=schedule_interval,
    slack_channel=slack_channel,
    slack_alert_only_for_prod=True
)

tags = {
    "Environment": env,
    "Job": name,
    "Resource": "EMR",
    "process": "dmp_export",
    "Source": "Airflow",
    "Team": "DATPRD",
}

cluster_configs_collector = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.resource.cpu-vcores": str(collector_executor_core_count)
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxRetries": "150",
        "fs.s3.sleepTimeSeconds": "10",
        "fs.s3.maxConnections": "200",
    }
}]

cluster_configs_publisher = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.resource.cpu-vcores": str(publisher_executor_core_count)
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxRetries": "150",
        "fs.s3.sleepTimeSeconds": "10",
        "fs.s3.maxConnections": "200",
    }
}]

ec2_key_name = "UsEastDataEngineering"

subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

cluster_bootscript = [ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + s3_folder + "/bin/boot/" + version + "/bootstrap.sh")]

additional_spark_args_collector = [
    ("conf", "spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.retryMode=STANDARD -Dcom.amazonaws.sdk.maxAttempts=15000"),
    ("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.files.ignoreCorruptFiles=false"), ("conf", "spark.driver.memory=" + collector_driver_memory),
    ("conf",
     "spark.driver.cores=" + str(collector_driver_core_count)), ("conf", "spark.executor.instances=" + str(collector_executor_count)),
    ("conf", "spark.executor.cores=" + str(collector_executor_core_count)),
    ("conf", "spark.executor.memory=" + str(collector_executor_memory)),
    ("conf", "spark.default.parallelism=" + str(collector_executor_count * collector_executor_core_count)),
    ("conf", "spark.sql.shuffle.partitions=" + str(collector_executor_count * collector_executor_core_count)),
    ("conf", "spark.sql.broadcastTimeout=600"), ("conf", "spark.task.cpus=" + str(collector_core_machine_vcpu_granularity))
]

additional_spark_args_publisher = [
    ("conf", "spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.retryMode=STANDARD -Dcom.amazonaws.sdk.maxAttempts=15000"),
    ("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.files.ignoreCorruptFiles=false"), ("conf", "spark.driver.memory=" + publisher_driver_memory),
    ("conf",
     "spark.driver.cores=" + str(publisher_driver_core_count)), ("conf", "spark.executor.instances=" + str(publisher_executor_count)),
    ("conf", "spark.executor.cores=" + str(publisher_executor_core_count)),
    ("conf", "spark.executor.memory=" + str(publisher_executor_memory)),
    ("conf", "spark.default.parallelism=" + str(publisher_executor_count * publisher_executor_core_count)),
    ("conf", "spark.sql.shuffle.partitions=" + str(publisher_executor_count * publisher_executor_core_count)),
    ("conf", "spark.sql.broadcastTimeout=600"), ("conf", "spark.task.cpus=" + str(publisher_core_machine_vcpu_granularity))
]

cold_storage_address = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", port=4333)|replace(":4333", ":aerospike-cluster.adsrvr.org:4333")}}'

# Collector

create_cluster_collector = EmrClusterTask(
    name=job_emr_cluster_name + "_collector",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types_collector, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=instance_types_collector, on_demand_weighted_capacity=collector_core_machine_count),
    cluster_tags=tags | {"Duration": "extremely_long"},
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs_collector,
    cluster_auto_termination_idle_timeout_seconds=600,
    bootstrap_script_actions=cluster_bootscript,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_scan_dmp = EmrJobTask(
    name="add_scan_dmp",
    cluster_specs=create_cluster_collector.cluster_specs,
    class_name=group_id + ".collector.Main",
    additional_args_option_pairs_list=additional_spark_args_collector,
    deploy_mode="client",
    command_line_arguments=[
        "scan-dmp", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}",
        "--provisioningDbUrl", "jdbc:sqlserver://provdb.adsrvr.org:1433;database=Provisioning;authenticationScheme=NTLM",
        "--provisioningDbDomain", "ops.adsrvr.org", "--provisioningDbUser",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__PROVISIONING_DB_USER", "--provisioningDbPass",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__PROVISIONING_DB_PASS", "--dmpDbUser",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__DMP_DB_USER", "--dmpDbPass",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__DMP_DB_PASS", "--dmpDbNamespace", "ttd-coldstorage-onprem",
        "--dmpDbDesiredMaxParallelScansPerNode", "4", "--dagTimestamp", "{{ts}}", "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address, "--dmpDbAddress", cold_storage_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/collector/" + version + "/" + group_id + "-collector-" + version +
    "-all.jar",
    configure_cluster_automatically=False,
    timeout_timedelta=timedelta(hours=150),
)

# Publisher

create_cluster_publisher = EmrClusterTask(
    name=job_emr_cluster_name + "_publisher",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types_publisher, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=instance_types_publisher, on_demand_weighted_capacity=publisher_core_machine_count),
    cluster_tags=tags | {"Duration": "very_long"},
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs_publisher,
    cluster_auto_termination_idle_timeout_seconds=600,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_assemble = EmrJobTask(
    name="add_assemble",
    cluster_specs=create_cluster_publisher.cluster_specs,
    class_name=group_id + ".collector.Main",
    additional_args_option_pairs_list=additional_spark_args_publisher,
    deploy_mode="client",
    command_line_arguments=[
        "assemble", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}", "--dagTimestamp",
        "{{ts}}", "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/collector/" + version + "/" + group_id + "-collector-" + version +
    "-all.jar",
    configure_cluster_automatically=False,
    timeout_timedelta=timedelta(hours=8),
)

add_publish_to_legacy_dmp_data_folder = EmrJobTask(
    name="add_publish_to_legacy_dmp_data_folder",
    cluster_specs=create_cluster_publisher.cluster_specs,
    class_name=group_id + ".publisher.Main",
    additional_args_option_pairs_list=additional_spark_args_publisher,
    deploy_mode="client",
    command_line_arguments=[
        "publish-to-legacy-dmp-data-folder", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--provisioningDbUrl",
        "jdbc:sqlserver://provdb.adsrvr.org:1433;database=Provisioning;authenticationScheme=NTLM", "--provisioningDbDomain",
        "ops.adsrvr.org", "--provisioningDbUser", "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__PROVISIONING_DB_USER",
        "--provisioningDbPass", "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__PROVISIONING_DB_PASS", "--legacyDmpDataUrl",
        "s3://ttd-datamarketplace/dataexport/internal", "--referenceDate", "{{ds}}", "--dagTimestamp", "{{ts}}",
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/publisher/" + version + "/" + group_id + "-publisher-" + version +
    "-all.jar",
    configure_cluster_automatically=False,
    timeout_timedelta=timedelta(hours=8),
)

add_publish_to_snowflake_for_png = EmrJobTask(
    name="add_publish_to_snowflake_for_png",
    cluster_specs=create_cluster_publisher.cluster_specs,
    class_name=group_id + ".publisher.Main",
    additional_args_option_pairs_list=additional_spark_args_publisher,
    deploy_mode="client",
    command_line_arguments=[
        "publish-to-snowflake", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}",
        "--provisioningDbUrl",
        "jdbc:sqlserver://provdb.adsrvr.org:1433;database=Provisioning;integratedSecurity=true;authenticationScheme=NTLM",
        "--provisioningDbDomain", "ops.adsrvr.org", "--provisioningDbUser",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__PROVISIONING_DB_USER", "--provisioningDbPass",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__PROVISIONING_DB_PASS", "--snowflakeUrl",
        "thetradedesk-covington.snowflakecomputing.com", "--snowflakeUser",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__SNOWFLAKE_PNG_USER", "--snowflakePass",
        "@/home/hadoop/.ttd/dmp_export/app_params/TTD__DMP_EXPORT__SNOWFLAKE_PNG_PASS", "--snowflakeDatabase", "THETRADEDESK",
        "--snowflakeSchema", "DMP", "--snowflakeWarehouse", "AVAILS_WH", "--snowflakeUseCaseSensitiveIdentifiers", "false",
        "--includeOwnerIds", "pgdirect", "cxx0ttd", "rupwvu7", "--includeFields", "TargetingDataId", "UserId", "UserIdType",
        "ThirdPartyDataId", "FullPath", "--useDeltaUpdates", "false", "--dagTimestamp", "{{ts}}", "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/publisher/" + version + "/" + group_id + "-publisher-" + version +
    "-all.jar",
    configure_cluster_automatically=False,
    timeout_timedelta=timedelta(hours=8),
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))

create_cluster_collector.add_sequential_body_task(add_scan_dmp)
create_cluster_publisher.add_sequential_body_task(add_assemble)
create_cluster_publisher.add_sequential_body_task(add_publish_to_legacy_dmp_data_folder)
create_cluster_publisher.add_sequential_body_task(add_publish_to_snowflake_for_png)

dag >> create_cluster_collector >> create_cluster_publisher >> check
adag = dag.airflow_dag
