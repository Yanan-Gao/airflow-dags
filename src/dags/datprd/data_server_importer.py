from datetime import datetime, timedelta
from json import dumps
from math import floor

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
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
        "s3_folder": f"dev/{dev_user}/application/data_server",
        "application_data_url": f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/data_server/data",
        "slack_channel": f"@{dev_user}",
    },
    "prod": {
        "version": "2.2.5",
        "s3_folder": "application/data_server",
        "application_data_url": "s3://ttd-datprd-us-east-1/application/data_server/data",
        "slack_channel": "#scrum-data-products-alarms",
    }
}

name = "data_server_importer"
group_id = "com.thetradedesk.data_server"
version = DICT_CONFIG[env]["version"]
folder_urls = {"thetradedesk": "s3://thetradedesk-useast-data-import/thetradedesk"}
data_server_properties = [{
    "id": 1,
    "url": "http://usw-data.adsrvr.org/data"
}, {
    "id": 2,
    "url": "http://use-data.adsrvr.org/data"
}, {
    "id": 3,
    "url": "http://euw-data.adsrvr.org/data"
}, {
    "id": 9,
    "url": "http://va6-data.adsrvr.org/data"
}, {
    "id": 10,
    "url": "http://usw-ca2-data.adsrvr.org/data"
}, {
    "id": 13,
    "url": "http://jp1-data-int.adsrvr.org/data"
}, {
    "id": 14,
    "url": "http://sg2-data-int.adsrvr.org/data"
}, {
    "id": 16,
    "url": "http://de1-data.adsrvr.org/data"
}, {
    "id": 17,
    "url": "http://cn3-data-int.adsrvr.cn/data"
}, {
    "id": 18,
    "url": "http://cn4-data-int.adsrvr.cn/data"
}, {
    "id": 22,
    "url": "http://ny1-data.adsrvr.org/data"
}, {
    "id": 71,
    "url": "http://va6-data.adsrvr.org/data"
}, {
    "id": 73,
    "url": "http://ca4-data.adsrvr.org/data"
}, {
    "id": 93,
    "url": "http://vad-data.adsrvr.org/data"
}]

start_date = datetime(2022, 8, 31, 23, 0)
schedule_interval = "37 * * * *"
timeout_hours = 4
slack_channel = DICT_CONFIG[env]["slack_channel"]

s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]

instance_types_importer = [
    M7g.m7g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
    M6g.m6g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
]

importer_master_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
importer_master_machine_vcpu = 8
importer_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
importer_driver_memory_overhead = 0.30
importer_executor_memory_overhead = 0.30
importer_core_machine_vcpu = 8
importer_driver_count = 1
importer_core_machine_count = 2
importer_driver_core_count = int(floor(importer_master_machine_vcpu / importer_driver_count))
importer_driver_memory = str(int(floor(importer_master_machine_vmem / (1 + importer_driver_memory_overhead)))) + "G"
importer_executor_memory = str(int(floor(importer_core_machine_vmem / (1 + importer_executor_memory_overhead)))) + "G"

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
    slack_alert_only_for_prod=True,
)

tags = {
    "Environment": env,
    "Job": name,
    "Resource": "EMR",
    "process": "data_server_importer",
    "Source": "Airflow",
    "Team": "DATPRD",
    "very_long_running": ""
}

cluster_configs = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.resource.cpu-vcores": "8"
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

additional_spark_args = [
    ("conf", "spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.retryMode=STANDARD -Dcom.amazonaws.sdk.maxAttempts=15000"),
    ("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.sql.files.ignoreCorruptFiles=false"), ("conf", "spark.driver.memory=" + importer_driver_memory),
    ("conf", "spark.driver.cores=" + str(importer_driver_core_count)),
    ("conf", "spark.executor.instances=" + str(importer_core_machine_count)),
    ("conf", "spark.executor.cores=" + str(importer_core_machine_vcpu)), ("conf", "spark.executor.memory=" + str(importer_executor_memory)),
    ("conf", "spark.default.parallelism=" + str(importer_core_machine_count * importer_core_machine_vcpu)),
    ("conf", "spark.sql.shuffle.partitions=" + str(importer_core_machine_count * importer_core_machine_vcpu)),
    ("conf", "spark.sql.broadcastTimeout=600")
]

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))
create_cluster_step = OpTask(op=EmptyOperator(task_id='create_cluster_step', trigger_rule="none_failed"))
dag_end = OpTask(op=EmptyOperator(task_id='dag_end', trigger_rule="none_failed"))


def verify_input_data_available_fn():
    result = [dag_end.task_id]
    s3_hook = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    for url in folder_urls.values():
        url_components = url.split("/")
        url_bucket = url_components[2]
        url_prefix = "/".join(url_components[3:] + ["import"])
        if s3_hook.check_for_prefix(bucket_name=url_bucket, prefix=url_prefix, delimiter="/"):
            result.append(create_cluster_step.task_id)
            break
    return result


verify_input_data_available = OpTask(
    op=BranchPythonOperator(task_id='verify_input_data_available', python_callable=verify_input_data_available_fn)
)

create_cluster_importer = EmrClusterTask(
    name=job_emr_cluster_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types_importer, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=instance_types_importer, on_demand_weighted_capacity=importer_core_machine_count),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    bootstrap_script_actions=cluster_bootscript,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_import_into_dmp_thetradedesk = EmrJobTask(
    name="add_import_into_dmp_thetradedesk",
    cluster_specs=create_cluster_importer.cluster_specs,
    class_name=group_id + ".importer.Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=[
        "import-from-folder", "--env", env, "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--provisioningDbUrl",
        "jdbc:sqlserver://provdb.adsrvr.org:1433;database=Provisioning;integratedSecurity=true;authenticationScheme=NTLM",
        "--provisioningDbDomain", "ops.adsrvr.org", "--provisioningDbUser",
        "@/home/hadoop/.ttd/data_server/app_params/TTD__DATA_SERVER__PROVISIONING_DB_USER", "--provisioningDbPass",
        "@/home/hadoop/.ttd/data_server/app_params/TTD__DATA_SERVER__PROVISIONING_DB_PASS", "--folderUrl", folder_urls["thetradedesk"],
        "--dataProviderId", "thetradedesk", "--defaultDataCenterId", "2", "--dataServerProperties",
        dumps(data_server_properties), "--dataServerMaxUserCountPerRequest", "10000", "--dataServerMaxCallRetryCount", "100",
        "--dataServerMinWaitTimeAfterFailedRequest", "PT0.5S", "--dataServerMaxWaitTimeAfterFailedRequest", "PT5S",
        "--maxFileCountToImport", "2000000", "--deleteImportedFiles", "true", "--parallelism",
        str(importer_core_machine_count * importer_core_machine_vcpu
            ), "--dagTimestamp", "{{ts}}", "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/importer/" + version + "/" + group_id + "-importer-" + version +
    "-all.jar",
    configure_cluster_automatically=False
)
create_cluster_importer.add_sequential_body_task(add_import_into_dmp_thetradedesk)

dag >> verify_input_data_available >> create_cluster_step >> create_cluster_importer >> dag_end >> check
verify_input_data_available >> dag_end
adag = dag.airflow_dag
