from datetime import datetime, timedelta
from math import floor
from typing import List, Optional

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.emr_instance_types.general_purpose.m4 import M4
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.openlineage import OpenlineageConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

# We could pass this in as part of dag_run.conf as well; but would require variable such as application_data_url be defined at run time
env = "prod"

version = "1.98.0"
s3_bucket = "ttd-datprd-us-east-1"

# A dictionary to change between "dev" and "prod" parameters; the switching is done through setting `env`
# The thinking behind passing `dev_user` as part of `dag_run.conf` is that one doesnt need to do a `kubectl cp` to change where the DAG writes to; but simply pass the username when triggering a run, to see the output of the DAG
DICT_CONFIG = {
    "test": {
        "version": f"{version}-SNAPSHOT",
        "s3_folder": "dev/adam.sugarman/application/audience_extension",
        "application_data_url": f"s3://{s3_bucket}/dev/adam.sugarman/application/audience_extension/data",
        "job_flow_role": "DataPipelineDefaultResourceRole"
    },
    "prod": {
        "version": f"{version}",
        "s3_folder": "application/audience_extension",
        "application_data_url": f"s3://{s3_bucket}/application/audience_extension/data",
        "job_flow_role": "AudienceExtension"
    }
}

s3_folder = DICT_CONFIG[env]["s3_folder"]
name = "audience_expansion"
aws_conn_id = 'aws_default'
group_id = "com.thetradedesk.audience_extension"
version = DICT_CONFIG[env]["version"]
start_date = datetime(2022, 8, 31, 23, 0)
schedule_interval = "0 */12 * * *"
timeout_hours = 48
slack_channel = "@scrum-data-products-alarms"
cluster_name_prefix = "DATPRD-Audience-Expansion"
tags = {
    "Environment": env,
    "Job": name,
    "Resource": "EMR",
    "process": "audience_expansion",
    "Source": "Airflow",
    "Team": "DATPRD",
    "Duration": "long"
}
subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

copySecretsBoostrapActionPath = f"s3://{s3_bucket}/{s3_folder}/bin/bootstrap/{version}/copySecretsToEmr.sh"
configureGangliaBootstrapActionPath = f"s3://{s3_bucket}/{s3_folder}/bin/bootstrap/{version}/configureGangliaAccess.sh"

bootstrapActions = [
    ScriptBootstrapAction(name="CopySecretsToEmr", path=copySecretsBoostrapActionPath),
    ScriptBootstrapAction(name="ConfigureGanglia", path=configureGangliaBootstrapActionPath)
]

jobFlowRole = DICT_CONFIG[env]["job_flow_role"]

ebs_configuration = {
    'EbsOptimized': True,
    'EbsBlockDeviceConfigs': [{
        'VolumeSpecification': {
            'VolumeType': 'io1',
            'SizeInGB': 256,
            'Iops': 3000
        },
        'VolumesPerInstance': 1
    }]
}
master_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6gd_12xlarge().with_ebs_size_gb(20).with_ebs_iops(3000).with_fleet_weighted_capacity(1),
        M5a.m5a_12xlarge().with_ebs_size_gb(20).with_ebs_iops(3000).with_fleet_weighted_capacity(1)
    ],
    on_demand_weighted_capacity=1
)
avails_ingestion_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7gd_2xlarge().with_fleet_weighted_capacity(1),
        M6g.m6gd_2xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=300,
)
batch_ingestion_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7gd_2xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=100,
)
extension_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7gd_2xlarge().with_fleet_weighted_capacity(1),
        M6g.m6gd_2xlarge().with_fleet_weighted_capacity(1),
        # M7g.m7gd_4xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=200
)
publishing_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M4.m4_xlarge().with_ebs_size_gb(20).with_ebs_iops(3000).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=75
)

staging_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M4.m4_xlarge().with_ebs_size_gb(20).with_ebs_iops(3000).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=10
)

# Avails Ingestion cluster cluster params
avails_ingestion_core_machine_count = 300
avails_ingestion_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
avails_ingestion_master_machine_vcpu = 48
avails_ingestion_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
avails_ingestion_core_machine_vcpu_granularity = 2
avails_ingestion_core_machine_vcpu = 8 * avails_ingestion_core_machine_vcpu_granularity
avails_ingestion_driver_memory_overhead = 0.25
avails_ingestion_executor_memory_overhead = 0.25
avails_ingestion_driver_count = 1
avails_ingestion_executor_count_per_machine = 1
avails_ingestion_driver_core_count = int(floor(avails_ingestion_master_machine_vcpu / avails_ingestion_driver_count))
avails_ingestion_driver_memory = str(
    int(floor(avails_ingestion_master_machine_vmem / (avails_ingestion_driver_count * (1 + avails_ingestion_driver_memory_overhead))))
) + "G"
avails_ingestion_executor_count = avails_ingestion_core_machine_count * avails_ingestion_executor_count_per_machine
avails_ingestion_executor_core_count = int(floor(avails_ingestion_core_machine_vcpu / avails_ingestion_executor_count_per_machine))
avails_ingestion_executor_memory = str(
    int(
        floor(
            avails_ingestion_core_machine_vmem /
            (avails_ingestion_executor_count_per_machine * (1 + avails_ingestion_executor_memory_overhead))
        )
    )
) + "G"
avails_ingestion_default_parallelism = str(avails_ingestion_executor_count * avails_ingestion_executor_core_count)
avails_ingestion_shuffle_partitions = str(avails_ingestion_executor_count * avails_ingestion_executor_core_count)

# Small Ingestion cluster params
small_ingestion_core_machine_count = 100
small_ingestion_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
small_ingestion_master_machine_vcpu = 48
small_ingestion_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
small_ingestion_core_machine_vcpu_granularity = 2
small_ingestion_core_machine_vcpu = 8 * small_ingestion_core_machine_vcpu_granularity
small_ingestion_driver_memory_overhead = 0.25
small_ingestion_executor_memory_overhead = 0.25
small_ingestion_driver_count = 1
small_ingestion_executor_count_per_machine = 1
small_ingestion_driver_core_count = int(floor(small_ingestion_master_machine_vcpu / small_ingestion_driver_count))
small_ingestion_driver_memory = str(
    int(floor(small_ingestion_master_machine_vmem / (small_ingestion_driver_count * (1 + small_ingestion_driver_memory_overhead))))
) + "G"
small_ingestion_executor_count = small_ingestion_core_machine_count * small_ingestion_executor_count_per_machine
small_ingestion_executor_core_count = int(floor(small_ingestion_core_machine_vcpu / small_ingestion_executor_count_per_machine))
small_ingestion_executor_memory = str(
    int(
        floor(
            small_ingestion_core_machine_vmem /
            (small_ingestion_executor_count_per_machine * (1 + small_ingestion_executor_memory_overhead))
        )
    )
) + "G"
small_ingestion_default_parallelism = str(small_ingestion_executor_count * small_ingestion_executor_core_count)
small_ingestion_shuffle_partitions = str(small_ingestion_executor_count * small_ingestion_executor_core_count)

# Extension cluster params
extension_core_machine_count = 200
extension_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
extension_master_machine_vcpu = 48
extension_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
extension_core_machine_vcpu = 8
extension_driver_memory_overhead = 0.25
extension_executor_memory_overhead = 0.20
extension_driver_count = 1
extension_executor_count_per_machine = 1
extension_driver_core_count = int(floor(extension_master_machine_vcpu / extension_driver_count))
extension_driver_memory = str(
    int(floor(extension_master_machine_vmem / (extension_driver_count * (1 + extension_driver_memory_overhead))))
) + "G"
extension_executor_count = extension_core_machine_count * extension_executor_count_per_machine
extension_executor_core_count = int(floor(extension_core_machine_vcpu / extension_executor_count_per_machine))
extension_executor_memory = str(
    int(floor(extension_core_machine_vmem / (extension_executor_count_per_machine * (1 + extension_executor_memory_overhead))))
) + "G"
extension_default_parallelism = str(extension_executor_count * extension_executor_core_count)
extension_shuffle_partitions = str(extension_executor_count * extension_executor_core_count)

# Publishing cluster params
publishing_core_machine_count = 75
publishing_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
publishing_master_machine_vcpu = 48
publishing_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
publishing_core_machine_vcpu = 8
publishing_driver_memory_overhead = 0.25
publishing_executor_memory_overhead = 0.25
publishing_driver_count = 1
publishing_executor_count_per_machine = 1
publishing_driver_core_count = int(floor(publishing_master_machine_vcpu / publishing_driver_count))
publishing_driver_memory = str(
    int(floor(publishing_master_machine_vmem / (publishing_driver_count * (1 + publishing_driver_memory_overhead))))
) + "G"
publishing_executor_count = publishing_core_machine_count * publishing_executor_count_per_machine
publishing_executor_core_count = int(floor(publishing_core_machine_vcpu / publishing_executor_count_per_machine))
publishing_executor_memory = str(
    int(floor(publishing_core_machine_vmem / (publishing_executor_count_per_machine * (1 + publishing_executor_memory_overhead))))
) + "G"
publishing_default_parallelism = str(publishing_executor_count * publishing_executor_core_count)
publishing_shuffle_partitions = str(publishing_executor_count * publishing_executor_core_count)

# Staging cluster params
staging_core_machine_count = 10
staging_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
staging_master_machine_vcpu = 48
staging_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
staging_core_machine_vcpu = 8
staging_driver_memory_overhead = 0.25
staging_executor_memory_overhead = 0.25
staging_driver_count = 1
staging_executor_count_per_machine = 1
staging_driver_core_count = int(floor(staging_master_machine_vcpu / staging_driver_count))
staging_driver_memory = str(int(floor(staging_master_machine_vmem / (staging_driver_count * (1 + staging_driver_memory_overhead))))) + "G"
staging_executor_count = staging_core_machine_count * staging_executor_count_per_machine
staging_executor_core_count = int(floor(staging_core_machine_vcpu / staging_executor_count_per_machine))
staging_executor_memory = str(
    int(floor(staging_core_machine_vmem / (staging_executor_count_per_machine * (1 + staging_executor_memory_overhead))))
) + "G"
staging_default_parallelism = str(staging_executor_count * staging_executor_core_count)
staging_shuffle_partitions = str(staging_executor_count * staging_executor_core_count)

application_data_url = DICT_CONFIG[env]["application_data_url"]
prometheus_push_gateway_address = "pushgateway.gen.adsrvr.org:80"

log_level = "ERROR"
instance_timestamp = datetime.utcnow()

availsLookBackDays = "3"
availsUnsampledLookBackDays = "7"
impressionLookBackDays = "24"
lookBackDays = "30"
xdLookBackDays = "30"

availsTapadUrl = "s3://thetradedesk-useast-partners-avails/tapad"
availsUnsampledUrl = "s3://thetradedesk-useast-avails/datasets/withPII/prod/identity-avails-agg-daily-v2-delta/"
bidFeedbackUrl = "s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=5"
dataServerUrl = "https://data-internal.adsrvr.org"
iso3166Url = "s3://ttd-datprd-us-east-1/data/Iso3166/v=1"
owdiDemoExportUrl = "s3://ttd-datprd-us-east-1/application/radar/data/export/general/Demographic"
providerDataImportUrl = "s3://thetradedesk-useast-data-import"
provisioningDomain = "ops.adsrvr.org"
provisioningSiteClassificationUrl = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/siteclassification/v=1"
provisioningUrl = "jdbc:sqlserver://provdb.adsrvr.org:1433;database=Provisioning;authenticationScheme=NTLM"
provisioningBiUrl = "jdbc:sqlserver://provdb-bi.adsrvr.org:1433;database=Provisioning;authenticationScheme=NTLM"
sharedResourceUrl = f"s3://{s3_bucket}/data"
staticExpandedGeoElementUrl = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/staticexpandedgeoelements/v=1"
supplyVendorUrl = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendor/v=1/"
userProfileUrl = "s3://thetradedesk-useast-hadoop/unified-data-models/prod/user-profiles/avails"
verticaDbUrl = "jdbc:vertica://vertdb.adsrvr.org"
xdGraphUrl = "s3://thetradedesk-useast-data-import/sxd-etl/universal/nextgen"
cxtCategoryResultsUrl = "s3://ttd-contextual/prod/parquet/cxt_results/v=2"
cxtCategoryResultsThresholdPercentage = ".3"
cxtCategoryMinMatchedKeywordCount = "250"
ttdDataSupportPiiUrl = "s3://ttd-data-support-pii/AudienceExtension"
xdUserDownSelectionThreshold = "100"
xdUserDownSelectionFraction = "1"

job_instance_s3_folder = s3_folder + "/instance/" + instance_timestamp.isoformat()
job_emr_cluster_name = "DATPRD-AudienceExpansion"

referenceDate = "{{macros.ds_add(ds, 0)}}"
referenceDateTime = "{{ ts | replace('+00:00', '') }}"

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
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=timeout_hours),
    schedule_interval=schedule_interval,
    on_failure_callback=dag_post_to_slack_callback(dag_name=name, step_name="", slack_channel=slack_channel),
    slack_alert_only_for_prod=True
)


def create_emr_cluster(
    cluster_name,
    s3_bucket,
    s3_folder,
    master_fleet_instances,
    core_fleet_instances,
    bootstrap_actions: List[ScriptBootstrapAction],
):
    return EmrClusterTask(
        name=cluster_name_prefix + cluster_name,
        master_fleet_instance_type_configs=master_fleet_instances,
        core_fleet_instance_type_configs=core_fleet_instances,
        emr_release_label="emr-6.15.0",
        cluster_tags=tags,
        log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
        additional_application=["Hadoop", "Spark", "Ganglia", "Zeppelin"],
        additional_application_configurations=[{
            "Classification": "yarn-site",
            "Properties": {
                "yarn.nodemanager.vmem-check-enabled": "false",
                "yarn.nodemanager.pmem-check-enabled": "false",
                "yarn.nodemanager.resource.cpu-vcores": "8"
            }
        }, {
            "Classification": "emrfs-site",
            "Properties": {
                "fs.s3.aimd.enabled": "true",
                "fs.s3.aimd.maxAttempts": "600000",
                "fs.s3.multipart.part.attempts": "600000",
                "fs.s3.maxRetries": "600000",
                "fs.s3.maxConnections": "500",
            },
        }, {
            "Classification": "core-site",
            "Properties": {
                "fs.s3.multipart.th.fraction.parts.complete": "0.99"
            }
        }, {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true",
            }
        }],
        ec2_key_name="UsEastDataEngineering",
        ec2_subnet_ids=subnet_ids,
        bootstrap_script_actions=bootstrap_actions,
        cluster_auto_termination_idle_timeout_seconds=900
    )


def add_emr_spark_step(
    step_name,
    spark_args,
    driver_core_count,
    driver_memory,
    executor_count,
    executor_core_count,
    executor_memory,
    artifact_id,
    command_args,
    cluster_specs: Optional[EmrClusterSpecs] = None,
    utilizeMaximizeResourceAllocation=False,
    timeout_timedelta: timedelta = timedelta(hours=5),
):
    spark_conf = {
        "spark.driver.memory": driver_memory,
        "spark.driver.cores": str(driver_core_count),
        "spark.driver.maxResultSize": driver_memory,
        "spark.executor.instances": str(executor_count),
        "spark.executor.cores": str(executor_core_count),
        "spark.executor.memory": executor_memory,
        "spark.executor.extraJavaOptions": "-Dcom.amazonaws.sdk.retryMode=STANDARD -Dcom.amazonaws.sdk.maxAttempts=15000",
        # "spark.task.cpus": str(executor_core_count_per_task),
        "spark.dynamicAllocation.enabled": "false",
        "spark.sql.files.ignoreCorruptFiles": "false",
        "spark.default.parallelism": str(executor_count * executor_core_count),
        "spark.sql.shuffle.partitions": str(executor_count * executor_core_count)
    }

    maximizeResourceAllocationSetKeys = [
        "spark.driver.memory",
        "spark.driver.cores",
        "spark.driver.maxResultSize",
        "spark.executor.instances",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.task.cpus",
        "spark.default.parallelism",
    ]

    # Process spark_args into a dictionary
    spark_args_dict = {}
    for i in range(0, len(spark_args)):
        # Remove '--conf' and split key-value pair
        key, value = spark_args[i].split('=')
        spark_args_dict[key] = value

    spark_conf.update(spark_args_dict)

    if utilizeMaximizeResourceAllocation:
        for key in maximizeResourceAllocationSetKeys:
            if key in spark_conf:
                del spark_conf[key]

    spark_args_final = []
    for key, value in spark_conf.items():
        spark_args_final.append(("conf", "{}={}".format(key, value)))

    return EmrJobTask(
        name=step_name,
        class_name=group_id + "." + artifact_id + ".Main",
        cluster_specs=cluster_specs,
        executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
        "-" + version + "-all.jar",
        deploy_mode="client",
        additional_args_option_pairs_list=spark_args_final,
        configure_cluster_automatically=False,
        command_line_arguments=command_args,
        openlineage_config=OpenlineageConfig(enabled=False),
        timeout_timedelta=timeout_timedelta,
        action_on_failure="CONTINUE"
    )


def verify_daily_ingestion_success_fn(referenceDate, **kwargs):
    aws_cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    url_components = application_data_url.split("/")
    url_bucket = url_components[2]
    url_key = "/".join(url_components[3:] + ["backstage"] + ["supply-vendor"] + [f"Date={referenceDate}/_SUCCESS"])
    print("date: " + referenceDate)
    print("url_bucket: " + url_bucket)
    print("url_key: " + url_key)
    keys = aws_cloud_storage.list_keys(bucket_name=url_bucket, prefix=url_key)
    if keys is None or len(keys) == 0:
        print("Key does not exist")
        return "daily_ingestion_start_checkpoint"
    else:
        print("Key exists")
        return "daily_ingestion_end_checkpoint"


verify_daily_ingestion_task = OpTask(
    op=BranchPythonOperator(
        task_id="verify_daily_ingestion",
        python_callable=verify_daily_ingestion_success_fn,
        op_args=[referenceDate],
        provide_context=True,
    )
)

daily_ingestion_start_checkpoint = OpTask(op=EmptyOperator(task_id='daily_ingestion_start_checkpoint', ))

daily_ingestion_end_checkpoint = OpTask(op=EmptyOperator(task_id='daily_ingestion_end_checkpoint', trigger_rule="none_failed_or_skipped"))

create_avails_ingestion_cluster = create_emr_cluster(
    cluster_name="-DailyIngestion",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=master_fleet_instances,
    core_fleet_instances=avails_ingestion_core_fleet_instances,
    bootstrap_actions=bootstrapActions,
)

add__ingest_restricted_site = add_emr_spark_step(
    step_name="ingest_restricted_site",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory=avails_ingestion_driver_memory,
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-restricted-site", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate",
        referenceDate, "--lookBackDays", lookBackDays, "--provisioningSiteClassificationUrl", provisioningSiteClassificationUrl,
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

add__ingest_supply_vendor = add_emr_spark_step(
    step_name="ingest_supply_vendor",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory=avails_ingestion_driver_memory,
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-supply-vendor", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate",
        referenceDate, "--lookBackDays", lookBackDays, "--supplyVendorUrl", supplyVendorUrl, "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

add__ingest_avails = add_emr_spark_step(
    step_name="ingest_avails",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory="150G",
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-avails", "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", referenceDate,
        "--computationDays", availsLookBackDays, "--availsUrl", availsTapadUrl, "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address
    ],
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

add__ingest_unsampled_avails = add_emr_spark_step(
    step_name="ingest_unsampled_avails",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory="150G",
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-unsampled-avails",
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        referenceDate,
        "--lookbackDays",
        availsUnsampledLookBackDays,
        "--availsUrl",
        availsUnsampledUrl,
        "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address  # , "--ingestionSuccessFileOverride"
    ],
    cluster_specs=create_avails_ingestion_cluster.cluster_specs,
    timeout_timedelta=timedelta(hours=10),
    utilizeMaximizeResourceAllocation=True
)

add__ingest_owdi_demographic_export_data = add_emr_spark_step(
    step_name="ingest_owdi_demographic_export_data",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory=avails_ingestion_driver_memory,
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-owdi-demo-export", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--owdiDemoExportUrl", owdiDemoExportUrl, "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

add__ingest_cxt_category_results = add_emr_spark_step(
    step_name="ingest_cxt_category_results",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory=avails_ingestion_driver_memory,
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-cxt-category-result", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--lookBackDays", "1", "--referenceDate", referenceDate, "--cxtCategoryResultsUrl", cxtCategoryResultsUrl,
        "--cxtCategoryResultsThresholdPercentage", cxtCategoryResultsThresholdPercentage, "--cxtCategoryMinMatchedKeywordCount",
        cxtCategoryMinMatchedKeywordCount, "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

add__ingest_static_expanded_geo_element_data = add_emr_spark_step(
    step_name="ingest_static_expanded_geo_element_data",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory=avails_ingestion_driver_memory,
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-static-expanded-geo-element", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDate", referenceDate, "--staticExpandedGeoElementUrl", staticExpandedGeoElementUrl, "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_restricted_site)
create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_supply_vendor)
# create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_avails)
create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_unsampled_avails)
create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_owdi_demographic_export_data)
create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_cxt_category_results)
create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_static_expanded_geo_element_data)

batch_ingestion_start_checkpoint = OpTask(op=EmptyOperator(task_id='batch_ingestion_start_checkpoint', ))

dag >> verify_daily_ingestion_task
verify_daily_ingestion_task >> daily_ingestion_start_checkpoint
verify_daily_ingestion_task >> daily_ingestion_end_checkpoint

daily_ingestion_start_checkpoint >> create_avails_ingestion_cluster

create_avails_ingestion_cluster >> daily_ingestion_end_checkpoint
daily_ingestion_end_checkpoint >> batch_ingestion_start_checkpoint

create_small_ingestion_cluster = create_emr_cluster(
    cluster_name="-BatchIngestion",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=master_fleet_instances,
    core_fleet_instances=batch_ingestion_core_fleet_instances,
    bootstrap_actions=bootstrapActions,
)

create_staging_ingestion_cluster = create_emr_cluster(
    cluster_name="-StagingIngestion",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=master_fleet_instances,
    core_fleet_instances=staging_core_fleet_instances,
    bootstrap_actions=bootstrapActions,
)

add__ingest_provisioningdb_configuration = add_emr_spark_step(
    step_name="ingest_provisioningdb_configuration",
    spark_args=[],
    driver_core_count=small_ingestion_driver_core_count,
    driver_memory=small_ingestion_driver_memory,
    executor_count=small_ingestion_executor_count,
    executor_core_count=small_ingestion_executor_core_count,
    executor_memory=small_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-provdb-configuration", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--provisioningDbUrl", provisioningBiUrl, "--provisioningDbDomain", provisioningDomain,
        "--provisioningDbUser", "@/home/hadoop/.ttd/audience_extension/PROVISIONING_DB_USER", "--provisioningDbPassword",
        "@/home/hadoop/.ttd/audience_extension/PROVISIONING_DB_PASS", "--prometheusPushGatewayAddress", prometheus_push_gateway_address,
        "--activeAdgroupCountryCount", "3", "--inActiveAdgroupCountryCount", "1"
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_small_ingestion_cluster.cluster_specs
)

add__ingest_precomputed_relevant_site_data = add_emr_spark_step(
    step_name="ingest_precomputed_relevant_site",
    spark_args=[],
    driver_core_count=small_ingestion_driver_core_count,
    driver_memory=small_ingestion_driver_memory,
    executor_count=small_ingestion_executor_count,
    executor_core_count=small_ingestion_executor_core_count,
    executor_memory=small_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-precomputed-relevant-site", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--ttdDataSupportPiiPreformattedDataUrl", ttdDataSupportPiiUrl + "/precomputedSites",
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_small_ingestion_cluster.cluster_specs
)

add__ingest_data_support_s3_configs = add_emr_spark_step(
    step_name="ingest_data_support_s3_configs",
    spark_args=[],
    driver_core_count=small_ingestion_driver_core_count,
    driver_memory=small_ingestion_driver_memory,
    executor_count=small_ingestion_executor_count,
    executor_core_count=small_ingestion_executor_core_count,
    executor_memory=small_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-data-support-configuration", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--verticaDbUrl", verticaDbUrl, "--verticaDbUser",
        "@/home/hadoop/.ttd/audience_extension/VERTICA_DB_USER", "--verticaDbPassword",
        "@/home/hadoop/.ttd/audience_extension/VERTICA_DB_PASS", "--ttdDataSupportPiiPreformattedDataUrl",
        ttdDataSupportPiiUrl + "/preformattedSeedsV2", "--prometheusPushGatewayAddress", prometheus_push_gateway_address,
        "--availsLookback", availsUnsampledLookBackDays
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_small_ingestion_cluster.cluster_specs
)

add__ingest_partner_s3_metadata = add_emr_spark_step(
    step_name="ingest_partner_s3_metadata",
    spark_args=[],
    driver_core_count=small_ingestion_driver_core_count,
    driver_memory=small_ingestion_driver_memory,
    executor_count=small_ingestion_executor_count,
    executor_core_count=small_ingestion_executor_core_count,
    executor_memory=small_ingestion_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-partner-s3-metadata", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--provisioningDbUrl", provisioningBiUrl, "--provisioningDbDomain", provisioningDomain,
        "--provisioningDbUser", "@/home/hadoop/.ttd/audience_extension/PROVISIONING_DB_USER", "--provisioningDbPassword",
        "@/home/hadoop/.ttd/audience_extension/PROVISIONING_DB_PASS", "--providerDataImportUrl", providerDataImportUrl,
        "--contextualLookBackDays", "7", "--appSharedResourceUrl", sharedResourceUrl, "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address, "--dentsuTaxonomyAdoptionTimestamp", "2025-03-19"
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_small_ingestion_cluster.cluster_specs
)

add__stage_eligible_configuration = add_emr_spark_step(
    step_name="stage_eligible_configuration",
    spark_args=[],
    driver_core_count=staging_driver_core_count,
    driver_memory=staging_driver_memory,
    executor_count=staging_executor_count,
    executor_core_count=staging_executor_core_count,
    executor_memory=staging_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "stage-eligible-configuration", "--env", "prod", "--logLevel", "ERROR", "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--percentConfigsToSample", "1.0", "--stagingLookbackHours", "24", "--provisioningDbUrl",
        provisioningBiUrl, "--provisioningDbDomain", provisioningDomain, "--provisioningDbUser",
        "@/home/hadoop/.ttd/audience_extension/AUDIENCE_EXPANSION_PROVISIONING_DB_USER", "--provisioningDbPassword",
        "@/home/hadoop/.ttd/audience_extension/AUDIENCE_EXPANSION_PROVISIONING_DB_PASS", "--prometheusPushGatewayAddress",
        prometheus_push_gateway_address, "--activeRefreshPublishingFolderLookbackDays", "7", "--disableGeoRailFilter=false",
        "--removeNonActiveSegmentCountryPairs=true", "--defaultRefreshPublishingFolderLookbackDays", "24",
        "--tpdActiveGeoCountriesBatchSize", "50", "--activeAdgroupThreadPoolCount", "1"
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_staging_ingestion_cluster.cluster_specs
)

add__batch_extension_stages = add_emr_spark_step(
    step_name="batch_extension_stages",
    spark_args=[],
    driver_core_count=staging_driver_core_count,
    driver_memory=staging_driver_memory,
    executor_count=staging_executor_count,
    executor_core_count=staging_executor_core_count,
    executor_memory=staging_executor_memory,
    artifact_id="extension",
    command_args=[
        "batch-extension-stages", "--env", "prod", "--applicationDataUrl", application_data_url, "--referenceDateTime", referenceDateTime,
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address, "--mediumCountryBatchSizeFactor", "2.5",
        "--smallCountryBatchSizeFactor", "4"
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_staging_ingestion_cluster.cluster_specs
)


def verify_empty_batch_fn(referenceDateTime, **kwargs):
    aws_cloud_storage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    url_components = application_data_url.split("/")
    url_bucket = url_components[2]
    url_key = "/".join(url_components[3:] + ["backstage"] + ["extension-batch"] + [f"Date={referenceDateTime}/_EMPTYBATCH"])
    print("date: " + referenceDate)
    print("url_bucket: " + url_bucket)
    print("url_key: " + url_key)
    keys = aws_cloud_storage.list_keys(bucket_name=url_bucket, prefix=url_key)
    if keys is None or len(keys) == 0:
        print("Empty batch key does not exist")
        return "extension_start_checkpoint"
    else:
        print("Empty batch key exists")
        return "final_dag_status"


verify_empty_batch_task = OpTask(
    op=BranchPythonOperator(
        task_id="verify_empty_batch",
        python_callable=verify_empty_batch_fn,
        op_args=[referenceDateTime],
        provide_context=True,
    )
)

create_small_ingestion_cluster.add_sequential_body_task(add__ingest_provisioningdb_configuration)
create_small_ingestion_cluster.add_sequential_body_task(add__ingest_precomputed_relevant_site_data)
create_small_ingestion_cluster.add_sequential_body_task(add__ingest_data_support_s3_configs)
create_small_ingestion_cluster.add_sequential_body_task(add__ingest_partner_s3_metadata)
batch_ingestion_start_checkpoint >> create_small_ingestion_cluster

create_staging_ingestion_cluster.add_sequential_body_task(add__stage_eligible_configuration)
create_staging_ingestion_cluster.add_sequential_body_task(add__batch_extension_stages)

extension_start_checkpoint = OpTask(op=EmptyOperator(task_id='extension_start_checkpoint', ))
create_small_ingestion_cluster >> create_staging_ingestion_cluster >> verify_empty_batch_task >> extension_start_checkpoint

create_extension_cluster = create_emr_cluster(
    cluster_name="-Extension",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=master_fleet_instances,
    core_fleet_instances=extension_core_fleet_instances,
    bootstrap_actions=bootstrapActions,
)

add__ingest_preformatted_data = add_emr_spark_step(
    step_name="ingest_preformatted_data",
    spark_args=[],
    driver_core_count=extension_driver_core_count,
    driver_memory=extension_driver_memory,
    executor_count=extension_executor_count,
    executor_core_count=extension_executor_core_count,
    executor_memory=extension_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-preformatted-data", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--ttdDataSupportPiiPreformattedDataUrl", ttdDataSupportPiiUrl + "/preformattedSeedsV2",
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_extension_cluster.cluster_specs
)

add__ingest_dmp_data_segment_service = add_emr_spark_step(
    step_name="ingest_dmp_data_segment_service",
    spark_args=[],
    driver_core_count=extension_driver_core_count,
    driver_memory=extension_driver_memory,
    executor_count=extension_executor_count,
    executor_core_count=extension_executor_core_count,
    executor_memory=extension_executor_memory,
    artifact_id="ingestion",
    command_args=[
        "ingest-dmp-data-segment-service", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_extension_cluster.cluster_specs
)

add__extension_extend_seed_data = add_emr_spark_step(
    step_name="extension_extend_seed_data",
    spark_args=[
        "spark.sql.shuffle.partitions=" + str(extension_executor_count * extension_executor_core_count * 2),
        "spark.default.parallelism=" + str(extension_executor_count * extension_executor_core_count * 2), "spark.sql.adaptive.enabled=true",
        "spark.sql.adaptive.skewJoin.enabled=true"
    ],
    driver_core_count=extension_driver_core_count,
    driver_memory=extension_driver_memory,
    executor_count=extension_executor_count,
    executor_core_count=extension_executor_core_count,
    executor_memory=extension_executor_memory,
    artifact_id="extension",
    command_args=[
        "extend-seed-data", "--env", "prod", "--logLevel", "WARN", "--applicationDataUrl", application_data_url, "--referenceDateTime",
        referenceDateTime, "--lalImpressionDays", availsUnsampledLookBackDays, "--lalMaxSiteRank", "2500", "--xdLookBackDays",
        xdLookBackDays, "--xdGraphUrl", xdGraphUrl, "--xdUserDownSelectionThreshold", xdUserDownSelectionThreshold,
        "--xdUserDownSelectionFraction", xdUserDownSelectionFraction, "--lalMinImpressionsForSite", "500", "--lalMinSiteScore", "0.5",
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address, "--lalNonActiveMaxSiteRank", "500"
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_extension_cluster.cluster_specs,
    timeout_timedelta=timedelta(hours=40)
)

create_extension_cluster.add_sequential_body_task(add__ingest_preformatted_data)
create_extension_cluster.add_sequential_body_task(add__ingest_dmp_data_segment_service)
create_extension_cluster.add_sequential_body_task(add__extension_extend_seed_data)
extension_start_checkpoint >> create_extension_cluster

publishing_start_checkpoint = OpTask(op=EmptyOperator(task_id='publishing_start_checkpoint', ))
create_extension_cluster >> publishing_start_checkpoint

create_publishing_cluster = create_emr_cluster(
    cluster_name="-Publishing",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=master_fleet_instances,
    core_fleet_instances=publishing_core_fleet_instances,
    bootstrap_actions=bootstrapActions,
)

add__publishing_extend_seed_data = add_emr_spark_step(
    step_name="add__publishing_extend_seed_data",
    spark_args=[],
    driver_core_count=publishing_driver_core_count,
    driver_memory=publishing_driver_memory,
    executor_count=publishing_executor_count,
    executor_core_count=publishing_executor_core_count,
    executor_memory=publishing_executor_memory,
    artifact_id="publishing",
    command_args=[
        "publish-extended-data", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--provisioningDbUrl", provisioningUrl, "--provisioningDbDomain", provisioningDomain, "--provisioningDbUser",
        "@/home/hadoop/.ttd/audience_extension/PROVISIONING_DB_USER", "--provisioningDbPassword",
        "@/home/hadoop/.ttd/audience_extension/PROVISIONING_DB_PASS", "--audienceExpansionProvisioningUser",
        "@/home/hadoop/.ttd/audience_extension/AUDIENCE_EXPANSION_PROVISIONING_DB_USER", "--audienceExpansionProvisioningPassword",
        "@/home/hadoop/.ttd/audience_extension/AUDIENCE_EXPANSION_PROVISIONING_DB_PASS", "--dataServerUrl", dataServerUrl,
        "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_publishing_cluster.cluster_specs,
    timeout_timedelta=timedelta(hours=30)
)

add__publishing_partner_s3_metadata = add_emr_spark_step(
    step_name="add__publishing_partner_s3_metadata",
    spark_args=[],
    driver_core_count=publishing_driver_core_count,
    driver_memory=publishing_driver_memory,
    executor_count=publishing_executor_count,
    executor_core_count=publishing_executor_core_count,
    executor_memory=publishing_executor_memory,
    artifact_id="publishing",
    command_args=[
        "publish-partner-s3-metadata", "--env", "prod", "--logLevel", log_level, "--applicationDataUrl", application_data_url,
        "--referenceDateTime", referenceDateTime, "--providerDataImportUrl", providerDataImportUrl, "--publishingLookBackDays",
        impressionLookBackDays, "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_publishing_cluster.cluster_specs
)
create_publishing_cluster.add_sequential_body_task(add__publishing_extend_seed_data)
create_publishing_cluster.add_sequential_body_task(add__publishing_partner_s3_metadata)
publishing_start_checkpoint >> create_publishing_cluster
check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))
create_publishing_cluster >> check

adag = dag.airflow_dag
