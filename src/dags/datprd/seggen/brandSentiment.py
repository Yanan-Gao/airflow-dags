from datetime import datetime, timedelta
from math import floor
from typing import List, Optional

from airflow.operators.empty import EmptyOperator

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

env = "prod"

version = "2.0.3"
s3_bucket = "ttd-datprd-us-east-1"

DICT_CONFIG = {
    "test": {
        "version": f"{version}-SNAPSHOT",
        "s3_folder": "dev/adam.sugarman/application/seggen",
        "application_data_url": "s3://ttd-datprd-us-east-1/backstage",
    },
    "prod": {
        "version": f"{version}",
        "s3_folder": "application/seggen",
        "application_data_url": "s3://ttd-datprd-us-east-1/backstage",
    }
}

s3_folder = DICT_CONFIG[env]["s3_folder"]
aws_conn_id = 'aws_default'
name = "brandSentiment"
group_id = "com.thetradedesk.seggen"
version = DICT_CONFIG[env]["version"]
start_date = datetime(2022, 8, 31, 23, 0)
schedule_interval = "0 */12 * * *"
timeout_hours = 48
slack_channel = "@scrum-data-products-alarms"
cluster_name_prefix = "DATPRD-SegGen-BrandSentiment"
tags = {
    "Environment": env,
    "Job": name,
    "Resource": "EMR",
    "process": "brandSentiment",
    "Source": "Airflow",
    "Team": "DATPRD",
    "Duration": "long"
}
subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

configureGangliaBootstrapActionPath = f"s3://{s3_bucket}/{s3_folder}/brandSentiment/data/configureGangliaAccess.sh"

bootstrapActions = [ScriptBootstrapAction(name="ConfigureGanglia", path=configureGangliaBootstrapActionPath)]

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
    on_demand_weighted_capacity=100,
)

# Avails Ingestion cluster cluster params
avails_ingestion_core_machine_count = 100
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

application_data_url = DICT_CONFIG[env]["application_data_url"]
prometheus_push_gateway_address = "pushgateway.gen.adsrvr.org:80"

log_level = "ERROR"
instance_timestamp = datetime.utcnow()

availsUnsampledLookBackDays = "7"
owdiDemoExportUrl = "s3://ttd-datprd-us-east-1/application/radar/data/export/general/Demographic"
sentimentTrendsConfigUrl = "s3://ttd-data-support-general/SentimentTrends/Config/ConfigurationFile"
sentimentScoresUrl = "s3://ttd-contextual/work/lucy/sentiment_analysis/gitlab_data/v1/url-entity-sentiment"
unsampledAvailsUrl = "s3://thetradedesk-useast-avails/datasets/withPII/prod/identity-avails-agg-hourly-v2-delta"

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
    spark_jars: Optional[List[str]] = None,
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

    if spark_jars:
        joined_jars = ",".join(spark_jars)
        spark_args_final.append(("jars", joined_jars))

    return EmrJobTask(
        name=step_name,
        class_name=group_id + "." + artifact_id + ".Main",
        cluster_specs=cluster_specs,
        executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + group_id + "-" + artifact_id + "/" + version + "/" + group_id +
        "-" + artifact_id + "-" + version + "-all.jar",
        deploy_mode="client",
        additional_args_option_pairs_list=spark_args_final,
        configure_cluster_automatically=False,
        command_line_arguments=command_args,
        openlineage_config=OpenlineageConfig(enabled=True),
        timeout_timedelta=timeout_timedelta,
        action_on_failure="CONTINUE"
    )


create_avails_ingestion_cluster = create_emr_cluster(
    cluster_name="-DailyIngestion",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=master_fleet_instances,
    core_fleet_instances=avails_ingestion_core_fleet_instances,
    bootstrap_actions=bootstrapActions,
)

add__ingest_unsampled_avails = add_emr_spark_step(
    step_name="ingest_unsampled_avails",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory="150G",
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="brandSentiment",
    command_args=[
        "ingestUnsampledAvailsSparkJob",
        "--backstageUrl",
        application_data_url,
        "--applicationDataUrl",
        f"s3://{s3_bucket}/application/seggen/brandSentiment/data",
        "--sentimentTrendsConfigUrl",
        sentimentTrendsConfigUrl,
        "--referenceDate",
        referenceDate,
        "--lookbackDays",
        availsUnsampledLookBackDays,
        "--availsUrl",
        unsampledAvailsUrl,
        "--env",
        env,
    ],
    cluster_specs=create_avails_ingestion_cluster.cluster_specs,
    timeout_timedelta=timedelta(hours=10),
    utilizeMaximizeResourceAllocation=True
)

add__ingest_owdi_demographic_export_data = add_emr_spark_step(
    step_name="ingest_owdi_demographic_export_data",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory="150G",
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="brandSentiment",
    command_args=[
        "ingestOwdiDemographicExportSparkJob",
        "--env",
        env,
        "--backstageUrl",
        application_data_url,
        "--owdiDemoExportUrl",
        owdiDemoExportUrl,
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

add__generate_pulse_segment_users = add_emr_spark_step(
    step_name="generate_pulse_segment_users",
    spark_args=[],
    driver_core_count=avails_ingestion_driver_core_count,
    driver_memory="150G",
    executor_count=avails_ingestion_executor_count,
    executor_core_count=avails_ingestion_executor_core_count,
    executor_memory=avails_ingestion_executor_memory,
    artifact_id="brandSentiment",
    command_args=[
        "pulseSegmentGenerationSparkJob",
        "--backstageUrl",
        application_data_url,
        "--applicationDataUrl",
        f"s3://{s3_bucket}/application/seggen/brandSentiment/data",
        "--sentimentTrendsConfigUrl",
        sentimentTrendsConfigUrl,
        "--sentimentScoresUrl",
        sentimentScoresUrl,
        "--env",
        env,
        "--referenceDateTime",
        referenceDateTime,
    ],
    utilizeMaximizeResourceAllocation=True,
    cluster_specs=create_avails_ingestion_cluster.cluster_specs
)

create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_unsampled_avails)
create_avails_ingestion_cluster.add_sequential_body_task(add__ingest_owdi_demographic_export_data)
create_avails_ingestion_cluster.add_sequential_body_task(add__generate_pulse_segment_users)

daily_ingestion_start_checkpoint = OpTask(op=EmptyOperator(task_id='daily_ingestion_start_checkpoint', ))
daily_ingestion_end_checkpoint = OpTask(op=EmptyOperator(task_id='daily_ingestion_end_checkpoint', trigger_rule="none_failed_or_skipped"))

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))
dag >> daily_ingestion_start_checkpoint >> create_avails_ingestion_cluster >> daily_ingestion_end_checkpoint >> check

adag = dag.airflow_dag
