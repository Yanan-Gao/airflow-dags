from datetime import datetime, timedelta

from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.eldorado.base import TtdDag
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

env = "prod"
dev_user = "kevin.zhang"

DICT_CONFIG = {
    "prod": {
        "version": "2.0.1",
        "s3_folder": "application/seggen",
        "cache_base_url": "s3://ttd-datprd-us-east-1/backstage/90d/prod",
        "output_base_url": "s3://ttd-datprd-us-east-1/prod/data",
        "dataserver_upload_base_url":
        "s3://ttd-datprd-us-east-1/backstage/60d/prod/publishing/tpd/dataProvider=thetradedesk/system=geoPolitical",
        "slack_channel": "#scrum-data-products-alarms",
    },
    "dev": {
        "version": "1.1.7-SNAPSHOT",
        "s3_folder": f"dev/{dev_user}/application/seggen",
        "cache_base_url": f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/seggen/data",
        "output_base_url": f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/seggen/data",
        "dataserver_upload_base_url":
        "s3://ttd-datprd-us-east-1/backstage/60d/prod/publishing/tpd/dataProvider=thetradedesk/system=geoPolitical",
        "slack_channel": f"@{dev_user}",
    }
}

name = "geo_political_segments"
group_id = "com.thetradedesk.seggen-political"
version = DICT_CONFIG[env]["version"]

start_date = datetime(2025, 6, 6, 0, 0)
schedule_interval = "0 6 * * *"
timeout_hours = 16
slack_channel = DICT_CONFIG[env]["slack_channel"]
cluster_name_prefix = "DATPRD-Geo-Political-Segments"
s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]
otel_metrics_exporter_address = "https://opentelemetry-global-gateway.gen.adsrvr.org/http/v1/metrics"

# Input URLs
avails_url = "s3://thetradedesk-useast-avails/datasets/withPII/prod/identity-avails-agg-hourly-v2-delta"
geotargeting_datasets_base_url = f"s3://{s3_bucket}/data/Geotargeting"

# Output URLs
cache_base_url = DICT_CONFIG[env]["cache_base_url"]
output_base_url = DICT_CONFIG[env]["output_base_url"]
dataserver_upload_base_url = DICT_CONFIG[env]["dataserver_upload_base_url"]
latlng_output_base_url = output_base_url + "/ResidenceLatLng/v=1/Country=US"
latlng_cache_base_url = cache_base_url + "/DailyLatLngCache"
polygon_output_publishing_format_base_url = output_base_url + "/GeoPoliticalPolygons/v=1/Country=US"
segment_output_base_url = output_base_url + "/GeoPoliticalSegments/v=1/Country=US"

polygon_types = "county,congressionalDistrict"

instance_types_core = [
    R7gd.r7gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
    R6gd.r6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
]

executor_core_count = 8
executor_memory = "42G"
executor_instance_count = 200
parallelism = executor_core_count * executor_instance_count

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
        "in_cluster": True
    },
    start_date=start_date,
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=timeout_hours),
    schedule_interval=schedule_interval,
    slack_channel=slack_channel,
    slack_alert_only_for_prod=True,
)

tags = {
    "Process": "DATPRD-geo_political_segments",
    "ClusterVersion": "emr-6.12.0",
    "Resource": "EMR",
    "Environment": env,
    "Service": name,
    "Team": "DATPRD",
    "Creator": dev_user,
    "Job": name,
    "Monitoring": "aws_emr",
    "Source": "Airflow",
}

cluster_configs = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.resource.cpu-vcores": str(executor_core_count)
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxRetries": "150",
        "fs.s3.sleepTimeSeconds": "10",
        "fs.s3.maxConnections": "200",
    }
}, {
    "Classification": "spark-env",
    "Properties": {},
    "Configurations": [{
        "Classification": "export",
        "Properties": {
            "JAVA_HOME": "/home/hadoop/.ttd/app/jdk"
        }
    }]
}, {
    "Classification": "spark-defaults",
    "Properties": {
        "spark.executorEnv.JAVA_HOME": "/home/hadoop/.ttd/app/jdk"
    }
}]

ec2_key_name = "UsEastDataEngineering"

subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

cluster_bootscript = [
    ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + s3_folder + "/bin/com.thetradedesk.seggen-boot/" + version + "/bootstrap.sh")
]

java_options = [
    "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens java.base/java.lang=ALL-UNNAMED", "--add-opens java.base/java.math=ALL-UNNAMED",
    "--add-opens java.base/java.util=ALL-UNNAMED", "--add-opens java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens java.base/java.net=ALL-UNNAMED", "--add-opens java.base/java.text=ALL-UNNAMED",
    "--add-opens java.base/java.nio=ALL-UNNAMED", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true", "-Dorg.apache.logging.log4j.level=ERROR", "-verbose:gc",
    "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/tmp"
]

additional_spark_args = [("conf", "spark.executor.extraJavaOptions=" + str(" ".join(java_options))),
                         ("conf", "spark.driver.extraJavaOptions=" + str(" ".join(java_options))),
                         ("conf", "spark.driver.maxResultSize=" + executor_memory), ("conf", "spark.dynamicAllocation.enabled=false"),
                         ("conf", "spark.sql.files.ignoreCorruptFiles=false"), ("conf", "spark.driver.memory=" + executor_memory),
                         ("conf", "spark.driver.cores=" + str(executor_core_count)),
                         ("conf", "spark.executor.instances=" + str(executor_instance_count)),
                         ("conf", "spark.executor.cores=" + str(executor_core_count)),
                         ("conf", "spark.executor.memory=" + str(executor_memory)),
                         ("conf", "spark.default.parallelism=" + str(parallelism)),
                         ("conf", "spark.sql.shuffle.partitions=" + str(parallelism)), ("conf", "spark.sql.broadcastTimeout=600")]

create_cluster_step = EmrClusterTask(
    name=cluster_name_prefix + "-Input-Pipeline",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types_core, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=instance_types_core, on_demand_weighted_capacity=executor_instance_count),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    bootstrap_script_actions=cluster_bootscript,
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4_0,
)

add_residence_latlng_job = EmrJobTask(
    name="run_residence_latlng",
    cluster_specs=create_cluster_step.cluster_specs,
    class_name="com.thetradedesk.seggen.political.Main",
    additional_args_option_pairs_list=additional_spark_args,
    command_line_arguments=[
        "residenceLatLng",
        "--env",
        env,
        "--globalEnv",
        env,
        "--otelMetricsExporterAddress",
        otel_metrics_exporter_address,
        "--availsUrl",
        avails_url,
        "--stateUtcUrl",
        f"{geotargeting_datasets_base_url}/stateUtcMap",
        "--latLngOutputBaseUrl",
        latlng_output_base_url,
        "--cacheOutputBaseUrl",
        latlng_cache_base_url,
    ],
    deploy_mode="client",
    executable_path=f"s3://{s3_bucket}/{s3_folder}/bin/{group_id}/{version}/{group_id}-{version}-all.jar",
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False,
)

add_polygon_segment_job = EmrJobTask(
    name="run_polygon_segment",
    cluster_specs=create_cluster_step.cluster_specs,
    class_name="com.thetradedesk.seggen.political.Main",
    additional_args_option_pairs_list=additional_spark_args,
    command_line_arguments=[
        "polygonSegment",
        "--env",
        env,
        "--globalEnv",
        env,
        "--otelMetricsExporterAddress",
        otel_metrics_exporter_address,
        "--residenceLatLngUrl",
        latlng_output_base_url,
        "--polygonBaseUrl",
        geotargeting_datasets_base_url,
        "--polygonOutputPublishingFormatBaseUrl",
        polygon_output_publishing_format_base_url,
        "--segmentOutputBaseUrl",
        segment_output_base_url,
        "--dataserverUploadBaseUrl",
        dataserver_upload_base_url,
        "--polygonTypes",
        polygon_types,
    ],
    deploy_mode="client",
    executable_path=f"s3://{s3_bucket}/{s3_folder}/bin/{group_id}/{version}/{group_id}-{version}-all.jar",
    timeout_timedelta=timedelta(hours=8),
    configure_cluster_automatically=False
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))

create_cluster_step.add_sequential_body_task(add_residence_latlng_job)
create_cluster_step.add_sequential_body_task(add_polygon_segment_job)

dag >> create_cluster_step >> check
adag = dag.airflow_dag
