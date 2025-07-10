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
dev_user = "rehaan.mehta"

DICT_CONFIG = {
    "dev": {
        "version": "1.0.0-SNAPSHOT",
        "s3_folder": f"dev/{dev_user}/application/data_server",
        "application_data_url": f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/data_server/data",
        "slack_channel": f"@{dev_user}",
    },
    "prod": {
        "version": "2.2.16",
        "s3_folder": "application/data_server",
        "application_data_url": "s3://ttd-datprd-us-east-1/backstage/60d/prod/publishing",
        "slack_channel": "#scrum-data-products-alarms",
    }
}

name = "segment_publishing"
group_id = "com.thetradedesk.data_server-publisher"
version = DICT_CONFIG[env]["version"]

start_date = datetime(2025, 6, 1, 0, 0)
schedule_interval = "0 */6 * * *"
timeout_hours = 192
slack_channel = DICT_CONFIG[env]["slack_channel"]
cluster_name_prefix = "DATPRD-Segment-Publishing"
s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]

publishing_url_fpd = DICT_CONFIG[env]["application_data_url"] + "/fpd"
publishing_url_tpd = DICT_CONFIG[env]["application_data_url"] + "/tpd"

instance_types_core = [
    R7gd.r7gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
    R6gd.r6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
]

executor_core_count = 8
executor_memory = "42G"
executor_instance_count = 100
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
    "process": "segment_publishing",
    "Source": "Airflow",
    "Team": "DATPRD",
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
}]

ec2_key_name = "UsEastDataEngineering"

subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

cluster_bootscript = [ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + s3_folder + "/bin/boot/" + version + "/bootstrap.sh")]

additional_spark_args = [("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.dynamicAllocation.enabled=false"),
                         ("conf", "spark.sql.files.ignoreCorruptFiles=false"), ("conf", "spark.driver.memory=" + executor_memory),
                         ("conf", "spark.driver.cores=" + str(executor_core_count)),
                         ("conf", "spark.executor.instances=" + str(executor_instance_count)),
                         ("conf", "spark.executor.cores=" + str(executor_core_count)),
                         ("conf", "spark.executor.memory=" + str(executor_memory)),
                         ("conf", "spark.default.parallelism=" + str(parallelism)),
                         ("conf", "spark.sql.shuffle.partitions=" + str(parallelism)), ("conf", "spark.sql.broadcastTimeout=600")]

create_cluster_step = EmrClusterTask(
    name=cluster_name_prefix,
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
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_tpd_publishing_task = EmrJobTask(
    name="run_tpd_publishing",
    cluster_specs=create_cluster_step.cluster_specs,
    class_name="com.thetradedesk.dataServer.publisher.Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=["publish", "--env", env, "--inputDataUrl", publishing_url_tpd, "--firstPartyData", "false"],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/publisher/" + version + "/" + group_id + "-" + version + "-all.jar",
    timeout_timedelta=timedelta(hours=24),
    configure_cluster_automatically=False
)

add_fpd_publishing_task = EmrJobTask(
    name="run_fpd_publishing",
    cluster_specs=create_cluster_step.cluster_specs,
    class_name="com.thetradedesk.dataServer.publisher.Main",
    additional_args_option_pairs_list=additional_spark_args,
    deploy_mode="client",
    command_line_arguments=["publish", "--env", env, "--inputDataUrl", publishing_url_fpd, "--firstPartyData", "true"],
    executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/publisher/" + version + "/" + group_id + "-" + version + "-all.jar",
    timeout_timedelta=timedelta(hours=24),
    configure_cluster_automatically=False
)

create_cluster_step.add_sequential_body_task(add_tpd_publishing_task)
create_cluster_step.add_sequential_body_task(add_fpd_publishing_task)
final_check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))
dag >> create_cluster_step >> final_check
adag = dag.airflow_dag
