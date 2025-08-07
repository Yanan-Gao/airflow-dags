from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from ttd.ec2.emr_instance_types.memory_optimized.r6id import R6id
from ttd.ec2.emr_instance_types.storage_optimized.i8g import I8g
from ttd.ec2.emr_instance_types.storage_optimized.i4i import I4i
from ttd.ec2.emr_instance_types.storage_optimized.i7ie import I7ie
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.eldorado.base import TtdDag
from datetime import datetime, timedelta, timezone

from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

env = "prod"
dev_user = "rehaan.mehta"
root_bucket = f"s3://thetradedesk-useast-hadoop/unified-data-models/{env}/"

DICT_CONFIG = {
    "dev": {
        "s3_folder": f"dev/{dev_user}/application/unified_model",
        "bootstrap_folder": f"dev/{dev_user}/application/unifiedModel",
        "slack_channel": f"@{dev_user}",
        "action_on_failure": "CONTINUE",
        "jar_location": f"dev/{dev_user}/prod-uber-thetradedesk-data-streaming-0.1.jar"
    },
    "prod": {
        "s3_folder": "application/unified_model",
        "bootstrap_folder": "application/unifiedModel",
        "slack_channel": "#scrum-data-products-alarms",
        "action_on_failure": "TERMINATE_CLUSTER",
        "jar_location": root_bucket + "lib/prod-uber-thetradedesk-data-streaming-0.1.jar",
    }
}

config_jobs_end = OpTask(op=EmptyOperator(task_id='config_jobs_end', trigger_rule="none_failed"))
config_jobs_start = OpTask(op=EmptyOperator(task_id='config_jobs_start', trigger_rule="none_failed"))
config_jobs_start_uid2 = OpTask(op=EmptyOperator(task_id='config_jobs_start_uid2', trigger_rule="none_failed"))

jobs = {  # Check if data exists there or not, if not, then run job that creates it
    config_jobs_start.task_id: "unified-data-models/prod/config/percentileSegments/latest_percentile_config_date/config_percentileThresholds.tsv",
    config_jobs_start_uid2.task_id: "unified-data-models/prod/config/percentileSegments/latest_percentile_config_date/config_UID2percentileThresholds.tsv"
}

name = "unified_model"
aws_conn_id = 'aws_default'
action_on_failure = DICT_CONFIG[env]["action_on_failure"]
start_date = datetime(2022, 8, 31, 23, 0)
schedule_interval = "0 12 * * *"
timeout_hours = 48
slack_channel = DICT_CONFIG[env]["slack_channel"]
s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]
bootstrap_folder = DICT_CONFIG[env]["bootstrap_folder"]
cluster_config_s3_bucket = "thetradedesk-useast-hadoop"
cluster_name_prefix = "DATPRD-Unified-Model"
user_profile_url = "s3://thetradedesk-useast-hadoop/unified-data-models/prod/user-profiles/avails"
instance_timestamp = datetime.now(timezone.utc)
jar_location = DICT_CONFIG[env]["jar_location"]
bootstrap_version = "1.5.2"

tags = {"Environment": env, "Job": name, "Resource": "EMR", "process": "unified_model", "Source": "Airflow", "Team": "DATPRD"}

ec2_key_name = "UsEastDataEngineering"

subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

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
    },
}, {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true",
    },
}]

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


def get_latest_user_profile_date(**kwargs):
    s3_hook = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    source_url_components = user_profile_url.split("/")
    source_url_bucket = source_url_components[2]
    source_url_prefix = "/".join(source_url_components[3:] + [""])
    date_prefixes = s3_hook.list_prefixes(source_url_bucket, source_url_prefix)
    success_prefixes = [prefix for prefix in date_prefixes if s3_hook.check_for_key(prefix + "_SUCCESS", source_url_bucket)]
    string_dates = [prefix.split("/")[-2].lower().replace("date=", "") for prefix in success_prefixes]
    dates = [datetime.strptime(string_date, "%Y-%m-%d") for string_date in string_dates]
    source_most_recent_date = max(dates).strftime("%Y%m%d")
    kwargs['ti'].xcom_push(key='percentile_date', value=source_most_recent_date)


def check_config_jobs_to_run(**kwargs):
    latest_percentile_config_date = kwargs['ti'].xcom_pull(task_ids=get_um_date.task_id, key='percentile_date')
    s3_hook = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    jobs_to_run = [config_jobs_end.task_id]
    for job in jobs:
        s3_prefix = jobs[job]
        if s3_prefix == "":
            continue
        s3_prefix = s3_prefix.replace("latest_percentile_config_date", latest_percentile_config_date)
        if not (s3_hook.check_for_key(s3_prefix, cluster_config_s3_bucket)):
            jobs_to_run.append(job)
    return jobs_to_run


get_um_date = OpTask(op=PythonOperator(task_id='get_um_date', python_callable=get_latest_user_profile_date, provide_context=True))

check_config_jobs = OpTask(
    op=BranchPythonOperator(task_id="check_config_jobs", python_callable=check_config_jobs_to_run, provide_context=True)
)

# /////////////////// PERCENTILE CONFIG JOB ////////////////////////

create_cluster_percentile_config_job = EmrClusterTask(
    name=cluster_name_prefix + "-PercentileConfigJob",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=400
    ),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_percentile_config_job = EmrJobTask(
    name="run_percentile_config_job",
    cluster_specs=create_cluster_percentile_config_job.cluster_specs,
    class_name="com.thetradedesk.data.streaming.unifiedclassification.PercentileInterestConfigJob",
    additional_args_option_pairs_list=[("conf", "spark.driver.maxResultSize=18g"), ("conf", "spark.yarn.am.waitTime=1000s"),
                                       ("conf", "spark.files.ignoreCorruptFiles=true"), ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                                       ("conf", "spark.driver.extraJavaOptions=-Xss2M"),
                                       ("conf", "spark.executor.extraJavaOptions=-Xss2M")],
    deploy_mode="client",
    command_line_arguments=["--configPath=" + root_bucket + "config/percentileSegments/percentileConfigConfig_json"],
    executable_path=jar_location,
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False
)
create_cluster_percentile_config_job.add_sequential_body_task(add_percentile_config_job)
# ////////////////////////////////////////////////////////////

# ////////////////UID2 PERCENTILE CONFIG JOB ////////////////
create_cluster_uid2_percentile_config_job = EmrClusterTask(
    name=cluster_name_prefix + "-Uid2PercentileConfigJob",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=200
    ),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_uid2_percentile_config_job = EmrJobTask(
    name="run_uid2_percentile_config_job",
    cluster_specs=create_cluster_uid2_percentile_config_job.cluster_specs,
    class_name="com.thetradedesk.data.streaming.unifiedclassification.UID2PercentileInterestConfigJob",
    additional_args_option_pairs_list=[("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.files.ignoreCorruptFiles=true"),
                                       ("conf", "spark.sql.files.ignoreCorruptFiles=true")],
    deploy_mode="client",
    command_line_arguments=[
        "--configPath=" + root_bucket + "config/percentileSegments/UID2percentileConfigConfig_json",
        "--uid2ConfigToGenerateDatePartition={{task_instance.xcom_pull(dag_id='" + dag.airflow_dag.dag_id +
        "', task_ids='get_um_date', key='percentile_date')}}"
    ],
    executable_path=jar_location,
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False
)
create_cluster_uid2_percentile_config_job.add_sequential_body_task(add_uid2_percentile_config_job)
# ////////////////////////////////////////////////////////////

# /////////////// AVAILS UNIFIED MODEL //////////////////////
create_cluster_avails_unified_model = EmrClusterTask(
    name=cluster_name_prefix + "-Avails",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            I4i.i4i_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            I7ie.i7ie_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            I8g.i8g_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R6id.r6id_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=512
    ),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    bootstrap_script_actions=
    [ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + bootstrap_folder + "/bin/boot/" + bootstrap_version + "/bootstrap.sh")]
)

add_avails_unified_model = EmrJobTask(
    name="run_avails_unified_model",
    cluster_specs=create_cluster_avails_unified_model.cluster_specs,
    class_name="com.thetradedesk.data.streaming.unifiedclassification.BatchUnifiedModelJobUserHistory",
    additional_args_option_pairs_list=[("conf", "spark.driver.memory=600g"), ("conf", "spark.driver.cores=90"),
                                       ("conf", "spark.driver.maxResultSize=64g"), ("conf", "spark.dynamicAllocation.enabled=false"),
                                       ("conf", "spark.executor.memory=100g"), ("conf", "spark.executor.cores=12"),
                                       ("conf", "spark.executor.instances=512"), ("conf", "spark.files.ignoreCorruptFiles=true"),
                                       ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.default.parallelism=128000"),
                                       ("conf", "spark.sql.shuffle.partitions=128000"), ("conf", "spark.broadcast.blockSize=200000")],
    deploy_mode="client",
    command_line_arguments=[
        "--configPath=" + root_bucket + "config/avails_batch_prod_config_json",
        "--referenceDate=" + '{{ data_interval_end.strftime("%Y-%m-%d") }}'
    ],
    executable_path=jar_location,
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False
)
create_cluster_avails_unified_model.add_sequential_body_task(add_avails_unified_model)
# ////////////////////////////////////////////////////////////

# //////////////////// BIDREQUESTS UNIFIED MODEL //////////////////////

create_cluster_bidrequests_unified_model = EmrClusterTask(
    name=cluster_name_prefix + "-BidRequests",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=35
    ),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    bootstrap_script_actions=
    [ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + bootstrap_folder + "/bin/boot/" + bootstrap_version + "/bootstrap.sh")],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_bidrequests_unified_model = EmrJobTask(
    name="run_bidrequests_unified_model",
    cluster_specs=create_cluster_bidrequests_unified_model.cluster_specs,
    class_name="com.thetradedesk.data.streaming.unifiedclassification.BatchUnifiedModelJobUserHistory",
    additional_args_option_pairs_list=[("conf", "spark.driver.memory=196g"), ("conf", "spark.driver.cores=24"),
                                       ("conf", "spark.driver.maxResultSize=64g"), ("conf", "spark.dynamicAllocation.enabled=false"),
                                       ("conf", "spark.executor.memory=90g"), ("conf", "spark.executor.cores=16"),
                                       ("conf", "spark.executor.instances=35"), ("conf", "spark.files.ignoreCorruptFiles=true"),
                                       ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.default.parallelism=64000"),
                                       ("conf", "spark.sql.shuffle.partitions=64000"), ("conf", "spark.broadcast.blockSize=200000")],
    deploy_mode="client",
    command_line_arguments=[
        "--configPath=" + root_bucket + "config/bidrequest_batch_prod_config_json",
        "--referenceDate=" + '{{ data_interval_end.strftime("%Y-%m-%d") }}'
    ],
    timeout_timedelta=timedelta(hours=16),
    executable_path=jar_location,
    configure_cluster_automatically=False
)
create_cluster_bidrequests_unified_model.add_sequential_body_task(add_bidrequests_unified_model)
# ////////////////////////////////////////////////////////////

# ///////////////////////// TPD PUBLISHING TASK ////////////////////
create_cluster_publishing = EmrClusterTask(
    name=cluster_name_prefix + "-TpdPublishing",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6g.r6g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256),
            R7g.r7g_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256)
        ],
        on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6g.r6g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256),
            R7g.r7g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256)
        ],
        on_demand_weighted_capacity=600
    ),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=600,
    bootstrap_script_actions=[
        ScriptBootstrapAction(path="s3://thetradedesk-useast-hadoop/unified-data-models/prod/bootstrap-configs/copySecretsToEmr.sh"),
        ScriptBootstrapAction(path="s3://" + s3_bucket + "/" + bootstrap_folder + "/bin/boot/" + bootstrap_version + "/bootstrap.sh")
    ],
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

add_publishing_avails_unified_model = EmrJobTask(
    name="run_publishing_avails_unified_model",
    cluster_specs=create_cluster_publishing.cluster_specs,
    class_name="com.thetradedesk.data.streaming.publishing.DataServerTpdPublishJob",
    additional_args_option_pairs_list=[("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.files.ignoreCorruptFiles=true"),
                                       ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.default.parallelism=14400"),
                                       ("conf", "spark.sql.shuffle.partitions=14400"), ("conf", "spark.executor.cores=6"),
                                       ("conf", "spark.executor.memory=40g")],
    deploy_mode="client",
    command_line_arguments=[
        "--tpdUpdatesPath=" + root_bucket + "dataserver-tpd-updates/",
        "--finalUpdatesPath=" + root_bucket + "final-dataserver-tpd-updates/", "--useTestThirdPartyDataProvider=false",
        "--thirdPartyDataAvoidUid2Location=s3://thetradedesk-useast-hadoop/unified-data-models/prod/config/ThirdPartyDataToAvoidUID2.txt"
    ],
    executable_path=jar_location,
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False
)

add_publishing_bidrequests_unified_model = EmrJobTask(
    name="run_publishing_bidrequests_unified_model",
    cluster_specs=create_cluster_publishing.cluster_specs,
    class_name="com.thetradedesk.data.streaming.publishing.DataServerTpdPublishJob",
    additional_args_option_pairs_list=[("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.files.ignoreCorruptFiles=true"),
                                       ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.default.parallelism=6400"),
                                       ("conf", "spark.sql.shuffle.partitions=6400")],
    deploy_mode="client",
    command_line_arguments=[
        "--tpdUpdatesPath=" + root_bucket + "bidrequest-dataserver-tpd-updates/",
        "--finalUpdatesPath=" + root_bucket + "final-bidrequest-dataserver-tpd-updates/", "--useTestThirdPartyDataProvider=false",
        "--thirdPartyDataAvoidUid2Location=s3://thetradedesk-useast-hadoop/unified-data-models/prod/config/ThirdPartyDataToAvoidUID2.txt"
    ],
    executable_path=jar_location,
    timeout_timedelta=timedelta(hours=16),
    configure_cluster_automatically=False
)
create_cluster_publishing.add_sequential_body_task(add_publishing_avails_unified_model)
create_cluster_publishing.add_sequential_body_task(add_publishing_bidrequests_unified_model)
# ////////////////////////////////////////////////////////////

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))

dag >> get_um_date >> check_config_jobs
check_config_jobs >> config_jobs_end
check_config_jobs >> config_jobs_start
check_config_jobs >> config_jobs_start_uid2
config_jobs_start >> create_cluster_percentile_config_job
config_jobs_start_uid2 >> create_cluster_uid2_percentile_config_job
create_cluster_percentile_config_job >> config_jobs_end
create_cluster_uid2_percentile_config_job >> config_jobs_end
config_jobs_end >> create_cluster_bidrequests_unified_model
config_jobs_end >> create_cluster_avails_unified_model
create_cluster_bidrequests_unified_model >> create_cluster_publishing
create_cluster_avails_unified_model >> create_cluster_publishing
create_cluster_publishing >> check

adag = dag.airflow_dag
