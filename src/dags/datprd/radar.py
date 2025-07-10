from airflow.operators.bash import BashOperator
from airflow import AirflowException
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.storage_optimized.i4g import I4g
from ttd.ec2.emr_instance_types.graphics_optimized.g5 import G5
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from math import floor
from airflow.operators.python import PythonOperator, BranchPythonOperator
import time

env = "prod"
dev_user = "rehaan.mehta"
version = "2.0.98"


class Config:

    def __init__(self, version: str, s3_folder: str, application_data_url: str, slack_channel: str, data_provider_id: str):
        self.version = version
        self.s3_folder = s3_folder
        self.application_data_url = application_data_url
        self.slack_channel = slack_channel
        self.data_provider_id = data_provider_id


DICT_CONFIG = {
    "dev":
    Config(
        version=f"{version}-SNAPSHOT",
        s3_folder=f"dev/{dev_user}/application/radar",
        application_data_url=f"s3://ttd-datprd-us-east-1/dev/{dev_user}/application/radar/data",
        slack_channel=f"@{dev_user}",
        data_provider_id="eltoro",
    ),
    "prod":
    Config(
        version=f"{version}",
        s3_folder="application/radar",
        application_data_url="s3://ttd-datprd-us-east-1/application/radar/data",
        slack_channel="#scrum-data-products-alarms",
        data_provider_id="thetradedesk",
    ),
}

name = "radar"
group_id = "com.thetradedesk." + name
version = DICT_CONFIG[env].version


class TrainClusterParams:

    def __init__(self, countries: list[str], executor_count: int, max_train_user_count: int):
        self.countries: list[str] = countries
        self.executor_count: int = executor_count
        self.max_train_user_count: int = max_train_user_count


train_cluster_params_sets = [
    TrainClusterParams(countries=["ES", "GB", "JP", "TH", "HK", "PL", "BE"], executor_count=8, max_train_user_count=5000000),
    TrainClusterParams(countries=["AU", "FR", "ID", "PH", "HU", "NL", "AE"], executor_count=8, max_train_user_count=5000000),
    TrainClusterParams(countries=["CA", "DE", "IT", "SG", "MX"], executor_count=8, max_train_user_count=5000000),
    TrainClusterParams(countries=["NZ", "TW", "MY", "NO", "GR", "CZ", "ZA", "BR"], executor_count=8, max_train_user_count=5000000),
    TrainClusterParams(countries=["US", "IN"], executor_count=16, max_train_user_count=10000000),
]

countries = [
    "ES", "GB", "JP", "TH", "HK", "PL", "BE", "AU", "FR", "ID", "PH", "HU", "NL", "AE", "CA", "DE", "IT", "SG", "MX", "NZ", "TW", "MY",
    "NO", "GR", "CZ", "ZA", "BR", "US", "IN", "AF", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AT", "AZ", "BS",
    "BH", "BD", "BB", "BY", "BZ", "BJ", "BM", "BT", "BO", "BQ", "BA", "BW", "BV", "IO", "VG", "BN", "BG", "BF", "BI", "CV", "KH", "CM",
    "KY", "CF", "TD", "CL", "CN", "MO", "CX", "CC", "CO", "KM", "CG", "CK", "CR", "HR", "CU", "CW", "CY", "CI", "KP", "CD", "DK", "DJ",
    "DM", "DO", "EC", "EG", "SV", "GQ", "ER", "EE", "SZ", "ET", "FK", "FO", "FJ", "FI", "GF", "PF", "TF", "GA", "GM", "GE", "GH", "GI",
    "GL", "GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY", "HT", "HM", "VA", "HN", "IS", "IR", "IQ", "IE", "IM", "IL", "JM", "JE", "JO",
    "KZ", "KE", "KI", "KW", "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI", "LT", "LU", "MG", "MW", "MV", "ML", "MT", "MH", "MQ", "MR",
    "MU", "YT", "FM", "MC", "MN", "ME", "MS", "MA", "MZ", "MM", "NA", "NR", "NP", "NC", "NI", "NE", "NG", "NU", "NF", "MP", "OM", "PK",
    "PW", "PA", "PG", "PY", "PE", "PN", "PT", "PR", "QA", "KR", "MD", "RO", "RU", "RW", "RE", "BL", "SH", "KN", "LC", "MF", "PM", "VC",
    "WS", "SM", "ST", "SA", "SN", "RS", "SC", "SL", "SX", "SK", "SI", "SB", "SO", "GS", "SS", "LK", "PS", "SD", "SR", "SJ", "SE", "CH",
    "SY", "TJ", "MK", "TL", "TG", "TK", "TO", "TT", "TN", "TR", "TM", "TC", "TV", "UG", "UA", "TZ", "UM", "VI", "UY", "UZ", "VU", "VE",
    "VN", "WF", "EH", "YE", "ZM", "ZW", "AX"
]
model_countries: list[str] = [country for countries in list(map(lambda x: x.countries, train_cluster_params_sets)) for country in countries]
private_model_countries = ["US"]
public_model_countries = [e for e in model_countries if e not in private_model_countries]
motp_countries = ["XX"]
sw_countries = ["XX"]
gdpr_countries = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GB", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL",
    "PT", "RO", "SK", "SI", "ES", "SE"
]
extended_segment_countries = ["IN"]

countriesWithGlobal = countries.copy()
countriesWithGlobal.append(
    "Global"
)  # Handling global segments properly (not as a country) to be implemented in https://atlassian.thetradedesk.com/jira/browse/DATPRD-1854

look_back_days = 60

start_date = datetime(2024, 8, 30, 1, 0)
schedule_interval = "0 1 * * *"
timeout_hours = 96
slack_channel = DICT_CONFIG[env].slack_channel

s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env].s3_folder
boostrapActionPath = f"s3://{s3_bucket}/{s3_folder}/bin/bootstrap/{version}/bootstrap.sh"

main_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
main_master_machine_vcpu = 48
main_core_machine_vmem = 53  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
main_core_machine_vcpu_granularity = 1
main_core_machine_vcpu = 8 * main_core_machine_vcpu_granularity
main_driver_memory_overhead = 0.20
main_executor_memory_overhead = 0.20
main_driver_count = 1
main_core_machine_count = 400
main_executor_count_per_machine = 1
main_driver_core_count = int(floor(main_master_machine_vcpu / main_driver_count))
main_driver_memory = str(int(floor(main_master_machine_vmem / (main_driver_count * (1 + main_driver_memory_overhead))))) + "G"
main_executor_count = main_core_machine_count * main_executor_count_per_machine
main_executor_core_count = int(floor(main_core_machine_vcpu / main_executor_count_per_machine))
main_executor_memory = str(
    int(floor(main_core_machine_vmem / (main_executor_count_per_machine * (1 + main_executor_memory_overhead))))
) + "G"

main_master_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[M6g.m6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
main_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[R6gd.r6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=main_core_machine_count
)

train_master_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
train_master_machine_vcpu = 8
train_core_machine_vmem = 48  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
train_core_machine_vcpu_granularity = 1
train_core_machine_vcpu = 16 * train_core_machine_vcpu_granularity
train_driver_memory_overhead = 0.25
train_executor_memory_overhead = 0.25
train_driver_count = 1
train_executor_count_per_machine = 1
train_driver_core_count = int(floor(train_master_machine_vcpu / train_driver_count))
train_driver_memory = str(int(floor(train_master_machine_vmem / (train_driver_count * (1 + train_driver_memory_overhead))))) + "G"
train_executor_core_count = int(floor((train_core_machine_vcpu / train_executor_count_per_machine)))
train_executor_memory = str(
    int(floor(train_core_machine_vmem / (train_executor_count_per_machine * (1 + train_executor_memory_overhead))))
) + "G"

train_master_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[
        M5a.m5a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256).with_ebs_iops(3000),
        M5.m5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256).with_ebs_iops(3000)
    ],
    on_demand_weighted_capacity=1
)

score_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
score_master_machine_vcpu = 48
score_core_machine_vmem = 53  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
score_core_machine_vcpu_granularity = 1
score_core_machine_vcpu = 8 * score_core_machine_vcpu_granularity
score_driver_memory_overhead = 0.20
score_executor_memory_overhead = 0.20
score_driver_count = 1
score_core_machine_count = 200
score_executor_count_per_machine = 1
score_driver_core_count = int(floor(score_master_machine_vcpu / score_driver_count))
score_driver_memory = str(int(floor(score_master_machine_vmem / (score_driver_count * (1 + score_driver_memory_overhead))))) + "G"
score_executor_count = score_core_machine_count * score_executor_count_per_machine
score_executor_core_count = int(floor(score_core_machine_vcpu / score_executor_count_per_machine))
score_executor_memory = str(
    int(floor(score_core_machine_vmem / (score_executor_count_per_machine * (1 + score_executor_memory_overhead))))
) + "G"

score_master_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[M6g.m6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)
score_core_fleet_instances = EmrFleetInstanceTypes(
    instance_types=[I4g.i4g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=score_core_machine_count
)

log_level = "TRACE"

instance_timestamp = datetime.utcnow()
application_data_url = DICT_CONFIG[env].application_data_url
data_provider_id = DICT_CONFIG[env].data_provider_id
backstage_url = "s3://ttd-datprd-us-east-1/backstage"
prometheus_push_gateway_address = "pushgateway.gen.adsrvr.org:80"
lucid_data_url = "s3://thetradedesk-useast-data-import/lucid/otp-seed-data"
lifesight_data_url = "s3://thetradedesk-useast-data-import/lifesight/otp-seed-data"
iso_3166_url = "s3://ttd-datprd-us-east-1/data/Iso3166/v=1"
alternate_country_name_url = "s3://ttd-datprd-us-east-1/data/AlternateCountryName/v=1"
legacy_adbrain_cross_device_graph_url = "s3://thetradedesk-useast-data-import/sxd-etl/universal/adbrain_legacy"
iav2_cross_device_graph_url = "s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph"
legacy_iav2_cross_device_graph_url = "s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph_legacy"
open_graph_url = "s3://thetradedesk-useast-data-import/sxd-etl/universal/nextgen"
population_pyramid_url = "s3://ttd-datprd-us-east-1/data/PopulationPyramid/v=1/"
population_pyramid_url_v2 = "s3://ttd-datprd-us-east-1/data/PopulationPyramid/v=2/"
supply_vendor_url = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendor/v=1"
avails_url = "s3://thetradedesk-useast-partners-avails/tapad"
unsampled_avails_url = "s3://ttd-datprd-us-east-1/application/unified_model/backstage/avails/UserHistory"  # Using UM backstage
user_profile_url = "s3://thetradedesk-useast-hadoop/unified-data-models/prod/user-profiles/avails"
restricted_vendors_url = "s3://ttd-datprd-us-east-1/data/RestrictedPublisher/roku.csv"
min_feedback_score = 0.85
quarantined_providers = ["XX"]
provider_priority = ["lucid", "lifesight"]
country_provider_priority = ["JP:lucid", "JP:lifesight"]
job_application_version = version
job_k8s_in_cluster = True
job_k8s_cluster_context = None
job_k8s_namespace = "airflow"
job_subscription_docker_image_name = "dev.docker.adsrvr.org/com.thetradedesk.radar-subscription:" + job_application_version
job_ingestion_spark_library_s3_key = s3_folder + "/bin/ingestion/" + job_application_version + "/com.thetradedesk.radar-ingestion-" + job_application_version + "-all.jar"
job_emr_cluster_name = f"Data_Products-{name}"

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
    slack_alert_only_for_prod=True,
)

scoring_applications = [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "AmazonCloudWatchAgent"}]
scoring_release = AwsEmrVersions.AWS_EMR_SPARK_3_5

normal_applications = [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "Ganglia"}]
normal_release = AwsEmrVersions.AWS_EMR_SPARK_3_4_0

cluster_tags = {"Environment": env, "Job": name, "Team": "DATPRD", "Duration": "very_long"}

ec2_subnets_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]


def create_emr_cluster(
    cluster_name,
    s3_bucket,
    s3_folder,
    master_fleet_instances,
    core_fleet_instances,
    core_machine_vcpu,
    bootstrap_actions,
    is_scoring=False,
    ec2_key_name="UsEastDataEngineering"
):
    release_num = normal_release
    if is_scoring:
        release_num = scoring_release

    application_configuration = [{
        "Classification": "yarn-site",
        "Properties": {
            "yarn.nodemanager.vmem-check-enabled": "false",
            "yarn.nodemanager.pmem-check-enabled": "false",
            "yarn.nodemanager.resource.cpu-vcores": str(core_machine_vcpu)
        }
    }, {
        "Classification": "emrfs-site",
        "Properties": {
            "fs.s3.aimd.enabled": "true",
            "fs.s3.aimd.maxAttempts": "150000",
            "fs.s3.maxRetries": "150000"
        }
    }]

    return EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=master_fleet_instances,
        cluster_tags=cluster_tags,
        core_fleet_instance_type_configs=core_fleet_instances,
        ec2_subnet_ids=ec2_subnets_ids,
        emr_release_label=release_num,
        enable_prometheus_monitoring=True,
        log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
        ec2_key_name=ec2_key_name,
        additional_application_configurations=application_configuration,
        bootstrap_script_actions=bootstrap_actions,
        cluster_auto_termination_idle_timeout_seconds=1800,
        retries=5
    )


def add_emr_step(
    step_name, spark_args, driver_core_count, driver_memory, executor_count, executor_core_count, executor_core_count_per_task,
    executor_memory, artifact_id, command_name, command_args
):

    spark_conf = {
        "spark.network.timeout":
        "1200s",
        "spark.executor.heartbeatInterval":
        "120s",
        "spark.driver.memory":
        driver_memory,
        "spark.driver.cores":
        str(driver_core_count),
        "spark.driver.maxResultSize":
        driver_memory,
        "spark.driver.extraJavaOptions":
        "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintCodeCache -XX:+UseLargePages -XX:ReservedCodeCacheSize=512M -XX:InitialCodeCacheSize=512M -Xms"
        + driver_memory,
        "spark.executor.instances":
        str(executor_count),
        "spark.executor.cores":
        str(executor_core_count),
        "spark.executor.memory":
        executor_memory,
        "spark.executor.extraJavaOptions":
        "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintCodeCache -XX:+UseLargePages -XX:ReservedCodeCacheSize=512M -XX:InitialCodeCacheSize=512M -Xms"
        + executor_memory,
        "spark.task.cpus":
        str(executor_core_count_per_task),
        "spark.dynamicAllocation.enabled":
        "false",
        "spark.sql.files.ignoreCorruptFiles":
        "false",
        "spark.default.parallelism":
        str(executor_count * executor_core_count),
        "spark.sql.shuffle.partitions":
        str(executor_count * executor_core_count),
    }

    # Process spark_args into a dictionary
    spark_args_dict = {}
    for i in range(0, len(spark_args), 2):
        # Remove '--conf' and split key-value pair
        key, value = spark_args[i + 1].split('=')
        spark_args_dict[key] = value

    spark_conf.update(spark_args_dict)
    spark_args_final = []
    for key, value in spark_conf.items():
        spark_args_final.append(("conf", "{}={}".format(key, value)))

    command_args_final = [
        command_name, "--dagTimestamp", "{{ts}}", "--prometheusPushGatewayAddress", prometheus_push_gateway_address
    ] + command_args

    return EmrJobTask(
        name=step_name,
        class_name=group_id + "." + artifact_id + ".Main",
        action_on_failure="CONTINUE",
        additional_args_option_pairs_list=spark_args_final,
        deploy_mode="client",
        executable_path="s3://" + s3_bucket + "/" + s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" + artifact_id +
        "-" + version + "-all.jar",
        command_line_arguments=command_args_final,
        timeout_timedelta=timedelta(hours=50),
    )


def get_most_recent_date(url_bucket, url_prefix, s3_hook: AwsCloudStorage):
    date_prefixes = s3_hook.list_prefixes(url_bucket, url_prefix)

    success_prefixes = [prefix for prefix in date_prefixes if s3_hook.check_for_key(prefix + "_SUCCESS", url_bucket)]
    string_dates = [prefix.split("/")[-2].lower().replace("date=", "") for prefix in success_prefixes]
    dates = [datetime.strptime(string_date, "%Y-%m-%d") for string_date in string_dates]
    if (len(dates) > 0):
        return max(dates)
    else:
        return None


initializer = OpTask(op=BashOperator(
    task_id="initializer",
    bash_command="echo Initializer",
    env={"dev_user": f"{dev_user}"},
))


def is_new_unified_model_data_available_fn():
    source_url_components = user_profile_url.split("/")
    source_url_bucket = source_url_components[2]
    source_url_prefix = "/".join(source_url_components[3:] + [""])

    ingested_url_components = backstage_url.split("/")
    ingested_url_bucket = ingested_url_components[2]
    ingested_url_prefix = "/".join([ingested_url_components[3]] + ["90d"] + [env] + ["radar"] + ["UserProfile"] + [""])

    s3_hook: AwsCloudStorage = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()

    source_most_recent_date = get_most_recent_date(source_url_bucket, source_url_prefix, s3_hook)

    if (source_most_recent_date is None):
        raise Exception(f"ERROR: Can't find source unified model data at: s3://{source_url_bucket}/{source_url_prefix}")

    ingested_most_recented_date = get_most_recent_date(ingested_url_bucket, ingested_url_prefix, s3_hook)

    if (ingested_most_recented_date is None or source_most_recent_date > ingested_most_recented_date):
        return "create_main_cluster_checkpoint"
    else:
        return "finalizer"


is_new_unified_model_data_available = OpTask(
    op=BranchPythonOperator(
        task_id="is_new_unified_model_data_available",
        python_callable=is_new_unified_model_data_available_fn,
    )
)

create_main_cluster = create_emr_cluster(
    cluster_name=job_emr_cluster_name + "-Main",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=main_master_fleet_instances,
    core_fleet_instances=main_core_fleet_instances,
    core_machine_vcpu=main_core_machine_vcpu,
    bootstrap_actions=[ScriptBootstrapAction(boostrapActionPath, name="BootstrapActions")],
)

create_main_cluster_checkpoint = OpTask(
    op=BashOperator(
        task_id="create_main_cluster_checkpoint",
        bash_command="echo 'create_main_cluster_checkpoint'",
    )
)

ingest_user_labels_start_checkpoint = OpTask(
    op=BashOperator(
        task_id="ingest_user_labels_start_checkpoint",
        bash_command="echo 'ingest_user_labels_start_checkpoint'",
    )
)

add__ingest_s3_lucid_data = add_emr_step(
    step_name="ingest_s3_lucid_data",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="ingestion",
    command_name="ingest-s3-lucid-data",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--lucidDataUrl",
        lucid_data_url,
        "--iso3166Url",
        iso_3166_url,
        "--alternateCountryNameUrl",
        alternate_country_name_url,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

add__ingest_s3_lifesight_data = add_emr_step(
    step_name="ingest_s3_lifesight_data",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="ingestion",
    command_name="ingest-s3-lifesight-data",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--lifesightDataUrl",
        lifesight_data_url,
        "--iso3166Url",
        iso_3166_url,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

ingest_user_labels_end_checkpoint = OpTask(
    op=BashOperator(
        task_id="ingest_user_labels_end_checkpoint",
        bash_command="echo 'ingest_user_labels_end_checkpoint'",
    )
)

ingest_user_features_start_checkpoint = OpTask(
    op=BashOperator(
        task_id="ingest_user_features_start_checkpoint",
        bash_command="echo 'ingest_user_features_start_checkpoint'",
    )
)

add__ingest_supply_vendor = add_emr_step(
    step_name="ingest_supply_vendor",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="ingestion",
    command_name="ingest-supply-vendor",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "30",
        "--supplyVendorUrl",
        supply_vendor_url,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

add__ingest_unsampled_avails = add_emr_step(
    step_name="ingest_unsampled_avails",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="ingestion",
    command_name="ingest-unsampled-avails",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--unsampledAvailsUrl",
        unsampled_avails_url,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries)
)

add__ingest_user_profile = add_emr_step(
    step_name="ingest_user_profile",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="ingestion",
    command_name="ingest-user-profile",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "60",
        "--userProfileUrl",
        user_profile_url,
        "--iso3166Url",
        iso_3166_url,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries)
)

ingest_user_features_end_checkpoint = OpTask(
    op=BashOperator(
        task_id="ingest_user_features_end_checkpoint",
        bash_command="echo 'ingest_user_features_end_checkpoint'",
    )
)

ingest_checkpoint = OpTask(op=BashOperator(
    task_id="ingest_checkpoint",
    bash_command="echo 'ingest_checkpoint'",
))

add__aggregate_user_features = add_emr_step(
    step_name="aggregate_user_features",
    spark_args=[
        "--conf",
        "spark.default.parallelism=" + str(main_executor_count * main_executor_core_count),
        "--conf",
        "spark.sql.shuffle.partitions=" + str(main_executor_count * main_executor_core_count),
    ],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="engineering",
    command_name="aggregate-user-features",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--lookBackDays",
        str(look_back_days),
        "--userProfileAggregationDays",
        "14",
        "--userProfileFrequencyDays",
        "7",
        "--userProfileAggregationExtraDays",
        "7",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries) + (["--gdprCountries"] + gdpr_countries),
)

add__aggregate_user_labels = add_emr_step(
    step_name="aggregate_user_labels",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="engineering",
    command_name="aggregate-user-labels",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--minFeedbackScore",
        str(min_feedback_score),
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + model_countries) + (["--quarantinedProviders"] + quarantined_providers) +
    (["--providerPriority"] + provider_priority) + (["--countryProviderPriority"] + country_provider_priority)
)

aggregate_checkpoint = OpTask(op=BashOperator(
    task_id="aggregate_checkpoint",
    bash_command="echo 'aggregate_checkpoint'",
))

add__engineer_user_features = add_emr_step(
    step_name="engineer_user_features",
    spark_args=[
        "--conf",
        "spark.sql.broadcastTimeout=600",
        "--conf",
        "spark.default.parallelism=" + str(int(floor(main_executor_count * main_executor_core_count * 10))),
        "--conf",
        "spark.sql.shuffle.partitions=" + str(int(floor(main_executor_count * main_executor_core_count * 10))),
    ],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="engineering",
    command_name="engineer-user-features",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--unfilteredCategoricalFeatures",
        "mOtpDemo",
        "mOtpGender",
        "swdDemo",
        "swdGender",
        "--backstageUrl",
        backstage_url,
        "--includePoly2ColumnsInOutput=false",
        "--env",
        env,
    ] + (["--countries"] + countries)
)

add__engineer_user_labels = add_emr_step(
    step_name="engineer_user_labels",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="engineering",
    command_name="engineer-user-labels",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--populationPyramidUrl",
        population_pyramid_url,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + model_countries)
)

add__generate_ml_inputs = add_emr_step(
    step_name="generate_ml_inputs",
    spark_args=[
        "--conf",
        "spark.default.parallelism=" + str(main_executor_count * main_executor_core_count),
        "--conf",
        "spark.sql.shuffle.partitions=" + str(main_executor_count * main_executor_core_count),
    ],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="engineering",
    command_name="generate-ml-inputs",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries)
)

add__generate_ml_classifier_inputs = add_emr_step(
    step_name="generate_ml_classifier_inputs",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="engineering",
    command_name="generate-ml-classifier-inputs",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries)
)

engineer_checkpoint = OpTask(op=BashOperator(
    task_id="engineer_checkpoint",
    bash_command="echo 'engineer_checkpoint'",
))


def verify_model_timestamp_fn():
    s3_hook = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    url_components = backstage_url.split("/")
    url_bucket = url_components[2]
    url_key = "/".join([url_components[3]] + ["14d"] + [env] + ["radar"] + ["ModelMetadata"] + ["_MODEL_TIMESTAMP"])
    try:
        timestamp_file_content = s3_hook.download_file('/tmp/timestamp_file', url_key, url_bucket)
        with open(timestamp_file_content.get(), 'r') as f:
            timestamp_unix = int(f.read().strip())
        unix_14_days = 14 * 86400
        if int(timestamp_unix + unix_14_days) < int(time.time()):
            return "train_start_checkpoint"
        return "score_begin_checkpoint"
    except ClientError as err:
        return "train_start_checkpoint"
    except AirflowException as err:
        return "train_start_checkpoint"


verify_model_timestamp = OpTask(op=BranchPythonOperator(
    task_id="verify_model_timestamp",
    python_callable=verify_model_timestamp_fn,
))

train_start_checkpoint = OpTask(op=BashOperator(
    task_id="train_start_checkpoint",
    bash_command="echo 'train_start_checkpoint'",
))

train_end_checkpoint = OpTask(op=BashOperator(
    task_id="train_end_checkpoint",
    bash_command="echo 'train_end_checkpoint'",
))

create_train_model_clusters = []
for i, train_cluster_params in enumerate(train_cluster_params_sets):
    train_core_fleet_instances = EmrFleetInstanceTypes(
        instance_types=[
            G5.g5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(256).with_ebs_iops(3000),
        ],
        on_demand_weighted_capacity=train_cluster_params.executor_count
    )
    create_train_model_cluster = create_emr_cluster(
        cluster_name=job_emr_cluster_name + "-Train" + str(i) + "-" + (".".join(str(x) for x in train_cluster_params.countries)),
        s3_bucket=s3_bucket,
        s3_folder=s3_folder + "/emr",
        master_fleet_instances=train_master_fleet_instances,
        core_fleet_instances=train_core_fleet_instances,
        core_machine_vcpu=train_core_machine_vcpu,
        bootstrap_actions=[
            ScriptBootstrapAction(
                "s3://" + s3_bucket + "/" + s3_folder + "/bin/training/" + version + "/generate_getGpuResources.sh",
                name="Generate getGpuResources.sh script"
            )
        ],
    )
    add__train_models = add_emr_step(
        step_name="train_models-" + (".".join(str(x) for x in train_cluster_params.countries)),
        spark_args=[
            "--conf",
            "spark.executor.resource.gpu.amount=1",
            "--conf",
            "spark.executor.resource.gpu.discoveryScript=/home/hadoop/scripts/getGpuResources.sh",
            "--conf",
            "spark.task.resource.gpu.amount=1",
            "--conf",
            "spark.worker.resource.gpu.amount=1",
            "--conf",
            "spark.worker.resource.gpu.discoveryScript=/home/hadoop/scripts/getGpuResources.sh",
            "--conf",
            "spark.executorEnv.NCCL_DEBUG=INFO",
            "--conf",
            "spark.executorEnv.NCCL_SOCKET_IFNAME=eth0",
        ],
        driver_core_count=train_driver_core_count,
        driver_memory=train_driver_memory,
        executor_count=train_cluster_params.executor_count,
        executor_core_count=train_executor_core_count,
        executor_core_count_per_task=train_executor_core_count,
        executor_memory=train_executor_memory,
        artifact_id="training",
        command_name="train-models",
        command_args=[
            "--logLevel",
            log_level,
            "--applicationDataUrl",
            application_data_url,
            "--referenceDate",
            "{{ds}}",
            "--computationDays",
            "1",
            "--minTrainUserCountForClass",
            "100",
            "--maxTrainUserCount",
            str(train_cluster_params.max_train_user_count),
            "--concurrentTrainings",
            "1",
            "--validationsPerTraining",
            "1",
            "--workersPerValidation",
            str(train_cluster_params.executor_count),
            "--threadsPerWorker",
            str(int(train_executor_core_count * 2 / 4)),
            "--backstageUrl",
            backstage_url,
            "--env",
            env,
        ] + (["--countries"] + train_cluster_params.countries)
    )
    create_train_model_cluster.add_sequential_body_task(add__train_models)
    create_train_model_clusters.append(create_train_model_cluster)


def generate_model_timestamp_fn(**kwargs):
    s3_hook = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    url_components = backstage_url.split("/")
    url_bucket = url_components[2]
    url_key = "/".join([url_components[3]] + ["14d"] + [env] + ["radar"] + ["ModelMetadata"] + ["_MODEL_TIMESTAMP"])
    string_data = str(int(time.time()))
    s3_hook.put_object(url_bucket, url_key, string_data, replace=True)


generate_model_timestamp = OpTask(
    op=PythonOperator(
        task_id='generate_model_timestamp',
        python_callable=generate_model_timestamp_fn,
        provide_context=True,
    )
)

score_begin_checkpoint = OpTask(
    op=BashOperator(task_id="score_begin_checkpoint", bash_command="echo 'score_begin_checkpoint'", trigger_rule="none_failed_or_skipped")
)

create_score_cluster = create_emr_cluster(
    cluster_name=job_emr_cluster_name + "-Score",
    s3_bucket=s3_bucket,
    s3_folder=s3_folder + "/emr",
    master_fleet_instances=score_master_fleet_instances,
    core_fleet_instances=score_core_fleet_instances,
    core_machine_vcpu=score_core_machine_vcpu,
    bootstrap_actions=[ScriptBootstrapAction(boostrapActionPath, name="BootstrapActions")],
    is_scoring=True
)

add__evaluate_models = add_emr_step(
    step_name="evaluate_models",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="selection",
    command_name="evaluate-models",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--concurrentEvaluations",
        "16",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + model_countries)
)

add__select_best_models = add_emr_step(
    step_name="select_best_models",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="selection",
    command_name="select-best-models",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--minTrainUserCountForClass",
        "10000",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + model_countries)
)

add__select_nearby_models = add_emr_step(
    step_name="select_nearby_models",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="selection",
    command_name="select-nearby-models",
    command_args=[
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

models_selected_checkpoint = OpTask(
    op=BashOperator(
        task_id="models_selected_checkpoint",
        bash_command="echo 'models_selected_checkpoint'",
    )
)

add__score_users = add_emr_step(
    step_name="score_users",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="scoring",
    command_name="score-users",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--concurrentEvaluations",
        "2",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + model_countries)
)

add__score_nearby_model_users = add_emr_step(
    step_name="score_nearby_model_users",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="scoring",
    command_name="score-nearby-model-users",
    command_args=[
        "--logLevel",
        "WARN",
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--concurrentEvaluations",
        "2",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

users_scored_checkpoint = OpTask(op=BashOperator(
    task_id="users_scored_checkpoint",
    bash_command="echo 'users_scored_checkpoint'",
))

add__calibrate_scored_users = add_emr_step(
    step_name="calibrate_scored_users",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="scoring",
    command_name="calibrate-scored-users",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--crossDeviceGraphSources",
        f"legacy_adbrain,{legacy_adbrain_cross_device_graph_url}",
        f"iav2,{iav2_cross_device_graph_url}",
        f"legacy_iav2,{legacy_iav2_cross_device_graph_url}",
        f"open_graph,{open_graph_url}",
        "--populationPyramidUrl",
        population_pyramid_url_v2,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries)
)

labels_adjusted_checkpoint = OpTask(
    op=BashOperator(
        task_id="labels_adjusted_checkpoint",
        bash_command="echo 'labels_adjusted_checkpoint'",
    )
)

add__assemble_user_demo_labels = add_emr_step(
    step_name="assemble_user_demo_labels",
    spark_args=[
        "--conf",
        "spark.sql.broadcastTimeout=600",
        "--conf",
        "spark.default.parallelism=" + str(score_executor_count * score_executor_core_count * 20),
        "--conf",
        "spark.sql.shuffle.partitions=" + str(score_executor_count * score_executor_core_count * 20),
    ],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="scoring",
    command_name="assemble-user-demo-labels",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries) + (["--motpCountries"] + motp_countries) + (["--swCountries"] + sw_countries) +
    (["--gdprCountries"] + gdpr_countries) + (["--extendedSegmentCountries"] + extended_segment_countries) +
    ["--extendedSegmentGenderAmplitude", "1.6", "--extendedSegmentAgeAmplitude", "2"]
)

add__aggregate_global_segments = add_emr_step(
    step_name="aggregate_global_segments",
    spark_args=[
        "--conf",
        "spark.sql.broadcastTimeout=600",
        "--conf",
        "spark.default.parallelism=" + str(score_executor_count * score_executor_core_count * 20),
        "--conf",
        "spark.sql.shuffle.partitions=" + str(score_executor_count * score_executor_core_count * 20),
    ],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="scoring",
    command_name="aggregate-global-segments",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

data_ready_checkpoint = OpTask(op=BashOperator(
    task_id="data_ready_checkpoint",
    bash_command="echo 'data_ready_checkpoint'",
))

add__publish_to_dmp = add_emr_step(
    step_name="publish_to_dmp",
    spark_args=[
        "--conf", "spark.default.parallelism=" + str(score_executor_count * score_executor_core_count * 20), "--conf",
        "spark.sql.shuffle.partitions=" + str(score_executor_count * score_executor_core_count * 20), "--conf",
        "spark.sql.autoBroadcastJoinThreshold=-1"
    ],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="publishing",
    command_name="publish-to-dmp",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--dataProviderId",
        data_provider_id,
        "--dataServerUrl",
        "https://data-internal.adsrvr.org/data",
        "--provisioningDbUrl",
        "jdbc:sqlserver://provdb.adsrvr.org:1433;database=Provisioning;authenticationScheme=NTLM",
        "--provisioningDbDomain",
        "ops.adsrvr.org",
        "--provisioningDbUser",
        "@/home/hadoop/.ttd/radar/app_params/TTD__RADAR__PROVISIONING_DB_USER",
        "--provisioningDbPass",
        "@/home/hadoop/.ttd/radar/app_params/TTD__RADAR__PROVISIONING_DB_PASS",
        "--iso3166Url",
        iso_3166_url,
        "--dataServerMaxUserCountPerRequest",
        "10000",
        "--dataServerMaxCallRetryCount",
        "100",
        "--dataServerMinWaitTimeAfterFailedRequest",
        "PT0.5S",
        "--dataServerMaxWaitTimeAfterFailedRequest",
        "PT5S",
        "--parallelDataServerRequests",
        "1600",
        "--ttlDays",
        "33",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countriesWithGlobal)
)

add__publish_to_s3 = add_emr_step(
    step_name="publish_to_s3",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="publishing",
    command_name="publish-to-s3",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        "{{ds}}",
        "--computationDays",
        "1",
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ] + (["--countries"] + countries)
)

data_published_checkpoint = OpTask(op=BashOperator(
    task_id="data_published_checkpoint",
    bash_command="echo 'data_published_checkpoint'",
))

add__publish_person_level_to_s3 = add_emr_step(
    step_name="publish_person_level_to_s3",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="publishing",
    command_name="generate-person-level-data",
    command_args=[
        "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}", "--computationDays", "1",
        "--populationPyramidUrl", population_pyramid_url_v2, "--crossDeviceGraphSource", "legacy_adbrain", "--backstageUrl", backstage_url,
        "--env", env, "--concurrentEvaluations", "8"
    ] + (["--countries"] + countries)
)

# publishing iav2 person level to s3

add__iav2_publish_person_level_to_s3 = add_emr_step(
    step_name="iav2_publish_person_level_to_s3",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="publishing",
    command_name="generate-person-level-data",
    command_args=[
        "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}", "--computationDays", "1",
        "--populationPyramidUrl", population_pyramid_url_v2, "--crossDeviceGraphSource", "iav2", "--backstageUrl", backstage_url, "--env",
        env, "--concurrentEvaluations", "4"
    ] + (["--countries"] + countries)
)

# legacy iav2 publish

add__legacy_iav2_publish_person_level_to_s3 = add_emr_step(
    step_name="legacy_iav2_publish_person_level_to_s3",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="publishing",
    command_name="generate-person-level-data",
    command_args=[
        "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}", "--computationDays", "1",
        "--populationPyramidUrl", population_pyramid_url_v2, "--crossDeviceGraphSource", "legacy_iav2", "--backstageUrl", backstage_url,
        "--env", env, "--concurrentEvaluations", "4"
    ] + (["--countries"] + countries)
)

# open graph publish

add__open_graph_publish_person_level_to_s3 = add_emr_step(
    step_name="open_graph_publish_person_level_to_s3",
    spark_args=[],
    driver_core_count=score_driver_core_count,
    driver_memory=score_driver_memory,
    executor_count=score_executor_count,
    executor_core_count=score_executor_core_count,
    executor_core_count_per_task=score_core_machine_vcpu_granularity,
    executor_memory=score_executor_memory,
    artifact_id="publishing",
    command_name="generate-person-level-data",
    command_args=[
        "--logLevel", log_level, "--applicationDataUrl", application_data_url, "--referenceDate", "{{ds}}", "--computationDays", "1",
        "--populationPyramidUrl", population_pyramid_url_v2, "--crossDeviceGraphSource", "open_graph", "--backstageUrl", backstage_url,
        "--env", env, "--concurrentEvaluations", "4"
    ] + (["--countries"] + countries)
)

finalizer = OpTask(op=BashOperator(task_id="finalizer", bash_command="echo 'finalizer'", trigger_rule="none_failed_or_skipped"))

ingest_user_features_start_checkpoint >> add__ingest_supply_vendor >> add__ingest_unsampled_avails >> add__ingest_user_profile >> ingest_user_features_end_checkpoint >> \
                       ingest_checkpoint >> add__aggregate_user_labels >> aggregate_checkpoint >> add__engineer_user_labels >> add__generate_ml_inputs >> add__generate_ml_classifier_inputs
ingest_user_labels_start_checkpoint >> add__ingest_s3_lifesight_data >> add__ingest_s3_lucid_data >> ingest_user_labels_end_checkpoint >> \
                        ingest_checkpoint >> add__aggregate_user_features >> aggregate_checkpoint >> add__engineer_user_features >> add__generate_ml_inputs >> add__generate_ml_classifier_inputs

# main cluster downstream tasks
create_main_cluster.add_parallel_body_task(ingest_user_features_start_checkpoint)
create_main_cluster.add_parallel_body_task(ingest_user_labels_start_checkpoint)
create_main_cluster.add_parallel_body_task(add__ingest_supply_vendor)
create_main_cluster.add_parallel_body_task(add__ingest_unsampled_avails)
create_main_cluster.add_parallel_body_task(add__ingest_user_profile)
create_main_cluster.add_parallel_body_task(ingest_user_features_end_checkpoint)
create_main_cluster.add_parallel_body_task(add__ingest_s3_lifesight_data)
create_main_cluster.add_parallel_body_task(add__ingest_s3_lucid_data)
create_main_cluster.add_parallel_body_task(ingest_user_labels_end_checkpoint)
create_main_cluster.add_parallel_body_task(ingest_checkpoint)
create_main_cluster.add_parallel_body_task(add__aggregate_user_labels)
create_main_cluster.add_parallel_body_task(add__aggregate_user_features)
create_main_cluster.add_parallel_body_task(aggregate_checkpoint)
create_main_cluster.add_parallel_body_task(add__engineer_user_labels)
create_main_cluster.add_parallel_body_task(add__engineer_user_features)
create_main_cluster.add_parallel_body_task(add__generate_ml_inputs)
create_main_cluster.add_parallel_body_task(add__generate_ml_classifier_inputs)

# score cluster downstream tasks
create_score_cluster.add_sequential_body_task(add__evaluate_models)
create_score_cluster.add_sequential_body_task(add__select_best_models)
create_score_cluster.add_sequential_body_task(add__select_nearby_models)
create_score_cluster.add_sequential_body_task(models_selected_checkpoint)
create_score_cluster.add_sequential_body_task(add__score_nearby_model_users)
create_score_cluster.add_sequential_body_task(add__score_users)
create_score_cluster.add_sequential_body_task(users_scored_checkpoint)
create_score_cluster.add_sequential_body_task(add__calibrate_scored_users)
create_score_cluster.add_sequential_body_task(labels_adjusted_checkpoint)
create_score_cluster.add_sequential_body_task(add__assemble_user_demo_labels)
create_score_cluster.add_sequential_body_task(add__aggregate_global_segments)
create_score_cluster.add_sequential_body_task(data_ready_checkpoint)
create_score_cluster.add_sequential_body_task(add__publish_to_dmp)
create_score_cluster.add_sequential_body_task(add__publish_to_s3)
create_score_cluster.add_sequential_body_task(data_published_checkpoint)
create_score_cluster.add_sequential_body_task(add__publish_person_level_to_s3)
create_score_cluster.add_sequential_body_task(add__iav2_publish_person_level_to_s3)
create_score_cluster.add_sequential_body_task(add__legacy_iav2_publish_person_level_to_s3)
create_score_cluster.add_sequential_body_task(add__open_graph_publish_person_level_to_s3)

dag >> initializer >> is_new_unified_model_data_available >> finalizer

is_new_unified_model_data_available >> create_main_cluster_checkpoint >> create_main_cluster >> engineer_checkpoint >> verify_model_timestamp
verify_model_timestamp >> score_begin_checkpoint
verify_model_timestamp >> train_start_checkpoint

for i, create_train_model_cluster in enumerate(create_train_model_clusters):
    train_start_checkpoint >> create_train_model_cluster >> train_end_checkpoint

train_end_checkpoint >> generate_model_timestamp >> score_begin_checkpoint

score_begin_checkpoint >> create_score_cluster >> finalizer

adag = dag.airflow_dag
