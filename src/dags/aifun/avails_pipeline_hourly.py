import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5d import M5d
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow, DatabricksRegion
from ttd.openlineage import OpenlineageConfig, OpenlineageTransport
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.python import PythonTask


class AvailsPipelineRegionConfig:
    """
    Contains Databricks and EMR configs specific to a particular region
    """

    def __init__(
        self,
        databricks_region: DatabricksRegion,
        databricks_instance_profile_arn: str,
        proto_instance_fleet: List[EmrInstanceType],
        spark_args: Optional[Dict[str, str]] = None,
        agg_assume_role_arn: Optional[str] = None,
        **emr_cluster_kwargs
    ):
        self.databricks_region = databricks_region
        self.databricks_instance_profile_arn = databricks_instance_profile_arn

        self.proto_instance_fleet = proto_instance_fleet
        self.agg_assume_role_arn = agg_assume_role_arn

        self.spark_args: Dict[str, str] = spark_args if spark_args is not None else {}

        # arbitrary additional keyword arguments to be passed to the EMR cluster
        self.emr_cluster_kwargs = emr_cluster_kwargs


mem_instance_fleet = [
    R6g.r6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16).with_ebs_size_gb(256),
    R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(512),
    R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64).with_ebs_size_gb(1024),
    R5.r5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16).with_ebs_size_gb(256),
    R5.r5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(512),
]

ebs_instance_fleet = [
    M6g.m6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16).with_ebs_size_gb(256),
    M6g.m6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(512),
    M6g.m6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64).with_ebs_size_gb(1024),
    M5.m5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16).with_ebs_size_gb(256),
    M5.m5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(512),
]

disk_instance_fleet = [
    M6g.m6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    M6g.m6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    M6g.m6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
    M5d.m5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    M5d.m5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
]

databricks_spark_version = "16.4.x-scala2.12"
default_assume_role_arn = "arn:aws:iam::003576902480:role/ttd_avails_pipeline_dynamo_delta"

GET_LATEST_ARTEFACT_TASK_ID = "get_latest_artefacts"
ARTEFACTS_BUCKET = "ttd-build-artefacts"
GIT_BRANCH_NAME = "master"

ARTEFACT_BASE = f"s3://{ARTEFACTS_BUCKET}/avails-pipeline/{GIT_BRANCH_NAME}/"
AVAILS_PIPELINE_JAR_FILE_NAME = "availspipeline-spark-pipeline.jar"
SCHEDULER_POOL_FILE_NAME = "hourly-aggregate-scheduler-pools.xml"

CONFIGS_BY_REGION = {
    "us-east-1":
    AvailsPipelineRegionConfig(
        databricks_region=DatabricksRegion.vai(),
        databricks_instance_profile_arn="arn:aws:iam::503911722519:instance-profile/ttd_data_science_instance_profile",
        spark_args={
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
            "spark.hadoop.fs.s3a.assumed.role.arn": "arn:aws:iam::003576902480:role/ttd_cluster_compute_adhoc",
        },
        agg_assume_role_arn="arn:aws:iam::003576902480:role/ttd_cluster_compute_adhoc",
        proto_instance_fleet=ebs_instance_fleet,
    ),
    "us-west-2":
    AvailsPipelineRegionConfig(
        databricks_region=DatabricksRegion.or5(),
        databricks_instance_profile_arn="arn:aws:iam::503911722519:instance-profile/aifun-prod-compute-or5",
        emr_managed_master_security_group="sg-0bf03a9cbbaeb0494",
        emr_managed_slave_security_group="sg-0dfc2e6a823862dbf",
        ec2_subnet_ids=EmrSubnets.PrivateUSWest2.all(),
        pass_ec2_key_name=False,
        service_access_security_group="sg-0ccb4ca554f6e1165",
        proto_instance_fleet=disk_instance_fleet,
    ),
    "ap-northeast-1":
    AvailsPipelineRegionConfig(
        databricks_region=DatabricksRegion.jp3(),
        databricks_instance_profile_arn="arn:aws:iam::503911722519:instance-profile/aifun-prod-compute-jp3",
        emr_managed_master_security_group="sg-02cd06e673800a7d4",
        emr_managed_slave_security_group="sg-0a9b18bb4c0fa5577",
        ec2_subnet_ids=EmrSubnets.PrivateAPNortheast1.all(),
        pass_ec2_key_name=False,
        service_access_security_group="sg-0644d2eafc6dd2a8d",
        proto_instance_fleet=disk_instance_fleet,
    ),
    "ap-southeast-1":
    AvailsPipelineRegionConfig(
        databricks_region=DatabricksRegion.sg4(),
        databricks_instance_profile_arn="arn:aws:iam::503911722519:instance-profile/aifun-prod-compute-sg4",
        emr_managed_master_security_group="sg-014b895831026416d",
        emr_managed_slave_security_group="sg-03149058ce1479ab2",
        ec2_subnet_ids=EmrSubnets.PrivateAPSoutheast1.all(),
        pass_ec2_key_name=False,
        service_access_security_group="sg-008e3e75c75f7885d",
        proto_instance_fleet=disk_instance_fleet,
    ),
    "eu-west-1":
    AvailsPipelineRegionConfig(
        databricks_region=DatabricksRegion.ie2(),
        databricks_instance_profile_arn="arn:aws:iam::503911722519:instance-profile/aifun-prod-compute-ie2",
        emr_managed_master_security_group="sg-081d59c2ec2e9ef68",
        emr_managed_slave_security_group="sg-0ff0115d48152d67a",
        ec2_subnet_ids=EmrSubnets.PrivateEUWest1.all(),
        pass_ec2_key_name=False,
        service_access_security_group="sg-06a23349af478630b",
        proto_instance_fleet=mem_instance_fleet,
    ),
    "eu-central-1":
    AvailsPipelineRegionConfig(
        databricks_region=DatabricksRegion.de4(),
        databricks_instance_profile_arn="arn:aws:iam::503911722519:instance-profile/aifun-prod-compute-de4",
        emr_managed_master_security_group="sg-0a905c2e9d0b35fb8",
        emr_managed_slave_security_group="sg-054551f0756205dc8",
        ec2_subnet_ids=EmrSubnets.PrivateEUCentral1.all(),
        pass_ec2_key_name=False,
        service_access_security_group="sg-09a4d1b6a8145bd39",
        proto_instance_fleet=ebs_instance_fleet,
    ),
}


def format_java_options(java_options):
    return " ".join([f"-D{key}={value}" for key, value in java_options])


def get_artefact_for_dag_run(file_name: str) -> str:
    return ARTEFACT_BASE + "{{ task_instance.xcom_pull(task_ids='" + GET_LATEST_ARTEFACT_TASK_ID + "', key='artefact_version') }}/" + file_name


def task_pin_jar_file(ti: TaskInstance):
    s3_client = boto3.client("s3")

    base_prefix = f"avails-pipeline/{GIT_BRANCH_NAME}/"

    response = s3_client.list_objects_v2(Bucket=ARTEFACTS_BUCKET, Prefix=base_prefix, Delimiter="/")

    prefixes = [obj['Prefix'] for obj in response['CommonPrefixes']]

    max_pipeilne_id = 0
    artefact_version = None

    for prefix in prefixes:
        version_str = prefix.replace(base_prefix, "").replace("/", "")
        # TODO better handling of anything that doesn't match the pattern
        if version_str != "latest":
            pipeline_id = int(version_str.split("-")[0])
            if pipeline_id > max_pipeilne_id:
                max_pipeilne_id = pipeline_id
                artefact_version = version_str

    if artefact_version is None:
        raise AirflowFailException(f"Unable to find suitable jar under S3 bucket '{ARTEFACTS_BUCKET}', prefix '{base_prefix}'")

    logging.info(f"Found latest jar with version {artefact_version}")

    ti.xcom_push(key="artefact_version", value=artefact_version)


def create_dag(
    aws_region: str,
    start_date: datetime,
    etl_cores: int,
    agg_instances: int,
    agg_shuffle_partitions: int,
    enable_prometheus_monitoring: bool = True,
    enable_spark_history_server_stats: bool = True,
):
    config = CONFIGS_BY_REGION[aws_region]
    dag_id = "avails-pipeline-hourly-" + aws_region + "-airflow-2"

    openlineage_namespace_agg = "AvailsPipelineHourly-" + aws_region

    java_cluster_options_agg = [
        ("log4j2.formatMsgNoLookups", "true"),
        ("parallelizeOps", "true"),
        ("includeIqmDatasets", "false"),
        ("ttd.IdentityHouseholdIntermediateDataSet.forceSameReadWritePath", "true"),
        ("ttd.IdentityHouseholdIntermediateDataSet.storageSystem", "s3"),
        ("writeIntermediateDataSets", "true"),
        ("ttd.aws.assumeRoleArn", config.agg_assume_role_arn or default_assume_role_arn),
        ("ttd.aws.assumeRoleSessionName", "databricks-avails-pipeline-" + aws_region),
    ]

    spark_conf_agg = {
        "spark.databricks.acl.needAdminPermissionToViewLogs": "false",
        "spark.databricks.io.cache.enabled": "false",
        "spark.databricks.delta.checkpoint.partSize": "100000000",
        "spark.databricks.delta.checkpoint.triggers.deltaFileThreshold": "80MB",
        "spark.databricks.delta.dataSkippingNumIndexedCols": "0",
        "spark.databricks.delta.targetFileSize": "1200000000",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.shuffle.partitions": str(agg_shuffle_partitions),
        "spark.scheduler.allocation.file": get_artefact_for_dag_run(SCHEDULER_POOL_FILE_NAME),
        "spark.task.reaper.enabled": "false",
        "spark.databricks.preemption.enabled": "false",

        # Identity graphs
        "spark.identityGraphs.joins.explodeJoin.numSaltValues": "4",

        # Dynamo stuff
        "spark.databricks.delta.allowOSSLogStores": "true",
        "spark.delta.logStore.s3a.impl": "io.delta.storage.S3DynamoDBLogStore",
        "spark.delta.logStore.s3.impl": "io.delta.storage.S3DynamoDBLogStore",
        "spark.io.delta.storage.S3DynamoDBLogStore.credentials.provider":
        "com.thetradedesk.availspipeline.spark.auth.TTDAssumeRoleCredentialsProvider",
        "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region": "us-east-1",
        "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName": "avails_pipeline_delta_log",
    }

    # Add the custom region specific spark args in
    spark_conf_agg.update(config.spark_args)

    std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[M5.m5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
        on_demand_weighted_capacity=1,
        spot_weighted_capacity=0,
    )

    avails_pipeline_dag: TtdDag = TtdDag(
        dag_id=dag_id,
        start_date=start_date,
        schedule_interval=timedelta(hours=1) if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else None,
        max_active_runs=8,
        retries=1,
        retry_delay=timedelta(minutes=2),
        slack_tags="AIFUN",
        enable_slack_alert=False,
    )

    latest_artefact_task = PythonTask(task_id=GET_LATEST_ARTEFACT_TASK_ID, python_callable=task_pin_jar_file)

    # cluster to convert proto to delta
    etl_cluster = EmrClusterTask(
        name="AvailsPipelineProtoConvert-" + aws_region,
        log_uri="s3://thetradedesk-useast-avails/emr-logs",
        master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=config.proto_instance_fleet,
            on_demand_weighted_capacity=etl_cores,
        ),
        additional_application_configurations=[{
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            },
        }],
        cluster_tags={
            "Team": "AIFUN",
            "Process": "Avails-Proto-Convert-" + aws_region
        },
        enable_prometheus_monitoring=enable_prometheus_monitoring,
        enable_spark_history_server_stats=enable_spark_history_server_stats,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        region_name=aws_region,
        **config.emr_cluster_kwargs,
    )

    etl_step = EmrJobTask(
        cluster_specs=etl_cluster.cluster_specs,
        name="ProtoConvert",
        class_name="com.thetradedesk.availspipeline.spark.jobs.HourlyReformatProtoAvailsOperation",
        executable_path=get_artefact_for_dag_run(AVAILS_PIPELINE_JAR_FILE_NAME),
        additional_args_option_pairs_list=[
            ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
            (
                "conf",
                "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            ),
            ("conf", "spark.databricks.delta.retentionDurationCheck.enabled=false"),
            (
                "conf",
                "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled=true",
            ),
            ("conf", "spark.databricks.delta.vacuum.parallelDelete.enabled=true"),

            # Dynamo stuff
            ("conf", "spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore"),
            ("conf", "spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore"),
            ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-east-1"),
            ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=avails_pipeline_delta_log"),
        ],
        eldorado_config_option_pairs_list=[
            ("hourToTransform", '{{ logical_date.strftime("%Y-%m-%dT%H:00:00") }}'),
            ("ttd.AvailsProtoReader.parallelismFactor", "3"),
        ],
        region_name=aws_region,
        openlineage_config=OpenlineageConfig(enabled=OpenlineageConfig.supports_region(aws_region), transport=OpenlineageTransport.ROBUST)
    )

    etl_cluster.add_parallel_body_task(etl_step)

    publisher_agg = get_databricks_task(
        aws_region=aws_region,
        # super cheap query, doesn't need the same resources
        agg_instances=agg_instances // 2,
        agg_instance_type="m6g.4xlarge",
        config=config,
        openlineage_namespace_agg=openlineage_namespace_agg,
        transformer="PublisherAggTransformer",
        java_cluster_options_agg=java_cluster_options_agg,
        spark_conf_agg=spark_conf_agg,
        use_ebs=True,
    )

    deal_agg = get_databricks_task(
        aws_region=aws_region,
        agg_instances=agg_instances,
        agg_instance_type="m6gd.4xlarge",
        config=config,
        openlineage_namespace_agg=openlineage_namespace_agg,
        transformer="DealAvailsAggTransformer",
        java_cluster_options_agg=java_cluster_options_agg,
        spark_conf_agg=spark_conf_agg,
        use_ebs=False,
    )

    id_intermediate_agg = get_databricks_task(
        aws_region=aws_region,
        agg_instances=agg_instances,
        agg_instance_type="r6gd.4xlarge",
        config=config,
        openlineage_namespace_agg=openlineage_namespace_agg,
        transformer="IdentityAggregateIntermediateTransformer,IdentityJoinWithGraphIntermediateTransformer",
        java_cluster_options_agg=java_cluster_options_agg,
        spark_conf_agg=spark_conf_agg,
        use_ebs=False,
        short_name="IdentityAggAndGraphJoin",
    )

    id_deal_agg = get_databricks_task(
        aws_region=aws_region,
        agg_instances=agg_instances,
        agg_instance_type="m6gd.4xlarge",
        # coalesce to 4x number of cores
        coalesce_partitions=agg_instances * 16 * 4,
        config=config,
        openlineage_namespace_agg=openlineage_namespace_agg,
        transformer="IdentityAndDealTransformer",
        java_cluster_options_agg=java_cluster_options_agg,
        spark_conf_agg=spark_conf_agg,
        use_ebs=False,
        use_photon=False,
    )

    # auto shuffle partitions for id_agg
    id_agg_spark_conf = dict(spark_conf_agg)
    id_agg_spark_conf["spark.sql.shuffle.partitions"] = "auto"

    id_agg = get_databricks_task(
        aws_region=aws_region,
        # slightly scale this cluster up more
        agg_instances=agg_instances * 5 // 4,
        agg_instance_type="r6gd.4xlarge",
        config=config,
        openlineage_namespace_agg=openlineage_namespace_agg,
        transformer="IdentityAvailsAggTransformerV2",
        java_cluster_options_agg=java_cluster_options_agg,
        spark_conf_agg=spark_conf_agg,
        use_ebs=False,
    )

    deal_set_agg = get_databricks_task(
        aws_region=aws_region,
        agg_instances=agg_instances,
        agg_instance_type="m6g.4xlarge",
        config=config,
        openlineage_namespace_agg=openlineage_namespace_agg,
        transformer="DealSetAvailsAggTransformer",
        java_cluster_options_agg=java_cluster_options_agg,
        spark_conf_agg=id_agg_spark_conf,
        use_ebs=True,
    )

    avails_pipeline_dag >> latest_artefact_task

    latest_artefact_task >> etl_cluster

    etl_cluster >> publisher_agg
    etl_cluster >> deal_agg
    etl_cluster >> id_intermediate_agg

    id_intermediate_agg >> id_deal_agg
    id_intermediate_agg >> id_agg
    id_intermediate_agg >> deal_set_agg

    return avails_pipeline_dag.airflow_dag


def get_databricks_task(
    aws_region: str,
    agg_instances: int,
    agg_instance_type: str,
    config: AvailsPipelineRegionConfig,
    openlineage_namespace_agg: str,
    transformer: str,
    java_cluster_options_agg: List[Tuple[str, str]],
    spark_conf_agg: Dict[str, str],
    use_ebs: bool,
    coalesce_partitions: Optional[int] = None,
    short_name: Optional[str] = None,  # Databricks job cluster key has the maxumum length of 100 characters
    use_photon: bool = True,
) -> DatabricksWorkflow:

    eldorado_run_options = [
        ("hourToTransform", '{{ logical_date.strftime("%Y-%m-%dT%H:00:00") }}'),
        ("transformer", transformer),
        ("crossDeviceJoinSkewFactor", "4"),
        ("enableUsingIdentityGraphsClient", "true"),
    ]

    if coalesce_partitions is not None:
        for transformerName in transformer.split(","):
            eldorado_run_options.append((f"ttd.{transformerName}.coalescePartitions", str(coalesce_partitions)))

    if not short_name:
        short_name = transformer

    return DatabricksWorkflow(
        job_name=f"avails-agg-{aws_region}-{short_name}",
        cluster_name=openlineage_namespace_agg,
        databricks_instance_profile_arn=config.databricks_instance_profile_arn,
        cluster_tags={
            "Process": f"Aggregate-Avails-{aws_region}-{short_name}",
            "Team": "AIFUN"
        },
        databricks_spark_version=databricks_spark_version,
        worker_node_type=agg_instance_type,
        worker_node_count=agg_instances,
        ebs_config=DatabricksEbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=200) if use_ebs else None,
        use_photon=use_photon,
        eldorado_cluster_options_list=java_cluster_options_agg,
        spark_configs=spark_conf_agg,
        region=config.databricks_region,
        tasks=[
            SparkDatabricksTask(
                class_name="com.thetradedesk.availspipeline.spark.jobs.TransformEntryPoint",
                executable_path=get_artefact_for_dag_run(AVAILS_PIPELINE_JAR_FILE_NAME),
                job_name=f"avails-agg-{aws_region}-{short_name}",
                eldorado_run_options_list=eldorado_run_options,
                openlineage_config=OpenlineageConfig(transport=OpenlineageTransport.ROBUST),
            )
        ]
    )


us_east_1_dag = create_dag(
    aws_region="us-east-1",
    start_date=datetime(2023, 10, 25, 14, 0),
    etl_cores=4096,
    agg_instances=100,
    agg_shuffle_partitions=17920,
)

us_west_2_dag = create_dag(
    aws_region="us-west-2",
    start_date=datetime(2023, 12, 11, 16, 0),
    etl_cores=2048,
    agg_instances=80,
    agg_shuffle_partitions=10000,
    enable_prometheus_monitoring=False,
)

ap_northeast_1_dag = create_dag(
    aws_region="ap-northeast-1",
    start_date=datetime(2023, 12, 11, 16, 0),
    etl_cores=1024,
    agg_instances=40,
    agg_shuffle_partitions=7500,
    enable_prometheus_monitoring=False,
)

ap_southeast_1_dag = create_dag(
    aws_region="ap-southeast-1",
    start_date=datetime(2023, 12, 11, 16, 0),
    etl_cores=1024,
    agg_instances=40,
    agg_shuffle_partitions=7500,
    enable_prometheus_monitoring=False,
)

eu_west_1_dag = create_dag(
    aws_region="eu-west-1",
    start_date=datetime(2023, 12, 4, 16, 0),
    etl_cores=1024,
    agg_instances=40,
    agg_shuffle_partitions=7500,
)

eu_central_1_dag = create_dag(
    aws_region="eu-central-1",
    start_date=datetime(2023, 12, 4, 16, 0),
    etl_cores=1024,
    agg_instances=50,
    agg_shuffle_partitions=10000,
    # specifically in eu-central, we can't load bootstrap scripts properly.
    # probably has something to do with de1 deprecation. We need to move this cluster to
    # the production-europe account at some point, but that's a whole can of worms.
    # for now, just disable them
    enable_prometheus_monitoring=False,
    enable_spark_history_server_stats=False
)
