from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.slack.slack_groups import AIFUN
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.ttdenv import TtdEnv, TtdEnvFactory

jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline.jar"
log_uri = "s3://thetradedesk-useast-avails/emr-logs"
core_fleet_capacity = 512

standard_cluster_tags = {'Team': AIFUN.team.jira_team}

std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
)

std_core_instance_types = [
    M6g.m6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16).with_ebs_size_gb(1024),
    M5.m5_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16).with_ebs_size_gb(1024),
    M6g.m6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(2048),
    M5.m5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(2048),
]

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

cluster_kwargs_by_region = {
    "us-east-1": {},
    "us-west-2": {
        "emr_managed_master_security_group": "sg-0bf03a9cbbaeb0494",
        "emr_managed_slave_security_group": "sg-0dfc2e6a823862dbf",
        "ec2_subnet_ids": EmrSubnets.PrivateUSWest2.all(),
        "ec2_key_name": None,
        "service_access_security_group": "sg-0ccb4ca554f6e1165",
    },
    "ap-northeast-1": {
        "emr_managed_master_security_group": "sg-02cd06e673800a7d4",
        "emr_managed_slave_security_group": "sg-0a9b18bb4c0fa5577",
        "ec2_subnet_ids": EmrSubnets.PrivateAPNortheast1.all(),
        "ec2_key_name": None,
        "service_access_security_group": "sg-0644d2eafc6dd2a8d",
    },
    "ap-southeast-1": {
        "emr_managed_master_security_group": "sg-014b895831026416d",
        "emr_managed_slave_security_group": "sg-03149058ce1479ab2",
        "ec2_subnet_ids": EmrSubnets.PrivateAPSoutheast1.all(),
        "ec2_key_name": None,
        "service_access_security_group": "sg-008e3e75c75f7885d",
    },
    "eu-west-1": {
        "emr_managed_master_security_group": "sg-081d59c2ec2e9ef68",
        "emr_managed_slave_security_group": "sg-0ff0115d48152d67a",
        "ec2_subnet_ids": EmrSubnets.PrivateEUWest1.all(),
        "ec2_key_name": None,
        "service_access_security_group": "sg-06a23349af478630b",
    },
    "eu-central-1": {
        "emr_managed_master_security_group": "sg-0a905c2e9d0b35fb8",
        "emr_managed_slave_security_group": "sg-054551f0756205dc8",
        "ec2_subnet_ids": EmrSubnets.PrivateEUCentral1.all(),
        "ec2_key_name": None,
        "service_access_security_group": "sg-09a4d1b6a8145bd39",
    },
}


def create_dag(environment: TtdEnv = None):
    dag_id_suffix = f"-{str(environment)}-env" if environment is not None else ""

    avails_pipeline_dag = TtdDag(
        dag_id=f"avails-pipeline-aggregate-delta-table-cleanup{dag_id_suffix}",
        start_date=datetime(2024, 12, 1, 1, 0),
        schedule_interval=timedelta(days=1),
        retries=1,
        retry_delay=timedelta(minutes=2),
        slack_tags=AIFUN.team.jira_team,
        enable_slack_alert=False,
    )

    for aws_region, kwargs in cluster_kwargs_by_region.items():
        enable_bootstraps = aws_region != 'eu-central-1'
        delta_cleanup_cluster = EmrClusterTask(
            name="AvailsPipelineDeltaCleanup-" + aws_region,
            log_uri=log_uri,
            master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=
            EmrFleetInstanceTypes(instance_types=std_core_instance_types, on_demand_weighted_capacity=core_fleet_capacity),
            additional_application_configurations=[additional_application_configurations],
            cluster_tags={
                **standard_cluster_tags, "Process": "Delta-Cleanup-Daily-" + aws_region
            },
            enable_prometheus_monitoring=enable_bootstraps,
            enable_spark_history_server_stats=enable_bootstraps,
            pass_ec2_key_name=False,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
            region_name=aws_region,
            environment=environment or TtdEnvFactory().get_from_system(),
            **kwargs
        )

        config_args = [
            ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
            ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("conf", f"spark.sql.shuffle.partitions={core_fleet_capacity}"),
            ("conf", "spark.dynamicAllocation.enabled=false"),
            ("conf", "spark.executor.heartbeatInterval=30s"),
            ("conf", "spark.driver.maxResultSize=4g"),
            ("conf", "spark.driver.memory=32G"),
            ("conf", "spark.databricks.delta.retentionDurationCheck.enabled=false"),
            ("conf", "spark.databricks.delta.vacuum.parallelDelete.enabled=true"),

            # Dynamo stuff
            ("conf", "spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore"),
            ("conf", "spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore"),
            ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-east-1"),
            ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=avails_pipeline_delta_log"),
            ("conf", "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled=true"),
        ]

        delta_delete_step = EmrJobTask(
            cluster_specs=delta_cleanup_cluster.cluster_specs,
            name="DeltaDeleteTask",
            class_name="com.thetradedesk.availspipeline.spark.jobs.DeltaCleanupTask",
            executable_path=jar_path,
            additional_args_option_pairs_list=config_args,
            eldorado_config_option_pairs_list=[
                ('runDate', '{{ logical_date.strftime(\"%Y-%m-%d\") }}'),
                ("enableUsingIdentityGraphsClient", "true"),
                ('deltaOperation', 'delete'),
            ],
            region_name=aws_region
        )

        delta_vacuum_step = EmrJobTask(
            cluster_specs=delta_cleanup_cluster.cluster_specs,
            name="DeltaVacuumTask",
            class_name="com.thetradedesk.availspipeline.spark.jobs.DeltaCleanupTask",
            executable_path=jar_path,
            additional_args_option_pairs_list=config_args,
            eldorado_config_option_pairs_list=[
                ('runDate', '{{ logical_date.strftime(\"%Y-%m-%d\") }}'),
                ("enableUsingIdentityGraphsClient", "true"),
                ('deltaOperation', 'vacuum'),
            ],
            region_name=aws_region
        )

        delta_cleanup_cluster.add_parallel_body_task(delta_delete_step)
        delta_cleanup_cluster.add_parallel_body_task(delta_vacuum_step)

        delta_delete_step >> delta_vacuum_step

        avails_pipeline_dag >> delta_cleanup_cluster

    return avails_pipeline_dag


prod_dag = create_dag().airflow_dag
test_dag = create_dag(environment=TtdEnvFactory.get_from_str("test")).airflow_dag
