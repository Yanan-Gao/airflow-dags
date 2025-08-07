from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.slack.slack_groups import AIFUN
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from datasources.sources.avails_datasources import AvailsDatasources

jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline.jar"
log_uri = "s3://thetradedesk-useast-avails/emr-logs"

aws_region = "us-east-1"
core_fleet_capacity = 12288
dependent_dataset_list = [
    AvailsDatasources.deal_agg_hourly_dataset.with_check_type("day").with_region(aws_region),
    AvailsDatasources.identity_agg_hourly_dataset.with_check_type("day").with_region(aws_region),
    AvailsDatasources.publisher_agg_hourly_dataset.with_check_type("day").with_region(aws_region)
]

standard_cluster_tags = {'Team': AIFUN.team.jira_team}

std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32),
    ],
    on_demand_weighted_capacity=1,
)

std_core_instance_types = [
    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
]

additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

ttd_dag = TtdDag(
    dag_id="avails-pipeline-daily",
    start_date=datetime(2024, 7, 10, 1, 0),
    schedule_interval=timedelta(days=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_tags=AIFUN.team.jira_team,
    enable_slack_alert=False
)

daily_cluster = EmrClusterTask(
    name="AvailsPipelineDaily-" + aws_region,
    log_uri=log_uri,
    master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=std_core_instance_types,
        on_demand_weighted_capacity=core_fleet_capacity,
    ),
    additional_application_configurations=[additional_application_configurations],
    cluster_tags={
        **standard_cluster_tags,
        "Process": "Aggregate-Avails-Daily-" + aws_region,
    },
    enable_prometheus_monitoring=True,
    enable_spark_history_server_stats=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    region_name=aws_region,
)

transformers = [
    "DealAvailAggDailyRollup",
    "IdentityAvailsAggDailyRollupV2",
    "PublisherAvailsAggDailyRollup",
]

for transformer in transformers:
    daily_transform_step = EmrJobTask(
        cluster_specs=daily_cluster.cluster_specs,
        name=f"DailyTransform-{transformer}-{aws_region}",
        class_name="com.thetradedesk.availspipeline.spark.jobs.TransformEntryPoint",
        executable_path=jar_path,
        additional_args_option_pairs_list=[
            ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
            ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("conf", "spark.sql.shuffle.partitions=15000"),

            # Dynamo stuff
            ("conf", "spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore"),
            ("conf", "spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore"),
            ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-east-1"),
            ("conf", "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=avails_pipeline_delta_log"),
        ],
        eldorado_config_option_pairs_list=[
            ('hourToTransform', '{{ logical_date.strftime(\"%Y-%m-%dT00:00:00\") }}'),
            ('transformer', transformer),
        ],
        region_name=aws_region,
    )

    daily_cluster.add_sequential_body_task(daily_transform_step)

ttd_dag >> daily_cluster

ds_op = DatasetCheckSensor(
    dag=ttd_dag.airflow_dag,
    task_id="check-dependencies-" + aws_region,
    ds_date="{{ logical_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    # wait 6 hours for hourly datasets. should be enough to account for long runtimes or failures/retries of 23:00 hour
    timeout=60 * 60 * 6,
    datasets=dependent_dataset_list
)

ds_op >> daily_cluster.first_airflow_op()

dag = ttd_dag.airflow_dag
