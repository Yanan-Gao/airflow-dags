from airflow import DAG

from datetime import datetime, timedelta

from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.openlineage import OpenlineageConfig
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import DEAL_MANAGEMENT

jar_path = "s3://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline.jar"


def get_cluster_and_task(
    aws_region: str,
    transformer: str,
    log_uri: str,
    core_fleet_capacity: int,
    enable_prometheus_monitoring: bool = True,
    enable_spark_history_server_stats: bool = False,
    partitions: int = 4000,
    **fleet_cluster_kwargs
) -> EmrClusterTask:

    standard_cluster_tags = {'Team': DEAL_MANAGEMENT.team.jira_team}
    std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[M5.m5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
        on_demand_weighted_capacity=1,
    )
    additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}

    cluster_task = EmrClusterTask(
        name=transformer + "Cluster-" + aws_region,
        log_uri=log_uri,
        master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R6g.r6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
                R6g.r6g_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            ],
            on_demand_weighted_capacity=core_fleet_capacity
        ),
        additional_application_configurations=[additional_application_configurations],
        cluster_tags={
            **standard_cluster_tags, "Process": transformer + "-" + aws_region
        },
        enable_prometheus_monitoring=enable_prometheus_monitoring,
        enable_spark_history_server_stats=enable_spark_history_server_stats,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
        region_name=aws_region,
        **fleet_cluster_kwargs
    )

    eldorado_config_option_pairs_list = [
        ('hourToTransform', '{{ logical_date.strftime("%Y-%m-%dT%H:00:00") }}'),
        ("transformer", transformer),
        ('ttd.' + transformer + '.coalescePartitions', str(partitions)),
    ]
    additional_args_option_pairs_list = [("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
                                         ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                         ("conf", f"spark.sql.shuffle.partitions={partitions}"),
                                         ("conf", "spark.sql.adaptive.enabled=true"), ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
                                         ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true")]

    job_task = EmrJobTask(
        cluster_specs=cluster_task.cluster_specs,
        name=transformer + "HourlyTransform",
        class_name="com.thetradedesk.availspipeline.spark.jobs.TransformEntryPoint",
        executable_path=jar_path,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
        region_name=aws_region,
        openlineage_config=OpenlineageConfig(enabled=OpenlineageConfig.supports_region(aws_region))
    )

    cluster_task.add_parallel_body_task(job_task)

    return cluster_task


def rollup_deals_aggregations_hourly_dag(
    aws_region: str,
    log_uri: str,
    core_fleet_capacity: int,
    job_start_date: datetime,
    enable_prometheus_monitoring: bool = True,
    enable_spark_history_server_stats: bool = False,
    partitions: int = 4000,
    **fleet_cluster_kwargs
) -> DAG:

    avails_pipeline_dag_v2: TtdDag = TtdDag(
        dag_id="contributing-deal-avails-agg-hourly-" + aws_region,
        start_date=job_start_date,
        schedule_interval=timedelta(hours=1),
        max_active_runs=5,
        retries=1,
        retry_delay=timedelta(minutes=2),
        slack_tags=DEAL_MANAGEMENT.team.jira_team,
        slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
        tags=[DEAL_MANAGEMENT.team.jira_team]
    )

    # the ETL is in airflow 2, so we need to wait on the data being ready
    etl_done = DatasetCheckSensor(
        dag=avails_pipeline_dag_v2.airflow_dag,
        ds_date="{{ logical_date.to_datetime_string() }}",
        datasets=[AvailsDatasources.avails_pipeline_raw_delta_dataset.with_check_type("hour").with_region(aws_region)],
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        timeout=60 * 60 * 4,  # 4 hours
        task_id="check_raw_avails_ready"
    )

    contributing_agg_cluster = get_cluster_and_task(
        aws_region=aws_region,
        transformer="ContributingDealAvailsAggTransformer",
        log_uri=log_uri,
        core_fleet_capacity=core_fleet_capacity,
        enable_prometheus_monitoring=enable_prometheus_monitoring,
        enable_spark_history_server_stats=enable_spark_history_server_stats,
        partitions=partitions,
        **fleet_cluster_kwargs
    )
    rollup_agg_cluster = get_cluster_and_task(
        aws_region=aws_region,
        transformer="RollupDealAvailsAggTransformer",
        log_uri=log_uri,
        core_fleet_capacity=round(core_fleet_capacity * 1.5),
        enable_prometheus_monitoring=enable_prometheus_monitoring,
        enable_spark_history_server_stats=enable_spark_history_server_stats,
        partitions=partitions,
        **fleet_cluster_kwargs
    )
    deal_matchtype_agg_cluster = get_cluster_and_task(
        aws_region=aws_region,
        transformer="DealMatchTypeAvailsAggTransformer",
        log_uri=log_uri,
        core_fleet_capacity=core_fleet_capacity,
        enable_prometheus_monitoring=enable_prometheus_monitoring,
        enable_spark_history_server_stats=enable_spark_history_server_stats,
        partitions=partitions,
        **fleet_cluster_kwargs
    )

    avails_pipeline_dag_v2 >> contributing_agg_cluster
    avails_pipeline_dag_v2 >> rollup_agg_cluster
    avails_pipeline_dag_v2 >> deal_matchtype_agg_cluster

    etl_done >> contributing_agg_cluster.first_airflow_op()
    etl_done >> rollup_agg_cluster.first_airflow_op()
    etl_done >> deal_matchtype_agg_cluster.first_airflow_op()

    return avails_pipeline_dag_v2.airflow_dag


job_start_date = datetime(2025, 7, 18, 0, 0)

us_east_1_dag = rollup_deals_aggregations_hourly_dag(
    aws_region="us-east-1",
    log_uri="s3://thetradedesk-useast-avails/emr-logs",
    core_fleet_capacity=1520,
    job_start_date=job_start_date,
    partitions=10000
)

us_west_2_dag = rollup_deals_aggregations_hourly_dag(
    aws_region="us-west-2",
    log_uri="s3://thetradedesk-uswest-2-avails/emr-logs",
    core_fleet_capacity=672,
    job_start_date=job_start_date,
    emr_managed_master_security_group="sg-0bf03a9cbbaeb0494",
    emr_managed_slave_security_group="sg-0dfc2e6a823862dbf",
    ec2_subnet_ids=EmrSubnets.PrivateUSWest2.all(),
    pass_ec2_key_name=False,
    service_access_security_group="sg-0ccb4ca554f6e1165",
    partitions=5000
)

ap_southeast_1_dag = rollup_deals_aggregations_hourly_dag(
    aws_region="ap-southeast-1",
    log_uri="s3://thetradedesk-sg2-avails/emr-logs",
    core_fleet_capacity=240,
    job_start_date=job_start_date,
    emr_managed_master_security_group="sg-014b895831026416d",
    emr_managed_slave_security_group="sg-03149058ce1479ab2",
    ec2_subnet_ids=EmrSubnets.PrivateAPSoutheast1.all(),
    pass_ec2_key_name=False,
    service_access_security_group="sg-008e3e75c75f7885d"
)

ap_northeast_1_dag = rollup_deals_aggregations_hourly_dag(
    aws_region="ap-northeast-1",
    log_uri="s3://thetradedesk-jp1-avails/emr-logs",
    core_fleet_capacity=144,
    job_start_date=job_start_date,
    emr_managed_master_security_group="sg-02cd06e673800a7d4",
    emr_managed_slave_security_group="sg-0a9b18bb4c0fa5577",
    ec2_subnet_ids=EmrSubnets.PrivateAPNortheast1.all(),
    pass_ec2_key_name=False,
    service_access_security_group="sg-0644d2eafc6dd2a8d",
    partitions=3000
)

eu_west_1_dag = rollup_deals_aggregations_hourly_dag(
    aws_region="eu-west-1",
    log_uri="s3://thetradedesk-ie1-avails/emr-logs",
    core_fleet_capacity=176,
    job_start_date=job_start_date,
    emr_managed_master_security_group="sg-081d59c2ec2e9ef68",
    emr_managed_slave_security_group="sg-0ff0115d48152d67a",
    ec2_subnet_ids=EmrSubnets.PrivateEUWest1.all(),
    pass_ec2_key_name=False,
    service_access_security_group="sg-06a23349af478630b"
)

eu_central_1_dag = rollup_deals_aggregations_hourly_dag(
    aws_region="eu-central-1",
    log_uri="s3://thetradedesk-de2-avails/emr-logs",
    core_fleet_capacity=352,
    job_start_date=job_start_date,
    emr_managed_master_security_group="sg-0a905c2e9d0b35fb8",
    emr_managed_slave_security_group="sg-054551f0756205dc8",
    ec2_subnet_ids=EmrSubnets.PrivateEUCentral1.all(),
    enable_prometheus_monitoring=False,
    enable_spark_history_server_stats=False,
    pass_ec2_key_name=False,
    service_access_security_group="sg-09a4d1b6a8145bd39"
)
