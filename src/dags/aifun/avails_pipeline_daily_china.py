from datetime import datetime, timedelta

from ttd.slack.slack_groups import AIFUN, MEASUREMENT_UPPER, IDENTITY
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.ttdenv import TtdEnvFactory
from ttd.cloud_provider import CloudProviders

emr_version = AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2
emr_cluster_tags = {"Team": AIFUN.team.jira_team}

master_instance_type = (
    ElDoradoAliCloudInstanceTypes(instance_type=AliCloudInstanceTypes.ECS_G6_4X()
                                  ).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60)
)

core_instance_type = (
    ElDoradoAliCloudInstanceTypes(instance_type=AliCloudInstanceTypes.ECS_G6_6X()
                                  ).with_node_count(24).with_data_disk_count(4).with_data_disk_size_gb(100).with_sys_disk_size_gb(120)
)

jar_path = "oss://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline-ali.jar"
region = "Ali_China_East_2"

dependent_dataset_list = [
    AvailsDatasources.deal_agg_hourly_dataset.with_check_type("day").with_region(region).with_cloud(CloudProviders.ali),
    AvailsDatasources.identity_agg_hourly_dataset.with_check_type("day").with_region(region).with_cloud(CloudProviders.ali),
    AvailsDatasources.publisher_agg_hourly_dataset.with_check_type("day").with_region(region).with_cloud(CloudProviders.ali),
]

ttd_dag = TtdDag(
    dag_id="avails-pipeline-daily-china",
    start_date=datetime(2025, 4, 10, 0, 0),
    schedule_interval=timedelta(days=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_tags=AIFUN.team.jira_team,
    enable_slack_alert=False,
    teams_allowed_to_access=[MEASUREMENT_UPPER.team.jira_team, IDENTITY.team.jira_team],
    run_only_latest=None if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False
)

daily_cluster = AliCloudClusterTask(
    name="AvailsPipelineDaily-" + region,
    emr_version=emr_version,
    cluster_tags=emr_cluster_tags,
    master_instance_type=master_instance_type,
    core_instance_type=core_instance_type,
    retries=1 if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 0
)

transformers = [
    "DealAvailAggDailyRollup",
    "IdentityAvailsAggDailyRollupV2",
    "PublisherAvailsAggDailyRollup",
]

first_step = True

for transformer in transformers:
    daily_transform_step = AliCloudJobTask(
        name=f"DailyTransform-{transformer}-{region}",
        jar_path=jar_path,
        class_name="com.thetradedesk.availspipeline.spark.jobs.TransformEntryPoint",
        additional_args_option_pairs_list=[
            ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("spark.sql.shuffle.partitions", 15000),
        ],
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=[
            ('hourToTransform', '{{ logical_date.strftime(\"%Y-%m-%dT00:00:00\") }}'),
            ('transformer', transformer),
            ("availsStorageSystem", "oss"),
            ("ttd.AvailsRawDataSet.storageSystem", "oss"),
            ("ttd.AvailsRawDataSet.dataExpirationInDays", "30"),
            ("ttd.DealAvailsAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.PublisherAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.IdentityAvailsAggHourlyDataSetV2.storageSystem", "oss"),
            ("ttd.IdentityAndDealAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.DealSetAvailsAggHourlyDataset.storageSystem", "oss"),
            ("ttd.DealAvailsAggDailyDataSet.storageSystem", "oss"),
            ("ttd.PublisherAggDailyDataSet.storageSystem", "oss"),
            ("ttd.IdentityAvailsAggDailyDataSetV2.storageSystem", "oss"),
        ]
    )
    # if use add_sequential_body_task for all steps, no steps are added.
    if first_step:
        daily_cluster.add_parallel_body_task(daily_transform_step)
    else:
        daily_cluster.add_sequential_body_task(daily_transform_step)
    first_step = False

ttd_dag >> daily_cluster

ds_op = DatasetCheckSensor(
    dag=ttd_dag.airflow_dag,
    task_id="check-dependencies-" + region,
    ds_date="{{ logical_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    # wait 6 hours for hourly datasets. should be enough to account for long runtimes or failures/retries of 23:00 hour
    timeout=60 * 60 * 6,
    datasets=dependent_dataset_list,
    cloud_provider=CloudProviders.ali
)

ds_op >> daily_cluster.first_airflow_op()

dag = ttd_dag.airflow_dag
