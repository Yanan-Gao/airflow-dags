from datetime import datetime, timedelta
from typing import Optional, Sequence, Tuple
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.ttdenv import TtdEnvFactory
from ttd.slack.slack_groups import AIFUN, MEASUREMENT_UPPER, IDENTITY

emr_version = AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2
emr_cluster_tags = {"Team": AIFUN.team.jira_team}

avails_pipeline_jar = "oss://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline-ali.jar"


class AvailsPipelineEmrClusterConfig:

    def __init__(self, master_instance_type, core_instance_type, shuffle_partitions: int):
        self.master_instance_type = master_instance_type
        self.core_instance_type = core_instance_type
        self.shuffle_partitions = shuffle_partitions


class AvailsPipelineRegionConfig:

    def __init__(self, etl_config, agg_config):
        self.etl_config = etl_config
        self.agg_config = agg_config


configs_by_region: dict[str, AvailsPipelineRegionConfig] = {
    "ChinaEast2":
    AvailsPipelineRegionConfig(
        etl_config=AvailsPipelineEmrClusterConfig(
            master_instance_type=ElDoradoAliCloudInstanceTypes(
                instance_type=AliCloudInstanceTypes.ECS_G6_X()
            ).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(
                instance_type=AliCloudInstanceTypes.ECS_G6_X()
            ).with_node_count(8).with_data_disk_count(4).with_data_disk_size_gb(100).with_sys_disk_size_gb(120),
            shuffle_partitions=8192
        ),
        agg_config=AvailsPipelineEmrClusterConfig(
            master_instance_type=ElDoradoAliCloudInstanceTypes(
                instance_type=AliCloudInstanceTypes.ECS_G6_4X()
            ).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(
                instance_type=AliCloudInstanceTypes.ECS_G6_4X()
            ).with_node_count(8).with_data_disk_count(4).with_data_disk_size_gb(100).with_sys_disk_size_gb(120),
            shuffle_partitions=8192
        )
    )
}


def create_dag(cloud_region: str, start_date: datetime, end_date: Optional[datetime] = None):
    configs = configs_by_region[cloud_region]

    avails_pipeline_dag: TtdDag = TtdDag(
        dag_id="avails-pipeline-hourly-" + cloud_region + "-airflow-2",
        start_date=start_date,
        end_date=end_date,
        schedule_interval=timedelta(hours=1),
        max_active_runs=5 if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 1,
        retries=1,
        retry_delay=timedelta(minutes=2),
        slack_tags=AIFUN.team.jira_team,
        enable_slack_alert=False,
        teams_allowed_to_access=[MEASUREMENT_UPPER.team.jira_team, IDENTITY.team.jira_team]
    )

    def create_cluster(
        cluster_name: str,
        step_name: str,
        class_name: str,
        config: AvailsPipelineEmrClusterConfig,
        eldorado_configs: Optional[Sequence[Tuple[str, str]]] = None
    ) -> AliCloudClusterTask:
        cluster = AliCloudClusterTask(
            name=cluster_name,
            emr_version=emr_version,
            cluster_tags=emr_cluster_tags,
            master_instance_type=config.master_instance_type,
            core_instance_type=config.core_instance_type,
            retries=1 if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 0
        )

        eldorado_configs_final = [
            ("hourToTransform", '{{ logical_date.strftime("%Y-%m-%dT%H:00:00") }}'),
            ("availsStorageSystem", "oss"),
            ("ttd.AvailsRawDataSet.storageSystem", "oss"),
            ("ttd.AvailsRawDataSet.dataExpirationInDays", "30"),
            ("ttd.DealAvailsAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.PublisherAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.IdentityAvailsAggHourlyDataSetV2.storageSystem", "oss"),
            ("ttd.IdentityAndDealAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.DealSetAvailsAggHourlyDataset.storageSystem", "oss"),
        ]
        if eldorado_configs is not None:
            eldorado_configs_final.extend(eldorado_configs)

        step = AliCloudJobTask(
            name=step_name,
            jar_path=avails_pipeline_jar,
            class_name=class_name,
            additional_args_option_pairs_list=[
                ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
                ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                ("spark.sql.shuffle.partitions", config.shuffle_partitions),
                ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
                ("spark.databricks.delta.vacuum.parallelDelete.enabled", "true"),
                ("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true"),
            ],
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=eldorado_configs_final
        )
        cluster.add_parallel_body_task(step)
        return cluster

    etl_cluster = create_cluster(
        cluster_name=f"AvailsPipelineProtoConvert-{cloud_region}",
        step_name="ProtoConvert",
        class_name="com.thetradedesk.availspipeline.spark.jobs.HourlyReformatProtoAvailsOperation",
        config=configs.etl_config
    )
    cores = configs.agg_config.core_instance_type.instance_type.cores * configs.agg_config.core_instance_type.node_count
    agg_cluster = create_cluster(
        cluster_name=f"avails-agg-{cloud_region}",
        step_name="avails-agg",
        class_name="com.thetradedesk.availspipeline.spark.jobs.HourlyTransformOperation",
        config=configs.agg_config,
        eldorado_configs=[("ttd.IdentityAndDealTransformer.coalescePartitions", cores * 2)]
    )

    avails_pipeline_dag >> etl_cluster >> agg_cluster
    return avails_pipeline_dag.airflow_dag


china_east_2_dag = create_dag(cloud_region="ChinaEast2", start_date=datetime(2025, 3, 26, 1, 0), end_date=None)
