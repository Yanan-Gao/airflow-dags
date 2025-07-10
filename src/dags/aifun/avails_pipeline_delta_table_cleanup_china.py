from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnv, TtdEnvFactory
from ttd.slack.slack_groups import AIFUN, MEASUREMENT_UPPER, IDENTITY
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes

jar_path = "oss://ttd-build-artefacts/avails-pipeline/master/latest/availspipeline-spark-pipeline-ali.jar"
if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
    jar_path = "oss://ttd-build-artefacts/avails-pipeline/yxw-MEASURE-6858-catch-exceptions-when-vacum/latest/availspipeline-spark-pipeline-ali.jar"

master_instance_type = (ElDoradoAliCloudInstanceTypes(instance_type=AliCloudInstanceTypes.ECS_G6_4X()).with_node_count(1))

core_instance_type = (ElDoradoAliCloudInstanceTypes(instance_type=AliCloudInstanceTypes.ECS_G6_6X()).with_node_count(8))


def create_dag():
    avails_pipeline_dag = TtdDag(
        dag_id="avails-pipeline-aggregate-delta-table-cleanup-china",
        start_date=datetime(2025, 5, 11, 1, 0),
        schedule_interval=timedelta(days=1),
        retries=1,
        retry_delay=timedelta(minutes=2),
        slack_tags=AIFUN.team.jira_team,
        enable_slack_alert=False,
        teams_allowed_to_access=[MEASUREMENT_UPPER.team.jira_team, IDENTITY.team.jira_team],
    )

    def create_cluster(environment: TtdEnv = None):
        step_name_suffix = f"-{str(environment)}-env" if environment is not None else ""

        delta_cleanup_cluster = AliCloudClusterTask(
            name=f"AvailsPipelineDeltaCleanup-Ali_China_East_2{step_name_suffix}",
            emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2,
            cluster_tags={"Team": AIFUN.team.jira_team},
            master_instance_type=master_instance_type,
            core_instance_type=core_instance_type,
            retries=1 if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 0,
            environment=environment or TtdEnvFactory().get_from_system(),
        )

        step_kwargs = {
            "cluster_spec":
            delta_cleanup_cluster.cluster_specs,
            "class_name":
            "com.thetradedesk.availspipeline.spark.jobs.DeltaCleanupTask",
            "jar_path":
            jar_path,
            "additional_args_option_pairs_list": [
                ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
                ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                ("spark.sql.shuffle.partitions", "16"),
                ("spark.dynamicAllocation.enabled", "false"),
                ("spark.executor.heartbeatInterval", "30s"),
                ("spark.driver.maxResultSize", "4g"),
                ("spark.driver.memory", "32G"),
                ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
                ("spark.databricks.delta.vacuum.parallelDelete.enabled", "true"),
                ("spark.kryoserializer.buffer.max", "256m"),
                ("spark.executor.heartbeatInterval", "60s"),

                # Dynamo stuff
                ("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true"),
            ],
            "configure_cluster_automatically":
            True,
        }

        eldorado_config_option_pairs_list = [
            ('runDate', '{{ logical_date.strftime(\"%Y-%m-%d\") }}'),
            ("ttd.AvailsRawDataSet.storageSystem", "oss"),
            ("ttd.CleanRoomHouseholdForecastingAvailsHourlyAggDataSet.storageSystem", "oss"),
            ("ttd.DealAvailsAggDailyDataSet.storageSystem", "oss"),
            ("ttd.DealAvailsAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.DealSetAvailsAggHourlyDataset.storageSystem", "oss"),
            ("ttd.IdentityAndDealAggHourlyDataSet.storageSystem", "oss"),
            ("ttd.IdentityAvailsAggDailyDataSetV2.storageSystem", "oss"),
            ("ttd.IdentityAvailsAggHourlyDataSetV2.storageSystem", "oss"),
            ("ttd.IdentityHouseholdIntermediateDataSet.storageSystem", "oss"),
            ("ttd.PublisherAggDailyDataSet.storageSystem", "oss"),
            ("ttd.PublisherAggHourlyDataSet.storageSystem", "oss"),
        ]

        delta_delete_step = AliCloudJobTask(
            name=f"DeltaDeleteTask{step_name_suffix}",
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + [('deltaOperation', 'delete')],
            **step_kwargs
        )

        delta_vacuum_step = AliCloudJobTask(
            name=f"DeltaVacuumTask{step_name_suffix}",
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + [('deltaOperation', 'vacuum')],
            **step_kwargs
        )

        delta_cleanup_cluster.add_parallel_body_task(delta_delete_step)
        delta_cleanup_cluster.add_parallel_body_task(delta_vacuum_step)

        delta_delete_step >> delta_vacuum_step
        return delta_cleanup_cluster

    cluster_prod = create_cluster()
    cluster_test = create_cluster(environment=TtdEnvFactory.get_from_str("test"))

    avails_pipeline_dag >> cluster_prod >> cluster_test

    return avails_pipeline_dag


dag = create_dag().airflow_dag
