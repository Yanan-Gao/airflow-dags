import copy
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.adpb.datasets.datasets import get_rsm_id_relevance_score, get_rsm_seed_ids, get_rsm_seed_population_scores
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

redisHosts = "adpb-redis-prod-0001-001.adpb-redis-prod.hoonr9.use1.cache.amazonaws.com:6379;adpb-redis-prod-0001-002.adpb-redis-prod.hoonr9.use1.cache.amazonaws.com:6379;adpb-redis-prod-0001-003.adpb-redis-prod.hoonr9.use1.cache.amazonaws.com:6379" \
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "adpb-redis-non-prod-0001-001.adpb-redis-non-prod.hoonr9.use1.cache.amazonaws.com:6379;adpb-redis-non-prod-0001-002.adpb-redis-non-prod.hoonr9.use1.cache.amazonaws.com:6379;adpb-redis-non-prod-0001-003.adpb-redis-non-prod.hoonr9.use1.cache.amazonaws.com:6379"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000"
    }
}, {
    "Classification": "core-site",
    "Properties": {
        "fs.s3a.connection.maximum": "1000",
        "fs.s3a.threads.max": "50"
    }
}, {
    "Classification": "capacity-scheduler",
    "Properties": {
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}, {
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.vmem-check-enabled": "false"
    }
}]

spark_options_list = [("executor-memory", "615G"), ("executor-cores", "96"), ("num-executors", "80"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=50G"),
                      ("conf", "spark.driver.cores=10"), ("conf", "spark.driver.maxResultSize=16G"),
                      ("conf", "spark.sql.shuffle.partitions=8000"), ("conf", "spark.dynamicAllocation.enabled=false"),
                      ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

rsm_spark_options_list = [("executor-memory", "400G"), ("executor-cores", "64"), ("num-executors", "120"),
                          ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=50G"),
                          ("conf", "spark.driver.cores=10"), ("conf", "spark.driver.maxResultSize=16G"),
                          ("conf", "spark.sql.shuffle.partitions=30720"), ("conf", "spark.dynamicAllocation.enabled=false"),
                          ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
                          ("conf", "spark.shuffle.io.connectionTimeout=600s")]

export_redis_spark_options_list = [("executor-memory", "36G"), ("executor-cores", "16"), ("num-executors", "4"),
                                   ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                   ("conf", "spark.driver.memory=47G"), ("conf", "spark.driver.cores=16"),
                                   ("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.sql.shuffle.partitions=8000"),
                                   ("conf", "spark.dynamicAllocation.enabled=false"),
                                   ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

date = '{{ data_interval_start.strftime("%Y%m%d") }}'

au_tile_relevance_dag = TtdDag(
    dag_id="adpb-au-tile-relevance",
    start_date=datetime(2025, 3, 12, 23, 0),
    schedule_interval=timedelta(days=1),
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True
)

check_rsm_datasets_sensor_task = OpTask(
    op=DatasetCheckSensor(
        datasets=[get_rsm_id_relevance_score(split, env="prod")
                  for split in range(0, 10)] + [get_rsm_seed_ids(env="prod")] + [get_rsm_seed_population_scores(env="prod")],
        ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d 00:00:00\") }}",
        task_id='check_rsm_datasets',
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 24,  # wait up to 24 hours
    )
)

# UER Seed Audience Relevance Generation
audience_aggregation_cluster = EmrClusterTask(
    name="AuTileRelevanceAudienceAggregateGenerationCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_24xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=80
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

audience_aggregate_generation_step = EmrJobTask(
    name="AuTileRelevanceAudienceAggregateGeneration",
    class_name="jobs.advertiserautile.RoaringBitmapBasedAudienceAggregatorJob",
    additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
    eldorado_config_option_pairs_list=[("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                       ("active_audience_rolling_update_window_days", 5),
                                       ("inactive_audience_rolling_update_window_days", 90), ("old_audience_look_back_days", 30),
                                       ("frequency_look_back_days", 7), ("max_frequency_count", 24 * 2 * 7),
                                       ("num_ids_per_segment_to_keep", 100000),
                                       (
                                           "rsm_relevance_score_dataset_path",
                                           f"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/tdid2seedid/v=1/date={date}"
                                       )],
    executable_path=jar_path,
    timeout_timedelta=timedelta(hours=8)
)
audience_aggregation_cluster.add_sequential_body_task(audience_aggregate_generation_step)

rsm_based_audience_relevance_calculation_cluster = EmrClusterTask(
    name="RSMBasedSeedAudienceRelevanceGenerationCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)
        ],
        on_demand_weighted_capacity=120
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

rsm_based_seed_audience_relevance_generation_step = EmrJobTask(
    name="RSMBasedSeedAudienceRelevanceGeneration",
    class_name="jobs.advertiserautile.RSMBasedSeedAudienceRelevanceCalculatorJob",
    additional_args_option_pairs_list=copy.deepcopy(rsm_spark_options_list),
    eldorado_config_option_pairs_list=[
        ("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"), ("rolling_update_window_days", 90),
        ("rsm_seed_id_dataset_path", f"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/seedids/v=2/date={date}"),
        (
            "rsm_relevance_score_dataset_path",
            f"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/tdid2seedid/v=1/date={date}"
        ),
        (
            "population_seed_score_dataset_path",
            f"s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/seedpopulationscore/v=1/date={date}"
        ), ("max_audience_sample_size", 100000), ("rolling_update_window_days", 90), ("relevance_smoothening_max_seed_size", 12500),
        ("num_nodes", 119), ("available_cores", 63)
    ],
    executable_path=jar_path,
    timeout_timedelta=timedelta(hours=6)
)

rsm_based_audience_relevance_calculation_cluster.add_sequential_body_task(rsm_based_seed_audience_relevance_generation_step)

# Export Redis Cluster
export_redis_cluster = EmrClusterTask(
    name="AuTileExportRedis",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            M7g.m7g_4xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1),
            M7g.m7gd_4xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=4
    ),
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

export_redis_step = EmrJobTask(
    name="ExportRelevanceToRedis",
    class_name="jobs.advertiserautile.ExportRelevanceToRedisJob",
    additional_args_option_pairs_list=export_redis_spark_options_list,
    eldorado_config_option_pairs_list=[("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                       ("redis_hosts", redisHosts), ("write_rsm_relevance", "true")],
    executable_path=jar_path
)
export_redis_cluster.add_sequential_body_task(export_redis_step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=au_tile_relevance_dag.airflow_dag))

au_tile_relevance_dag >> check_rsm_datasets_sensor_task >> audience_aggregation_cluster
audience_aggregation_cluster >> rsm_based_audience_relevance_calculation_cluster
rsm_based_audience_relevance_calculation_cluster >> export_redis_cluster
export_redis_cluster >> check

dag = au_tile_relevance_dag.airflow_dag
