import copy
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask

# Parameters
dag_name = "adpb-adgroup-segment-performance-metrics-generation"
job_start_date = datetime(2025, 3, 11, 16, 0, 0)
job_schedule_interval = timedelta(hours=24)
owner = ADPB.team

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

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

override_adgroups_list: list[str] = ["xrsiwzg", "0o3e3k7"]

spark_options_list = [("executor-memory", "200G"), ("executor-cores", "30"), ("num-executors", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=50G"),
                      ("conf", "spark.driver.cores=10"), ("conf", "spark.sql.shuffle.partitions=5000"),
                      ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
                      ("conf", "spark.driver.maxResultSize=6G")]

eldorado_options_list = [
    ("runDate", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}"),
    ("overrideAdGroups", ",".join(override_adgroups_list)),
    ("maxLookbackDate", "2025-04-10"),  # this is intended and facilitates a schema change
    ('ttd.BidRequestVariantDataSet.isInChain', 'true'),
    ('ttd.BidRequestBfdeVariantDataSet.isInChain', 'true'),
    ('ttd.AdGroupSegmentSpendDailyDataSet.isInChain', 'true'),
    ('ttd.AdGroupSegmentConversionsDailyDataSet.isInChain', 'true'),
    ('ttd.AdGroupSegmentClickVideoEventsDailyDataSet.isInChain', 'true'),
    ('ttd.AdGroupSegmentKPIRollupDataSet.isInChain', 'true')
]

# DAG definition
adgroup_segment_performance_metrics_generation = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    enable_slack_alert=True
)


# Generate Sensors
def skip_downstream_on_timeout(context):
    exception = context['exception']
    if isinstance(exception, AirflowSensorTimeout):
        context['task_instance'].set_state("skipped")


dataset_names = [
    "bidrequest_variant", "bidrequest_bfde_variant", "adgroup_segment_spend_daily", "adgroup_segment_conversions_daily",
    "adgroup_segment_click_video_events_daily"
]

datasets = {
    dataset_name:
    DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        env_aware=True,
        data_name=f"segmentperformance/{dataset_name}",
        version=1,
        date_format="date=%Y%m%d",
        success_file=None
    )
    for dataset_name in dataset_names
}

sensors = {
    dataset_name:
    OpTask(
        op=DatasetCheckSensor(
            task_id=f"check_{dataset_name}",
            datasets=[datasets[dataset_name]],
            cloud_provider=CloudProviders.aws,
            dag=adgroup_segment_performance_metrics_generation.airflow_dag,
            ds_date='{{ data_interval_start.to_datetime_string() }}',
            poke_interval=60,
            timeout=60 * 3,
            on_failure_callback=skip_downstream_on_timeout
        ),
    )
    for dataset_name in dataset_names
}


# Cluster Definition
def get_default_cluster(cluster_name: str, on_demand_weighted_capacity: int = 32) -> EmrClusterTask:
    return EmrClusterTask(
        name=cluster_name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M5.m5_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
        ),
        cluster_tags={
            "Team": owner.jira_team,
        },
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R5.r5_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
                R5.r5_16xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(2),
                R5.r5_24xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(3)
            ],
            on_demand_weighted_capacity=on_demand_weighted_capacity
        ),
        additional_application_configurations=application_configuration,
        enable_prometheus_monitoring=True,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
    )


# Bid Request Variant
bid_request_variant_cluster = get_default_cluster("BidRequestVariantTransformerCluster")
bid_request_variant_step = EmrJobTask(
    cluster_specs=bid_request_variant_cluster.cluster_specs,
    name="BidRequestVariantTransformerStep",
    class_name="jobs.segmentperformance.BidRequestVariantTransformer",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(eldorado_options_list),
    executable_path=jar_path
)
bid_request_variant_cluster.add_sequential_body_task(bid_request_variant_step)

# Bid Request BFDE Variant
bid_request_bfde_variant_cluster = get_default_cluster("BidRequestBfdeVariantAggregatorCluster")
bid_request_bfde_variant_step = EmrJobTask(
    cluster_specs=bid_request_bfde_variant_cluster.cluster_specs,
    name="BidRequestBfdeVariantAggregatorStep",
    class_name="jobs.segmentperformance.BidRequestBfdeVariantAggregator",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(eldorado_options_list),
    executable_path=jar_path
)
bid_request_bfde_variant_cluster.add_sequential_body_task(bid_request_bfde_variant_step)

# AdGroup Segment Spend
adgroup_segment_spend_daily_aggregator_cluster = get_default_cluster("AdGroupSegmentSpendDailyAggregatorCluster")
adgroup_segment_spend_daily_aggregator_step = EmrJobTask(
    cluster_specs=adgroup_segment_spend_daily_aggregator_cluster.cluster_specs,
    name="AdGroupSegmentSpendDailyAggregatorStep",
    class_name="jobs.segmentperformance.AdGroupSegmentSpendDailyAggregator",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(eldorado_options_list),
    executable_path=jar_path
)
adgroup_segment_spend_daily_aggregator_cluster.add_sequential_body_task(adgroup_segment_spend_daily_aggregator_step)

# AdGroup Segment Conversions
adgroup_segment_conversions_daily_aggregator_cluster = get_default_cluster("AdGroupSegmentConversionsDailyAggregatorCluster")
adgroup_segment_conversions_daily_aggregator_step = EmrJobTask(
    cluster_specs=adgroup_segment_conversions_daily_aggregator_cluster.cluster_specs,
    name="AdGroupSegmentConversionsDailyAggregatorStep",
    class_name="jobs.segmentperformance.AdGroupSegmentConversionsDailyAggregator",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(eldorado_options_list) + [("conversionLookbackWindowDays", 14)],
    executable_path=jar_path
)
adgroup_segment_conversions_daily_aggregator_cluster.add_sequential_body_task(adgroup_segment_conversions_daily_aggregator_step)

# AdGroup Segment Click Video Events
adgroup_segment_click_video_events_daily_aggregator_cluster = get_default_cluster("AdGroupSegmentClickVideoEventsDailyAggregatorCluster")
adgroup_segment_click_video_events_daily_aggregator_step = EmrJobTask(
    cluster_specs=adgroup_segment_click_video_events_daily_aggregator_cluster.cluster_specs,
    name="AdGroupSegmentClickVideoEventsDailyAggregatorStep",
    class_name="jobs.segmentperformance.AdGroupSegmentClickVideoEventsDailyAggregator",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(eldorado_options_list),
    executable_path=jar_path
)
adgroup_segment_click_video_events_daily_aggregator_cluster.add_sequential_body_task(
    adgroup_segment_click_video_events_daily_aggregator_step
)

# AdGroup Segment KPI
kpi_rollup_trigger = OpTask(
    op=EmptyOperator(
        task_id="kpi_rollup_trigger",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        dag=adgroup_segment_performance_metrics_generation.airflow_dag
    )
)

adgroup_segment_kpi_rollup_aggregator_cluster = get_default_cluster("AdGroupSegmentKPIRollupAggregatorCluster", 12)
adgroup_segment_kpi_rollup_aggregator_step = EmrJobTask(
    cluster_specs=adgroup_segment_kpi_rollup_aggregator_cluster.cluster_specs,
    name="AdGroupSegmentKPIRollupAggregatorStep",
    class_name="jobs.segmentperformance.AdGroupSegmentKPIRollupAggregator",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=copy.deepcopy(eldorado_options_list) + [("rollupWindowInDays", 7)],
    executable_path=jar_path
)
adgroup_segment_kpi_rollup_aggregator_cluster.add_sequential_body_task(adgroup_segment_kpi_rollup_aggregator_step)

# Cluster Chain
dag = adgroup_segment_performance_metrics_generation.airflow_dag
check = OpTask(op=FinalDagStatusCheckOperator(dag=adgroup_segment_performance_metrics_generation.airflow_dag))

adgroup_segment_performance_metrics_generation >> bid_request_variant_cluster >> sensors["bidrequest_variant"]
sensors["bidrequest_variant"] >> bid_request_bfde_variant_cluster >> sensors["bidrequest_bfde_variant"]
sensors["bidrequest_variant"] >> adgroup_segment_spend_daily_aggregator_cluster >> sensors["adgroup_segment_spend_daily"]
sensors["bidrequest_bfde_variant"] >> adgroup_segment_conversions_daily_aggregator_cluster >> sensors["adgroup_segment_conversions_daily"]
sensors["bidrequest_bfde_variant"] >> adgroup_segment_click_video_events_daily_aggregator_cluster >> sensors[
    "adgroup_segment_click_video_events_daily"]
sensors["adgroup_segment_spend_daily"] >> kpi_rollup_trigger
sensors["adgroup_segment_conversions_daily"] >> kpi_rollup_trigger
sensors["adgroup_segment_click_video_events_daily"] >> kpi_rollup_trigger
kpi_rollup_trigger >> adgroup_segment_kpi_rollup_aggregator_cluster
adgroup_segment_kpi_rollup_aggregator_cluster >> check
