from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

owner = ADPB.team
jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

num_workers = 64
num_available_worker_cores = num_workers * (R5.r5_8xlarge().cores - 1)
number_audiences_to_process = 6000
num_partitions = max(num_available_worker_cores * 2, number_audiences_to_process)

inputSegmentCap = "18000"
enableAAExclusions = "true"

application_configuration = [{
    'Classification': 'spark',
    'Properties': {
        'maximizeResourceAllocation': 'true'
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "50",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

spark_options_list = [
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
    ("conf", "spark.dynamicAllocation.enabled=false"),
    ("conf", "spark.executor.heartbeatInterval=60s"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.driver.maxResultSize=32G"),
    ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
    ("conf", "spark.default.parallelism=%s" % num_partitions),
    ("conf", "spark.sql.parquet.enableVectorizedReader=false"),
]

audience_excluder_generation = TtdDag(
    dag_id="adpb-audience-excluder-generation",
    start_date=datetime(2025, 7, 3, 2, 0, 0),
    schedule_interval=timedelta(hours=2),
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    retries=0,
    dagrun_timeout=timedelta(hours=4)
)

cluster = EmrClusterTask(
    name="Audience_Excluder_Generation",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_16xlarge().with_ebs_size_gb(128).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_24xlarge().with_ebs_size_gb(192).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=num_workers
    ),
    cluster_tags={
        "Team": owner.jira_team,
    },
    additional_application_configurations=application_configuration,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    enable_prometheus_monitoring=True,
    environment=TtdEnvFactory.get_from_system()
)

selectioin_step = EmrJobTask(
    name="Audience_Excluder_Audience_Selection",
    class_name="jobs.audienceexcluder.AudienceExcluderAudienceSelection",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[
        ("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
        ("debugTrace", "false"),
        ("numberOfAudiencesToProcess", "6000"),
        ("inputSegmentCap", inputSegmentCap),
        ("potentialToActualImpressionRatioToSkipAudience", "1.25"),
        ("enableAAExclusions", enableAAExclusions),
        ("audiencerefreshperiod", "4"),  # refresh all audience every x days, default 3
        ("audienceprocesscadence", "2"),  # process cadence, equals to ApplyAEV2 sproc delay, default 1
        ("aeHistoryLookBackDays", "21"),  # AE history lookback, used as deduplication and identifying custom segments
        ("aeHistoryStarvationReCheckDays", "2"),
    ],
    timeout_timedelta=timedelta(hours=1),
    cluster_specs=cluster.cluster_specs,
)

generation_step = EmrJobTask(
    name="Audience_Excluder_Generation",
    class_name="jobs.audienceexcluder.AudienceExcluderGeneration",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"), ("debugTrace", "false"),
                                       ("inputSegmentCap", inputSegmentCap), ('defaultAAExclusionTargetingDataId', "345209213"),
                                       ("enableAAExclusions", enableAAExclusions), ("enableCustomSegment", "2"),
                                       ("computeBrands", "false")],
    timeout_timedelta=timedelta(hours=3),
    cluster_specs=cluster.cluster_specs,
)

cluster.add_sequential_body_task(selectioin_step)
cluster.add_sequential_body_task(generation_step)
selectioin_step >> generation_step

final_check = OpTask(op=FinalDagStatusCheckOperator(dag=audience_excluder_generation.airflow_dag))

audience_excluder_generation >> cluster >> final_check

dag = audience_excluder_generation.airflow_dag
