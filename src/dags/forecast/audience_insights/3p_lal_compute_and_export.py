from datetime import datetime, timedelta

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import FORECAST

# Config values
executionIntervalDays = 1
execution_date_key = 'execution_date'
executable_path = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'

current_emr_version = "emr-6.7.0"

standard_cluster_tags = {
    'Team': FORECAST.team.jira_team,
}

aerospike_hosts = "{{macros.ttd_extras.resolve_consul_url('ttd-lal.aerospike.service.useast.consul', port=3000, limit=1)}}"

third_party_lal_insights_dag = TtdDag(
    dag_id='audience-thirdparty-lal-insights',
    start_date=datetime(2024, 6, 1, 1, 0),
    schedule_interval="0 1-23/4 * * *",  # runs every 4 hours starting at 1am
    retries=2,
    retry_delay=timedelta(minutes=10),
    slack_tags=FORECAST.team.sub_team,
    enable_slack_alert=True,
    tags=['FORECAST'],
    run_only_latest=True
)

third_party_lal_count_cluster = EmrClusterTask(
    name="ThirdPartyLALCountCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5a.r5a_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(96),
            R5.r5_24xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(96),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96)
        ],
        on_demand_weighted_capacity=96 * 100,
    ),
    cluster_tags={
        **standard_cluster_tags, "Process": "ThirdPartyLAL"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

third_party_lal_count_step = EmrJobTask(
    name="ThirdPartyLALCounts",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.thirdpartylal.ThirdPartyTargetingDataCount",
    executable_path=executable_path,
    additional_args_option_pairs_list=[("executor-memory", "620G"), ("executor-cores", "95"), ("num-executors", "99"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=" + "620G"), ("conf", "spark.driver.cores=" + "95"),
                                       ("conf", "spark.network.timeout=1200s"), ("conf", "fs.s3.maxRetries=20"),
                                       ("conf", "fs.s3a.attempts.maximum=20"), ("conf", "spark.driver.maxResultSize=" + "20G")],
    eldorado_config_option_pairs_list=[('dateTime', "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"),
                                       ('totalUniquesCount', '20000000000'), ('totalPixelsCount', '7000')]
)

third_party_lal_ratio_cluster = EmrClusterTask(
    name="ThirdPartyLALRatioCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=960
    ),
    cluster_tags={
        **standard_cluster_tags, "Process": "ThirdPartyLAL"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

third_party_lal_relevance_ratio_compute_step = EmrJobTask(
    name="ThirdPartyLALComputeRatio",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.thirdpartylal.ComputeThirdPartyRelevanceScores",
    executable_path=executable_path,
    additional_args_option_pairs_list=[("executor-memory", "102000M"), ("conf", "spark.executor.cores=16"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=59"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=102000M"), ("conf", "spark.driver.cores=16"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000")],
    eldorado_config_option_pairs_list=[('dateTime', "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"),
                                       ("minimumThirdPartyDataUserOverlap", 1), ("minimumRelevanceRatioThreshold", 1.25),
                                       ("binoDesiredAlpha", 0.05), ("highValeOverlapThreshold", 0.0001),
                                       ("minimumUniquesUsersThreshold", 6100)]
)

third_party_lal_validate_and_push_cluster = EmrClusterTask(
    name="ValidateAndPushCluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(48)
        ],
        on_demand_weighted_capacity=128
    ),
    cluster_tags={
        **standard_cluster_tags, "Process": "ValidateAndPushThirdPartyLalData"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=current_emr_version
)

third_party_lal_validate_step = EmrJobTask(
    name="ValidateThirdPartyLal",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.thirdpartylal.ValidateThirdPartyLal",
    executable_path=executable_path,
    additional_args_option_pairs_list=[("executor-memory", "27000M"), ("conf", "spark.executor.cores=4"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=31"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=27000M"), ("conf", "spark.driver.cores=4"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000")],
    eldorado_config_option_pairs_list=[('dateTime', "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"),
                                       ("minimumRelevanceRatioThreshold", 1.25)]
)
push_to_aerospike_step = EmrJobTask(
    name="WriteThirdPartyLalDataToAerospike",
    class_name="com.thetradedesk.etlforecastjobs.audienceinsights.thirdpartylal.WriteThirdPartyLalDataToAerospike",
    executable_path=executable_path,
    additional_args_option_pairs_list=[("executor-memory", "27000M"), ("conf", "spark.executor.cores=4"),
                                       ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.instances=31"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=27000M"), ("conf", "spark.driver.cores=4"),
                                       ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                                       ("conf", "fs.s3.maxRetries=20"), ("conf", "fs.s3a.attempts.maximum=20"),
                                       ("conf", "spark.sql.shuffle.partitions=6000")],
    eldorado_config_option_pairs_list=[('dateTime', "{{data_interval_start.strftime('%Y-%m-%dT%H:00:00')}}"), ('aerospikeSet', '3p'),
                                       ('aerospikeNamespace', 'ttd-insights'), ('aerospikeHosts', aerospike_hosts)]
)

third_party_lal_count_cluster.add_parallel_body_task(third_party_lal_count_step)
third_party_lal_ratio_cluster.add_parallel_body_task(third_party_lal_relevance_ratio_compute_step)
third_party_lal_validate_and_push_cluster.add_parallel_body_task(third_party_lal_validate_step)
third_party_lal_validate_and_push_cluster.add_parallel_body_task(push_to_aerospike_step)

third_party_lal_insights_dag >> third_party_lal_count_cluster
third_party_lal_count_step.watch_job_task >> third_party_lal_ratio_cluster
third_party_lal_relevance_ratio_compute_step.watch_job_task >> third_party_lal_validate_and_push_cluster
third_party_lal_validate_step >> push_to_aerospike_step

dag = third_party_lal_insights_dag.airflow_dag
