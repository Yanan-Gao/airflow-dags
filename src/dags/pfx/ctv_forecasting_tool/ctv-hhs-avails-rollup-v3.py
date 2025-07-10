from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from datasources.sources.ctv_datasources import CtvDatasources

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

job_name = 'ctv-forecasting-tool-hhsampled-avails-rollup-v3'
cluster_name = 'ctv_forecasting_tool_hhsampled_avails_rollup_v3_cluster'
job_jar = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
job_class_rollup = 'com.thetradedesk.etlforecastjobs.preprocessing.hhsampledavails.RollupAggregateHHSampledAvailsV3'
job_class_map = {
    "rollup_ftile_sellers": "com.thetradedesk.etlforecastjobs.preprocessing.hhsampledavails.AvailsRollupV3FtileSellers",
    "rollup_ctvft_sellers": "com.thetradedesk.etlforecastjobs.preprocessing.hhsampledavails.AvailsRollupV3CtvftSellers",
    "rollup_ftile_pmp": "com.thetradedesk.etlforecastjobs.preprocessing.hhsampledavails.AvailsRollupV3FtilePmp",
    "rollup_ctvft_pmp": "com.thetradedesk.etlforecastjobs.preprocessing.hhsampledavails.AvailsRollupV3CtvftPmp"
}
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2
ctv_tag = 'ctv_forecasting_tool'

job_start_date = datetime(2025, 4, 10, 5, 00)
job_schedule_interval = "0 10 * * *"
job_slack_channel = PFX.team.alarm_channel
active_running_jobs = 1

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6gd_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

executor_cores = 17_536
base_ebs_size = 192

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_4xlarge().with_ebs_size_gb(950 + base_ebs_size).with_fleet_weighted_capacity(16),
        R6g.r6g_8xlarge().with_ebs_size_gb(1900 + (2 * base_ebs_size)).with_fleet_weighted_capacity(32),
        R6g.r6g_16xlarge().with_ebs_size_gb(3800 + (4 * base_ebs_size)).with_fleet_weighted_capacity(64),
        R6gd.r6gd_4xlarge().with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(16),
        R6gd.r6gd_8xlarge().with_ebs_size_gb(2 * base_ebs_size).with_fleet_weighted_capacity(32),
        R6gd.r6gd_16xlarge().with_ebs_size_gb(4 * base_ebs_size).with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=executor_cores
)

hhsampled_avails_rollup_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=PFX.dev_ctv_forecasting_tool().sub_team,
    tags=["avails_rollup", ctv_tag],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

dag = hhsampled_avails_rollup_dag.airflow_dag

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

hhsampled_avails_rollup_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_configs,
    cluster_tags={
        "Team": PFX.team.jira_team,
        "SubTeam": ctv_tag
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True,
    additional_application_configurations=[additional_application_configurations]
)

additional_args_options_pairs_list = [
    ("conf", f"spark.sql.shuffle.partitions={executor_cores * 2}"),
    ("conf", "spark.sql.adaptive.enabled=true"),
    ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
    ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
    # Default is 2000, but we need to increase to more than shuffle.partitions for skewJoin.enabled to work
    ("conf", f"spark.shuffle.minNumPartitionsToHighlyCompress={executor_cores * 2 + 1}"),
    ("conf", "spark.sql.autoBroadcastJoinThreshold=2147483648"),  # 2GiB
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.driver.maxResultSize=4G")
]

get_avails_dependency_7d = OpTask(
    op=DatasetCheckSensor(
        task_id="sampled_avails_dataset_generated_check",
        datasets=[CtvDatasources.daily_agg_filtered_avails_v3],
        ds_date="{{ logical_date.strftime('%Y-%m-%d 00:00:00') }}",
        # checks ds_date and then looks back another 6 (so 7 days in total)
        lookback=6,
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 4,  # wait 4 hours
    )
)

step7d = EmrJobTask(
    name="rollup_7d",
    class_name=job_class_rollup,
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("rollupLength", "7")],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=2),
    additional_args_option_pairs_list=additional_args_options_pairs_list,
    maximize_resource_allocation=True
)

rollup_step_ftile_pmp = EmrJobTask(
    name="rollup_ftile_pmp",
    class_name=job_class_map["rollup_ftile_pmp"],
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("inputRollupLength", "7")],
    executable_path=job_jar,
    action_on_failure="CONTINUE",
    timeout_timedelta=timedelta(hours=12),
    additional_args_option_pairs_list=additional_args_options_pairs_list,
    maximize_resource_allocation=True
)

rollup_step_ftile_sellers = EmrJobTask(
    name="rollup_ftile_sellers",
    class_name=job_class_map["rollup_ftile_sellers"],
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("inputRollupLength", "7")],
    executable_path=job_jar,
    action_on_failure="CONTINUE",
    timeout_timedelta=timedelta(hours=12),
    additional_args_option_pairs_list=additional_args_options_pairs_list,
    maximize_resource_allocation=True
)

rollup_step_ctv_pmp = EmrJobTask(
    name="rollup_ctvft_pmp",
    class_name=job_class_map["rollup_ctvft_pmp"],
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("inputRollupLength", "7")],
    executable_path=job_jar,
    action_on_failure="CONTINUE",
    timeout_timedelta=timedelta(hours=12),
    additional_args_option_pairs_list=additional_args_options_pairs_list,
    maximize_resource_allocation=True
)

rollup_step_ctv_sellers = EmrJobTask(
    name="rollup_ctvft_sellers",
    class_name=job_class_map["rollup_ctvft_sellers"],
    eldorado_config_option_pairs_list=[("date", "{{ ds }}"), ("inputRollupLength", "7")],
    executable_path=job_jar,
    action_on_failure="CONTINUE",
    timeout_timedelta=timedelta(hours=12),
    additional_args_option_pairs_list=additional_args_options_pairs_list,
    maximize_resource_allocation=True
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag, name="final_dag_status", trigger_rule=TriggerRule.ONE_FAILED))

hhsampled_avails_rollup_cluster.add_parallel_body_task(step7d)
hhsampled_avails_rollup_cluster.add_parallel_body_task(rollup_step_ftile_sellers)
hhsampled_avails_rollup_cluster.add_parallel_body_task(rollup_step_ftile_pmp)
hhsampled_avails_rollup_cluster.add_parallel_body_task(rollup_step_ctv_pmp)
hhsampled_avails_rollup_cluster.add_parallel_body_task(rollup_step_ctv_sellers)

step7d >> rollup_step_ftile_sellers >> rollup_step_ftile_pmp >> rollup_step_ctv_pmp >> rollup_step_ctv_sellers

hhsampled_avails_rollup_dag >> get_avails_dependency_7d >> hhsampled_avails_rollup_cluster >> final_dag_status_step
