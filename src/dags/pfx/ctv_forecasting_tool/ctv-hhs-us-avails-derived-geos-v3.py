from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from datasources.sources.avails_datasources import AvailsDatasources
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

job_name = 'ctv-forecasting-us-avails-derived-geo-v3'
cluster_name = 'ctv_forecasting_tool_us_avails_derived_geo_v3_cluster'
job_jar = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
job_class = 'com.thetradedesk.etlforecastjobs.preprocessing.geo.USAvailsDerivedHouseholdGeoV3'
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2
ctv_tag = 'ctv_forecasting_tool'

job_start_date = datetime(2025, 4, 27, 6, 00)
job_schedule_interval = timedelta(days=7)
job_slack_channel = PFX.team.alarm_channel
active_running_jobs = 1

master_fleet_instance_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6gd_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

executor_cores = 5120

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R6g.r6g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(16),
        R6g.r6g_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(32),
        R6g.r6g_16xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(64),
        R6gd.r6gd_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(16),
        R6gd.r6gd_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(32),
        R6gd.r6gd_16xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=executor_cores
)

us_avails_derived_geo_dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    slack_tags=PFX.dev_ctv_forecasting_tool().sub_team,
    tags=["us-avails-derived-geo-v3", ctv_tag],
    enable_slack_alert=True,
    slack_alert_only_for_prod=True,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

additional_application_configurations = {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    },
}

us_avails_derived_geo_cluster = EmrClusterTask(
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

additional_args_option_pairs_list = [
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("conf", "spark.databricks.delta.retentionDurationCheck.enabled=false"),
    ("conf", "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled=true"),
    ("conf", "spark.databricks.delta.vacuum.parallelDelete.enabled=true"),
]

step = EmrJobTask(
    name=job_name,
    class_name=job_class,
    # The interval end date formatted like yyyy-MM-dd
    eldorado_config_option_pairs_list=[("date", "{{ data_interval_end.subtract(days=1) | ds }}")],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=6),
    additional_args_option_pairs_list=[
        ("conf", f"spark.sql.shuffle.partitions={executor_cores * 2}"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        # Default is 2000, but we need to increase to more than shuffle.partitions for skewJoin.enabled to work
        ("conf", f"spark.shuffle.minNumPartitionsToHighlyCompress={executor_cores * 2 + 1}"),
        ("conf", "spark.sql.autoBroadcastJoinThreshold=2147483648"),  # 2GiB
    ] + additional_args_option_pairs_list,
)

us_avails_derived_geo_cluster.add_parallel_body_task(step)

dag = us_avails_derived_geo_dag.airflow_dag

get_avails_dependency = OpTask(
    op=DatasetCheckSensor(
        task_id="sampled_avails_dataset_generated_check",
        datasets=[AvailsDatasources.household_sampled_high_sample_avails_v2],
        ds_date="{{ data_interval_end.subtract(days=1).strftime('%Y-%m-%d 00:00:00') }}",
        lookback=6,  # we want to check for 7 days of data, but we have subtracted one day already and lookback is inclusive of ds_date
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 2,  # wait 2 hours
    )
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag, name="final_dag_status", trigger_rule=TriggerRule.ONE_FAILED))

us_avails_derived_geo_dag >> get_avails_dependency >> us_avails_derived_geo_cluster >> final_dag_status_step
