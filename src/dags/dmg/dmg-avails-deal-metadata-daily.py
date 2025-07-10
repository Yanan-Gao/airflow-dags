from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from datetime import datetime, timedelta
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack import slack_groups

ResultsRoot = "s3://ttd-identity/datapipeline/prod/markets/availsmetadata/"
AvailsRoot = "s3://thetradedesk-useast-logs-2/avails/cleansed/"

date_macro = "{{ (dag_run.start_date + macros.timedelta(days=-1)).strftime(\"%Y-%m-%d\") }}"
job_jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/el-dorado-assembly.jar"

job_slack_channel = "#scrum-dmg-alarms"

java_options_list = [("resultsRoot", ResultsRoot), ("availsRoot", AvailsRoot), ("date", date_macro)]

spark_options_list = [('conf', 'spark.dynamicAllocation.enabled=true'), ("conf", "spark.speculation=false"),
                      ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                      ("conf", "spark.sql.files.ignoreCorruptFiles=true"), ("conf", "spark.sql.adaptive.enabled=true"),
                      ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"), ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
                      ("conf", "maximizeResourceAllocation=true")]

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[C5.c5_9xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6g_4xlarge().with_ebs_size_gb(192).with_max_ondemand_price().with_fleet_weighted_capacity(4),
        M6g.m6g_8xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(8),
        M6g.m6g_12xlarge().with_ebs_size_gb(384).with_max_ondemand_price().with_fleet_weighted_capacity(12),
        M6g.m6g_16xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(16)
    ],
    on_demand_weighted_capacity=160
)

dag = TtdDag(
    dag_id="avails-deal-metadata-daily",
    start_date=datetime(2024, 3, 28, 1, 30) - timedelta(days=1),
    schedule_interval="30 1 * * *",
    slack_channel=job_slack_channel,
    tags=[slack_groups.DEAL_MANAGEMENT.team.jira_team],
    run_only_latest=True
)

adag = dag.airflow_dag
cluster_task = EmrClusterTask(
    name="AvailsDealMetadataDaily_cluster",
    cluster_tags={"Team": slack_groups.DEAL_MANAGEMENT.team.jira_team},
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name="AvailsDealMetadata_step",
    class_name="jobs.markets.AvailsDealMetadata",
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=8),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_options_list,
    configure_cluster_automatically=True
)

cluster_task.add_parallel_body_task(job_task)
dag >> cluster_task
