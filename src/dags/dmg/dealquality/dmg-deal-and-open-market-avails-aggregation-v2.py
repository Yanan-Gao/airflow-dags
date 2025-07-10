from datetime import datetime, timedelta

from dags.dmg.constants import DEAL_QUALITY_ALARMS_CHANNEL, DEAL_QUALITY_JAR
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.cluster_params import Defaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import DEAL_MANAGEMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from datasources.sources.avails_datasources import AvailsDatasources
from dags.dmg.utils import get_jar_file_path

job_name = "dmg-deal-and-open-market-avails-aggregation-v2"
start_date = datetime(2025, 6, 11, 0, 0)
env = TtdEnvFactory.get_from_system()

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=timedelta(days=1) if env == TtdEnvFactory.prod else None,
    retries=1,
    max_active_runs=2,
    slack_channel=DEAL_QUALITY_ALARMS_CHANNEL,
    enable_slack_alert=(env == TtdEnvFactory.prod),
    tags=[DEAL_MANAGEMENT.team.name, "DealQuality"],
    run_only_latest=None
)

dag = ttd_dag.airflow_dag

check_avails_exists = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        ds_date="{{ logical_date.to_datetime_string() }}",
        poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
        datasets=[AvailsDatasources.deal_set_agg_hourly_dataset.with_check_type("day").with_region("us-east-1")],
        timeout=60 * 60 * 12  # 12 hours
    )
)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_16xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7g_8xlarge().with_fleet_weighted_capacity(32).with_ebs_size_gb(2048).with_ebs_iops(Defaults.MAX_GP3_IOPS)
        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
        M7g.m7g_12xlarge().with_fleet_weighted_capacity(48).with_ebs_size_gb(3072).with_ebs_iops(Defaults.MAX_GP3_IOPS)
        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
        M7g.m7g_16xlarge().with_fleet_weighted_capacity(64).with_ebs_size_gb(4096).with_ebs_iops(Defaults.MAX_GP3_IOPS)
        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
    ],
    on_demand_weighted_capacity=M7g.m7g_16xlarge().cores * 196
)

cluster_task = EmrClusterTask(
    name=job_name + "-cluster",
    retries=1,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": DEAL_MANAGEMENT.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5,
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }]
)

job_task = EmrJobTask(
    name=job_name + "-job-task",
    retries=1,
    class_name="com.thetradedesk.deals.pipelines.dealquality.dealandopenmarketavails.DealAndOpenMarketAvailsJob",
    executable_path=get_jar_file_path(DEAL_QUALITY_JAR),
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=64g"),
        ("conf", "spark.sql.shuffle.partitions=20000"),
        ("conf", "spark.dynamicAllocation.enabled=true"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "spark.yarn.maxAppAttempts=1"),  # if a retry is attempted we will run out of HDFS space
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[("runStartInclusive", "{{ data_interval_start.strftime(\"%Y-%m-%dT00:00:00\") }}"),
                                       ("runEndExclusive", "{{ data_interval_start.add(days=1).strftime(\"%Y-%m-%dT00:00:00\") }}"),
                                       ('outputPartitionCount', '200')],
    timeout_timedelta=timedelta(hours=6)
)

cluster_task.add_parallel_body_task(job_task)

ttd_dag >> check_avails_exists >> cluster_task
