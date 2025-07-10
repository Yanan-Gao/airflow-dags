from datetime import timedelta, datetime

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.cluster_params import Defaults
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE, CHGROW, mqe
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

# Prod variables
job_name = 'global100-metrics-generation'
cluster_name = "invmkt_global100_metrics"
invmkt = INVENTORY_MARKETPLACE().team
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5
job_schedule_interval = timedelta(days=30)
job_start_date = datetime(2025, 3, 4, 1)
job_environment = TtdEnvFactory.get_from_system()
max_active_runs = 1

# Jar variables
jar_path = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-invmkt-assembly.jar'
distinct_hh_id_metrics_class_name = 'jobs.global100.IdentityAndDealAggDistinctMetrics'
deal_set_agg_metrics_class_name = 'jobs.global100.DealSetAggMetrics'
identity_deal_agg_impression_counts_class_name = 'jobs.global100.IdentityAndDealAggregatesAvailsMetrics'
global100_final_data_aggregation = 'jobs.global100.Global100FinalDataAggregation'

# Date and Partition configs
run_date = "{{ dag_run.logical_date.strftime(\"%Y-%m-%d\") }}"

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=max_active_runs,
    slack_channel=invmkt.alarm_channel,
    retries=1,
    retry_delay=timedelta(minutes=5),
    tags=[invmkt.jira_team, 'global100-metrics'],
    teams_allowed_to_access=[invmkt.jira_team, mqe.jira_team, CHGROW().team.jira_team],
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": invmkt.jira_team,
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        "start_date": job_start_date,
    }
)

adag = dag.airflow_dag

####################################################################################################################
# cluster
####################################################################################################################

master_instance_type = R6gd.r6gd_8xlarge()

instance_types = [
    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32).with_ebs_size_gb(2048)
    .with_ebs_iops(Defaults.MAX_GP3_IOPS).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC),
]

on_demand_weighted_capacity = 64 * 64

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[master_instance_type.with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=on_demand_weighted_capacity
)

global100_cluster_task = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": invmkt.jira_team},
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    emr_release_label=emr_release_label,
    environment=job_environment,
    cluster_auto_termination_idle_timeout_seconds=5 * 60,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }]
)

####################################################################################################################
# steps
####################################################################################################################

config_list = [('date', run_date), ('datasetGenerationType', 'MetadataGeneration')]
config_list_identity_deal = [('processedDate', run_date), ('daysToProcess', "7")]

distinct_hh_id_metrics_job_step = EmrJobTask(
    name="distinct_hh_id_metrics_generation_step",
    class_name=distinct_hh_id_metrics_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=192g"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "maximizeResourceAllocation=true"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=8),
    cluster_specs=global100_cluster_task.cluster_specs
)

global100_cluster_task.add_sequential_body_task(distinct_hh_id_metrics_job_step)

deal_set_agg_metrics_job_step = EmrJobTask(
    name="deal_set_agg_metrics_generation_step",
    class_name=deal_set_agg_metrics_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=192g"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "maximizeResourceAllocation=true"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=8),
    cluster_specs=global100_cluster_task.cluster_specs
)

global100_cluster_task.add_sequential_body_task(deal_set_agg_metrics_job_step)

identity_deal_agg_impression_counts_job_step = EmrJobTask(
    name="identity_deal_agg_impression_counts_job_step",
    class_name=identity_deal_agg_impression_counts_class_name,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=192g"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "maximizeResourceAllocation=true"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=8),
    cluster_specs=global100_cluster_task.cluster_specs
)

global100_cluster_task.add_sequential_body_task(identity_deal_agg_impression_counts_job_step)

global100_final_data_aggregation_job_step = EmrJobTask(
    name="global100_final_data_aggregation_job_step",
    class_name=global100_final_data_aggregation,
    executable_path=jar_path,
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
        ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ("conf", "spark.driver.maxResultSize=192g"),
        ("conf", "spark.sql.shuffle.partitions=6000"),
        ("conf", "spark.sql.adaptive.enabled=true"),
        ("conf", "spark.sql.adaptive.coalescePartitions.enabled=true"),
        ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
        ("conf", "maximizeResourceAllocation=true"),
    ],
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=config_list,
    timeout_timedelta=timedelta(hours=8),
    cluster_specs=global100_cluster_task.cluster_specs
)

global100_cluster_task.add_sequential_body_task(global100_final_data_aggregation_job_step)

wait_complete = DatasetCheckSensor(
    dag=dag.airflow_dag,
    ds_date="{{ logical_date.to_datetime_string() }}",
    poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler
    datasets=[AvailsDatasources.deal_set_agg_hourly_dataset.with_check_type("day").with_region("us-east-1")],
    timeout=60 * 60 * 8
)

###########################################
#   Dependencies
###########################################
final_dag_check = FinalDagStatusCheckOperator(dag=adag)
dag >> global100_cluster_task
wait_complete >> global100_cluster_task.first_airflow_op()
