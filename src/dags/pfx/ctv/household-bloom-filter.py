from datetime import timedelta, datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

# Configs
dag_name = "household-bloom-filter"
generation_job_name = "generate-household-bloom-filter"
generation_class_name = "jobs.ctv.household.HouseHoldAvailsBloomFilterGeneration"
validation_job_name = "validate-household-bloom-filter"
validation_class_name = "jobs.ctv.household.HouseHoldAvailsBloomFilterValidation"
job_start_date = datetime(2024, 10, 26, 22, 0)
job_schedule_interval = '0 12 * * SAT'
job_jar = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-tv-assembly.jar'

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2_1
cluster_name = "household_bloom_filter_cluster"

# DAG
household_bloom_filter_dag: TtdDag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=1,
    slack_channel=PFX.team.alarm_channel,
    slack_tags=PFX.dev_ctv_forecasting_tool().sub_team,
    tags=["avails", "household", "CTV"],
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/8ceHCQ",
    retries=1,
    retry_delay=timedelta(minutes=30),
)

dag: DAG = household_bloom_filter_dag.airflow_dag

master_fleet_instance_config = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

executor_cores = 512

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        C7g.c7g_8xlarge().with_ebs_size_gb(105).with_max_ondemand_price().with_fleet_weighted_capacity(32),
        C7g.c7g_16xlarge().with_ebs_size_gb(210).with_max_ondemand_price().with_fleet_weighted_capacity(64),
    ],
    on_demand_weighted_capacity=executor_cores
)

hh_bloom_filter_cluster = EmrClusterTask(
    name=cluster_name,
    master_fleet_instance_type_configs=master_fleet_instance_config,
    cluster_tags={"Team": PFX.team.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=emr_release_label,
    use_on_demand_on_timeout=True,
    enable_prometheus_monitoring=True
)

get_graph_dependency = OpTask(
    op=DatasetCheckSensor(
        task_id="ttd_graph_dataset_generated_check",
        datasets=[Datasources.common.ttd_graph],
        ds_date="{{ logical_date.strftime('%Y-%m-%d 00:00:00') }}",
        poke_interval=60 * 10,  # poke every 10 minutes
        timeout=60 * 60 * 4,  # wait 4 hours
    )
)

generate_task = EmrJobTask(
    name=generation_job_name,
    class_name=generation_class_name,
    eldorado_config_option_pairs_list=[
        ("date", "{{ ds }}"),
        ("householdGraphRootPath", Datasources.common.ttd_graph.get_root_path()),
        ("householdBloomFilterRootPath", Datasources.ctv.household_bloom_filter.get_root_path()),

        # UIID Bloom Filter Settings
        ("falsePositivePercentUiid", 1),
        ("hashSeedUiid", 0),
        ("numOfHashesUiid", 10),

        # IP Bloom Filter Settings
        ("falsePositivePercentIp", .001),
        ("hashSeedIp", 0),
        ("numOfHashesIp", 20),
    ],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4),
    additional_args_option_pairs_list=[
        ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
        ("conf", "spark.kryoserializer.buffer.max=2046m"),
        ("conf", "spark.rpc.message.maxSize=2046"),

        # IMPORTANT: The partition count needs to the number of Bloom Filters in the dataset so that each Bloom Filter
        #            is contained within its own file. This is a requirement of Delta which consumes them.
        ("conf", "spark.sql.files.maxRecordsPerFile=1"),
    ],
    configure_cluster_automatically=True
)

validate_task = EmrJobTask(
    name=validation_job_name,
    class_name=validation_class_name,
    eldorado_config_option_pairs_list=[
        ("date", "{{ ds }}"),
        ("householdGraphRootPath", Datasources.common.ttd_graph.get_root_path()),
        ("householdBloomFilterRootPath", Datasources.ctv.household_bloom_filter.get_root_path()),
    ],
    executable_path=job_jar,
    timeout_timedelta=timedelta(hours=4),
    additional_args_option_pairs_list=[
        ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
        ("conf", "spark.kryoserializer.buffer.max=2046m"),
    ],
    configure_cluster_automatically=True
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag, name="final_dag_status", trigger_rule=TriggerRule.ONE_FAILED))

hh_bloom_filter_cluster.add_parallel_body_task(generate_task)
hh_bloom_filter_cluster.add_parallel_body_task(validate_task)

generate_task >> validate_task

household_bloom_filter_dag >> get_graph_dependency >> hh_bloom_filter_cluster >> final_dag_status_step
