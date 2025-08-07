from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [('deviceVendorId', '0'), ('personVendorId', '1'), ('householdVendorId', '9'),
                      ('sketchTargetingDataIdsFileCount', '50')]

compute_sketches_args = [("conf", "spark.network.timeout=3600s"), ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                         ("conf", "spark.sql.shuffle.partitions=4000"), ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                         ("conf", "spark.yarn.max.executor.failures=1000"), ("conf", "spark.default.parallelism=2800"),
                         ("conf", "spark.scheduler.listenerbus.eventqueue.capacity=40000"),
                         ("conf", "spark.yarn.executor.failuresValidityInterval=1h"), ("conf", "spark.driver.maxResultSize=12G")]

additional_application_configurations = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "50",
        "fs.s3.sleepTimeSeconds": "10"
    }
}]

# VARIABLES FOR STORED AND RETRIEVED DATA
sketches_s3 = "s3://thetradedesk-useast-qubole/warehouse/thetradedesk.db/sib-daily-sketches"
targeting_data_staging_s3 = "s3://thetradedesk-useast-qubole/warehouse/thetradedesk.db/sib-daily-sketches-targeting-data-ids-staging/"
namespace = "thetradedesk"
aggregate_sketches_s3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/ttd/seenInBiddingWeekAggregation"
sketches_table = "SeenInBiddingUserSketches"

# default example values of these times, which will be overwritten
generation_str = '1582358400'  # unix time for execution datetime the dag runs
processDate = '2020-02-22'
aggDate = '20200222'  # the day before the dag runs
time_plus_ten = '1582358400'  # unix time for ten days after the dag runs
weekday_int = "0"  # integer representation of weekday, where sunday = 0
siblogs_date_str = "2020/02/22"

endpoint_url = None
dag_name = 'adpb-ae-sketches'
aws_region = "us-east-1"
codebase_url = "s3://ttd-build-artefacts/eldorado/snapshots/master/latest"
codebase_url_hive_jar = "s3://thetradedesk-useast-qubole/udf"

team_ownership = ADPB

emr_counts_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 8, 21, 6, 0),
    schedule_interval='0 6 * * *',
    slack_channel=team_ownership.team.alarm_channel,
    run_only_latest=True
)
dag = emr_counts_dag.airflow_dag


def get_date(**kwargs):
    cur_time = timezone.utcnow()

    execution_date = datetime.now()
    aerospike_processing_date = execution_date
    base_date_str = aerospike_processing_date.strftime('%Y-%m-%d')
    base_date = aerospike_processing_date  # base_date is normally the day before dag run date
    generation_str = str(int(cur_time.replace(tzinfo=timezone.utc).timestamp()))

    process_date = base_date_str
    aggDate = base_date.strftime("%Y%m%d")
    time_plus_ten = str(int((cur_time + timedelta(days=10)).replace(tzinfo=timezone.utc).timestamp()))
    weekday_int = base_date.strftime("%w")
    siblogs_date_str = base_date.strftime("%Y/%m/%d")
    # TODO : change back to without specifc time later
    siblogs_s3 = 's3://thetradedesk-useast-logs-2/seeninbiddingdataexportv2/collected/' + siblogs_date_str + '/*/*/*'

    kwargs['task_instance'].xcom_push(key='generation_str', value=generation_str)
    kwargs['task_instance'].xcom_push(key='process_date', value=process_date)
    kwargs['task_instance'].xcom_push(key='aggDate', value=aggDate)
    kwargs['task_instance'].xcom_push(key='time_plus_ten', value=time_plus_ten)
    kwargs['task_instance'].xcom_push(key='weekday_int', value=weekday_int)
    kwargs['task_instance'].xcom_push(key='siblogs_s3', value=siblogs_s3)


get_date_operator = PythonOperator(
    task_id="get-date",
    python_callable=get_date,
    dag=emr_counts_dag.airflow_dag,
    templates_dict={'start_date': '{{ yesterday_ds }}'},
    provide_context=True
)

elDorado_jar = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

master_instance_types = [M5.m5_2xlarge().with_ebs_size_gb(400).with_fleet_weighted_capacity(1)]
core_instance_types = [
    M5.m5_2xlarge().with_ebs_size_gb(400).with_fleet_weighted_capacity(1),
    M5.m5_4xlarge().with_ebs_size_gb(800).with_fleet_weighted_capacity(2)
]
cluster_compute_name = "emc-counts-to-emr-compute"

cluster_aggregate_master_type = M5.m5_2xlarge().with_ebs_size_gb(400)
cluster_aggregate_worker_type = M5.m5_2xlarge().with_ebs_size_gb(1024)
cluster_aggregate_instance_count = 8
cluster_aggregate_name = "emc-counts-to-emr-aggregate"

compute_sketches_step_class = "jobs.sibsegmentsketches.SibSegmentSketches"

cluster_tags = {
    "Team": team_ownership.team.jira_team,
}

master_fleet_instance_type_configs_compute = EmrFleetInstanceTypes(
    instance_types=master_instance_types,
    on_demand_weighted_capacity=1,
)
core_fleet_instance_type_configs_bid_request_aggregation = EmrFleetInstanceTypes(
    instance_types=core_instance_types,
    on_demand_weighted_capacity=160,
)

aggregate_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=core_instance_types,
    on_demand_weighted_capacity=8,
)

compute_cluster = EmrClusterTask(
    name=cluster_compute_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs_compute,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs_bid_request_aggregation,
    additional_application_configurations=additional_application_configurations,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
    cluster_tags={
        **cluster_tags, "long_running": "true"
    },
    enable_prometheus_monitoring=True,
    use_on_demand_on_timeout=True
)

# step 1 compute sketches
compute_sketches_step = EmrJobTask(
    name="compute-sketches-step",
    class_name=compute_sketches_step_class,
    executable_path=elDorado_jar,
    additional_args_option_pairs_list=[("conf", "spark.scheduler.spark.scheduler.minRegisteredResourcesRatio=0.90"),
                                       ("conf", "spark.scheduler.maxRegisteredResourcesWaitingTime=10m")] + compute_sketches_args,
    eldorado_config_option_pairs_list=java_settings_list +
    [('generation', "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get-date', key='generation_str') }}"),
     ('processingDate', "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get-date', key='process_date') }}"),
     ('calculateAllSegment', 'true'),
     ('sibLogsUrl', "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get-date', key='siblogs_s3') }}"),
     ('sketchesUrl', sketches_s3), ('targetingDataUrl', targeting_data_staging_s3), ('command', 'ComputeSketches')],
    timeout_timedelta=timedelta(hours=24),
    cluster_specs=compute_cluster.cluster_specs,
    configure_cluster_automatically=True,
    command_line_arguments=['--version'],
)

compute_cluster.add_parallel_body_task(compute_sketches_step)

# creates sib sketches external hive table
create_sib_segment_sketches_external_hive_table_step = EmrJobTask(
    class_name=None,
    name="aggregate-cluster-steps",
    job_jar="s3://" + aws_region + ".elasticmapreduce/libs/script-runner/script-runner.jar",
    executable_path=codebase_url + "/shell/jobs/sibsegmentsketches/CreateSibSegmentSketchesExternalHiveTable.sh",
    command_line_arguments=[namespace, sketches_table, sketches_s3]
)

# aggregate data using hive
aggregate_step = EmrJobTask(
    class_name=None,
    name='aggregate-tdid-sketches',
    job_jar="command-runner.jar",
    executable_path="hive-script",
    command_line_arguments=[
        '--run-hive-script', '--args', "-d", "schema=" + namespace, "-d",
        "hiveUdfJarUrl=" + codebase_url_hive_jar + "/uber-thetradedesk-hive-udfs-1.0.100.jar", "-d", "sketchesUrl=" + sketches_s3, "-d",
        "aggregatedSketchesUrl=" + aggregate_sketches_s3, "-d", "aggregatedDate=" + "{{ task_instance.xcom_pull(dag_id='" + dag_name +
        "', task_ids='get-date', key='aggDate') }}", "-d", "generation=" + "{{ task_instance.xcom_pull(dag_id='" + dag_name +
        "', task_ids='get-date', key='generation_str') }}", "-d", "kMinHashMaxSize=4096", "-d", "sketchesTable=" + sketches_table, "-f",
        codebase_url + "/hive/jobs/sibsegmentsketches/AggregateTdidSketches.sql"
    ],
)

sh_job_flow_cluster = EmrClusterTask(
    name=cluster_aggregate_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs_compute,
    core_fleet_instance_type_configs=aggregate_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2_1,  # This needs Hive
    cluster_tags={
        "Source": "Airflow",
        "Environment": TtdEnvFactory.get_from_system().execution_env,
        "Job": "segmentexport",
        "Team": team_ownership.team.jira_team,
        "Resource": "EMR",
        "process": "collect",
    },
    enable_prometheus_monitoring=True,
    use_on_demand_on_timeout=True,
    ec2_subnet_ids=['subnet-f2cb63aa'],
    additional_application=["Hive", "Tez", "Hadoop"]
)

sh_job_flow_cluster.add_sequential_body_task(create_sib_segment_sketches_external_hive_table_step)
sh_job_flow_cluster.add_sequential_body_task(aggregate_step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# Set up dependencies
get_date_operator_optask = OpTask(op=get_date_operator)

emr_counts_dag >> get_date_operator_optask
get_date_operator_optask >> compute_cluster
compute_cluster >> sh_job_flow_cluster >> check
compute_cluster >> check
