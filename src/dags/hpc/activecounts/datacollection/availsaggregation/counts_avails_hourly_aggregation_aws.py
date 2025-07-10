from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.utils import CrossDeviceLevel
from datasources.sources.avails_datasources import AvailsDatasources
from dags.hpc.counts_datasources import CountsDatasources, CountsDataName
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'counts-avails-hourly-aggregation-aws'
cadence_in_hours = 1
max_expected_delay = 3
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2_1

# # Prod Variables
job_start_date = datetime(2024, 12, 11, 1, 0)
schedule = f'0 */{cadence_in_hours} * * *'
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL

# # Test Variables
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/dgs-HPC-6311-filter-to-sib/latest/eldorado-hpc-assembly.jar"

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/AYACG',
    max_active_runs=5,
    depends_on_past=True,
    run_only_latest=False,
    tags=[hpc.jira_team],
)
adag = dag.airflow_dag

master_instance_types = [M6g.m6g_xlarge().with_fleet_weighted_capacity(1), M6g.m6g_2xlarge().with_fleet_weighted_capacity(1)]

core_memory_optimized_instance_types = [
    M6g.m6g_4xlarge().with_fleet_weighted_capacity(4),
    M6g.m6g_8xlarge().with_fleet_weighted_capacity(8),
    M6g.m6g_12xlarge().with_fleet_weighted_capacity(12)
]

###########################################
# Check Dependencies
###########################################

check_avails_upstream_completed = OpTask(
    op=DatasetCheckSensor(
        datasets=[AvailsDatasources.identity_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1")],
        ds_date='{{data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}',
        poke_interval=int(timedelta(minutes=5).total_seconds()),
        timeout=int(timedelta(hours=12).total_seconds()),
        task_id='avails_dataset_check',
        cloud_provider=CloudProviders.aws
    )
)

###########################################
# Steps
###########################################

# Avail Aggregation Cluster

avail_hourly_aggregation_cluster_name = 'counts-avails-hourly-aggregation'

avail_hourly_aggregation_cluster = EmrClusterTask(
    name=avail_hourly_aggregation_cluster_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=master_instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=core_memory_optimized_instance_types, on_demand_weighted_capacity=64),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    use_on_demand_on_timeout=True,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

# Avail Aggregation Step

avail_hourly_aggregation_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.availsaggregation.countsavails.CountsAvailsAggregation'
avail_hourly_aggregation_step_job_name = 'counts-avails-aggregation'

avail_hourly_aggregation_step = EmrJobTask(
    name=avail_hourly_aggregation_step_job_name,
    executable_path=aws_jar,
    class_name=avail_hourly_aggregation_step_spark_class_name,
    eldorado_config_option_pairs_list=[
        ('aerospikeAddress', constants.COLD_STORAGE_ADDRESS),
        ('datetime', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\")}}'),
        # HPC-6003 adjust cadence metric to extend thresholds for the CountsSparkJobDatasetTimestampAgeAlarm
        # dataset age = 5.33 hours < threshold = max_expected_delay * 2 = 3 * 2 = 6 hours
        ('cadenceInHours', max_expected_delay),
        ('crossDeviceLevel', str(CrossDeviceLevel.DEVICE))
    ],
    additional_args_option_pairs_list=[('conf', 'spark.yarn.maxAppAttempts=1')],
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    timeout_timedelta=timedelta(hours=1),
    configure_cluster_automatically=True
)

avail_hourly_aggregation_cluster.add_parallel_body_task(avail_hourly_aggregation_step)

# Dataset Transfer Task

dataset = CountsDatasources.get_counts_dataset(CountsDataName.ACTIVE_IPADDRESS, version=2)
ipaddress_copy_task = DatasetTransferTask(
    name='ipaddress_copy_task',
    dataset=dataset,
    src_cloud_provider=CloudProviders.aws,
    dst_cloud_provider=CloudProviders.azure,
    partitioning_args=dataset.get_partitioning_args(ds_date='{{data_interval_start.strftime(\"%Y-%m-%d %H:00:00\")}}'),
)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> check_avails_upstream_completed >> avail_hourly_aggregation_cluster >> ipaddress_copy_task >> final_dag_check
