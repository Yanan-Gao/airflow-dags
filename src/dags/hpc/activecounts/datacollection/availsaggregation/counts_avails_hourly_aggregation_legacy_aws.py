from datetime import datetime, timedelta

import dags.hpc.constants as constants
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r6a import R6a
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'counts-avails-hourly-aggregation-legacy-aws'
cadence_in_hours = 1
max_expected_delay = 3

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4

# Prod Variables
start_date = datetime(2024, 12, 11, 0)
schedule = f'0 */{cadence_in_hours} * * *'
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/sjh-HPC-4283-move-data-collection-spark3/latest/eldorado-hpc-assembly.jar"

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/P4ABG',
    max_active_runs=5,
    depends_on_past=True,
    run_only_latest=False,
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag

master_instance_types = [
    M7g.m7g_xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(20),
    M7g.m7g_2xlarge().with_fleet_weighted_capacity(1).with_ebs_size_gb(20)
]

core_memory_optimized_instance_types = [
    R6a.r6a_4xlarge().with_fleet_weighted_capacity(4),
    R6a.r6a_8xlarge().with_fleet_weighted_capacity(8),
    R6a.r6a_12xlarge().with_fleet_weighted_capacity(12)
]

###########################################
# Check Dependencies
###########################################

check_avails_upstream_completed = OpTask(
    op=DatasetCheckSensor(
        datasets=[Datasources.avails.avails_7_day.with_check_type(check_type="hour")],
        ds_date='{{data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}',
        poke_interval=int(timedelta(minutes=5).total_seconds()),
        timeout=int(timedelta(hours=12).total_seconds()),
        task_id='avails_dataset_check',
        cloud_provider=CloudProviders.aws,
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
    EmrFleetInstanceTypes(instance_types=core_memory_optimized_instance_types, on_demand_weighted_capacity=48),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    use_on_demand_on_timeout=True,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

# Avail Aggregation Step

avail_hourly_aggregation_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.availsaggregation.usersampledavailaggregation.UserSampledAvailsHourlyAggregation'
avail_hourly_aggregation_step_job_name = 'avails-hourly-aggregation'

avail_hourly_aggregation_step = EmrJobTask(
    name=avail_hourly_aggregation_step_job_name,
    executable_path=aws_jar,
    class_name=avail_hourly_aggregation_step_spark_class_name,
    eldorado_config_option_pairs_list=[('datetime', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'),
                                       ('cadenceInHours', max_expected_delay)],
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    timeout_timedelta=timedelta(hours=1),
    configure_cluster_automatically=True
)

avail_hourly_aggregation_cluster.add_parallel_body_task(avail_hourly_aggregation_step)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> check_avails_upstream_completed >> avail_hourly_aggregation_cluster >> final_dag_check
