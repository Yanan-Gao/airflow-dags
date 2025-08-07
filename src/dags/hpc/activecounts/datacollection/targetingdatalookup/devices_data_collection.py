from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDatasources, CountsDataName
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r6a import R6a
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'devices-data-collection'
cadence_in_hours = 24

emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4

# Prod Variables
job_start_date = datetime(2024, 7, 8, 1, 0)
schedule = '0 21 * * *'
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/sjh-HPC-4907-move-personhh-data-collection/latest/eldorado-hpc-assembly.jar"

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/P4ABG',
    max_active_runs=1,
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

recency_operator_step = OpTask(
    op=DatasetRecencyOperator(
        dag=adag,
        datasets_input=[CountsDatasources.get_counts_dataset(CountsDataName.ACTIVE_TDID_DAID)],
        cloud_provider=CloudProviders.aws,
        recency_start_date=datetime.today(),
        lookback_days=1,
        xcom_push=True
    )
)

avails_partition = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + CountsDataName.ACTIVE_TDID_DAID + "').strftime('%Y-%m-%dT%H:00:00') }}"

###########################################
# Steps
###########################################

# Devices Data Collection Cluster

devices_data_collection_cluster_name = 'counts-devices-data-collection'

devices_data_collection_cluster = EmrClusterTask(
    name=devices_data_collection_cluster_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=master_instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=core_memory_optimized_instance_types, on_demand_weighted_capacity=300),
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

# Devices Data Collection Step

devices_data_collection_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.targetingdatalookup.DevicesDataCollection'
devices_data_collection_step_job_name = 'devices-data-collection'

devices_data_collection_step_el_dorado_config_options = [
    ('availProcessingDateHour', avails_partition), ('aerospikeAddress', constants.COLD_STORAGE_ADDRESS),
    ('redisHost', 'gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com'), ('redisPort', '6379'),
    ('processingDateHour', '{{data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'), ('runCadence', 'slow')
]

devices_data_collection_step = EmrJobTask(
    name=devices_data_collection_step_job_name,
    executable_path=aws_jar,
    class_name=devices_data_collection_step_spark_class_name,
    eldorado_config_option_pairs_list=devices_data_collection_step_el_dorado_config_options,
    cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=4),
    timeout_timedelta=timedelta(hours=6),
    configure_cluster_automatically=True
)

devices_data_collection_cluster.add_parallel_body_task(devices_data_collection_step)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> recency_operator_step >> devices_data_collection_cluster >> final_dag_check
