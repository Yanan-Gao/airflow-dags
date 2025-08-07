from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.utils import CrossDeviceLevel
from dags.hpc.counts_datasources import CountsDatasources, CountsDataName
from datasources.sources.xdgraph_datasources import XdGraphDatasources, XdGraphVendorDataName
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.ec2.emr_instance_types.memory_optimized.r8g import R8g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'uid2-booster'
cadence_in_hours = 12
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4
job_start_date = datetime(2025, 1, 10, 1, 0)
counts_data_name = f"{CountsDataName.COUNTS_AVAILS_AGGREGATION}/{str(CrossDeviceLevel.DEVICE)}"

# Prod Variables
schedule = f'0 */{cadence_in_hours} * * *'
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL
counts_avails_aggregation_partition = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + counts_data_name + "').strftime('%Y-%m-%dT%H:00:00') }}"

# Test Variables
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-5508-uid2-booster-query-cold-storage/latest/eldorado-hpc-assembly.jar"
# counts_avails_aggregation_partition = '2025-01-08T11:00:00'

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/o4ABG',
    tags=[hpc.jira_team],
)
adag = dag.airflow_dag

###########################################
# Check Dependencies
###########################################
recency_operator_step = OpTask(
    op=DatasetRecencyOperator(
        dag=adag,
        datasets_input=[
            CountsDatasources.get_counts_dataset(counts_data_name),
            XdGraphDatasources.xdGraph(XdGraphVendorDataName.IAv2_Person)
        ],
        cloud_provider=CloudProviders.aws,
        recency_start_date=datetime.today(),
        lookback_days=12,
        xcom_push=True
    )
)

xd_graph_partition = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + XdGraphVendorDataName.IAv2_Person + "').strftime('%Y-%m-%d') }}"

###########################################
# Steps
###########################################

master_instance_types = [C7g.c7g_xlarge().with_fleet_weighted_capacity(1), C7g.c7g_2xlarge().with_fleet_weighted_capacity(1)]

core_instance_types = [
    R8g.r8g_16xlarge().with_fleet_weighted_capacity(16),
    R8g.r8g_24xlarge().with_fleet_weighted_capacity(24),
    R8g.r8g_48xlarge().with_fleet_weighted_capacity(48),
]

eldorado_config_option_pairs_list = [('datetime', counts_avails_aggregation_partition), ('cadenceInHours', cadence_in_hours),
                                     ('aerospikeAddress', constants.COLD_STORAGE_ADDRESS), ('xdGraphDate', xd_graph_partition)]
additional_args_option_pairs_list = [('conf', 'spark.network.timeout=3600s'), ('conf', 'spark.yarn.max.executor.failures=1000'),
                                     ('conf', 'spark.yarn.executor.failuresValidityInterval=1h'), ('conf', 'spark.yarn.maxAppAttempts=1'),
                                     ('conf', 'spark.shuffle.consolidateFiles=true')]

# Uid2 Booster Step
uid2_booster_task_name = 'uid2-booster'
uid2_booster_class_name = 'com.thetradedesk.jobs.activecounts.countsredesign.uid2booster.Uid2Booster'

uid2_booster_cluster = EmrClusterTask(
    name=uid2_booster_task_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=core_instance_types,
        on_demand_weighted_capacity=72,
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    region_name="us-east-1",
    retries=0
)

uid2_booster_task = EmrJobTask(
    name=uid2_booster_task_name,
    executable_path=aws_jar,
    class_name=uid2_booster_class_name,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
    timeout_timedelta=timedelta(hours=24),
    configure_cluster_automatically=True,
)

uid2_booster_cluster.add_sequential_body_task(uid2_booster_task)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> recency_operator_step >> uid2_booster_cluster >> final_dag_check
