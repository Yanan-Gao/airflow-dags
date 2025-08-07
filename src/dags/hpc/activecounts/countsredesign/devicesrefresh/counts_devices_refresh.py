from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDatasources, CountsDataName
from dags.hpc.utils import CrossDeviceLevel, Source, DataType
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r8g import R8g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'counts-devices-refresh'
cadence_in_hours = 6
max_expected_delay = 12
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4
job_start_date = datetime(2024, 12, 2, 1, 0)
counts_data_name = f"{CountsDataName.COUNTS_AVAILS_AGGREGATION}/{str(CrossDeviceLevel.DEVICE)}"

# Prod Variables
schedule = f'0 */{cadence_in_hours} * * *'
aws_jar = constants.HPC_AWS_EL_DORADO_JAR_URL
cardinality_service_host = constants.CARDINALITY_SERVICE_PROD_HOST
granite_hosts = constants.GRANITE_PROD_HOSTS
counts_avails_aggregation_partition = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + counts_data_name + "').strftime('%Y-%m-%dT%H:00:00') }}"

# Test Variables
# schedule = None
# aws_jar = "s3://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-7181-order-devices-global-active-id-space-test/latest/eldorado-hpc-assembly.jar"
# cardinality_service_host = constants.CARDINALITY_SERVICE_TEST_HOST
# granite_hosts = constants.GRANITE_CANARY_HOSTS
# counts_avails_aggregation_partition = '2025-02-13T03:00:00'

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=schedule,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/gYABG',
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
        ],
        cloud_provider=CloudProviders.aws,
        recency_start_date=datetime.today(),
        lookback_days=1,
        xcom_push=True
    )
)

###########################################
# Steps
###########################################

master_instance_types = [M7g.m7g_xlarge().with_fleet_weighted_capacity(1), M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)]

core_instance_types = [
    M7g.m7g_4xlarge().with_fleet_weighted_capacity(4),
    M7g.m7g_8xlarge().with_fleet_weighted_capacity(8),
    M7g.m7g_12xlarge().with_fleet_weighted_capacity(12)
]

eldorado_config_option_pairs_list = [
    ('datetime', counts_avails_aggregation_partition),
    # HPC-6003 adjust cadence metric to extend thresholds for the CountsSparkJobDatasetTimestampAgeAlarm
    # dataset age = 8.33 hours < threshold = max_expected_delay * 2 = 6 * 2 = 12 hours
    ('cadenceInHours', max_expected_delay),
    ('cardinalityServiceHost', cardinality_service_host),
    ('cardinalityServicePort', constants.CARDINALITY_SERVICE_PROD_PORT),
    ('crossDeviceLevel', str(CrossDeviceLevel.DEVICE)),
    ('storageProvider', str(CloudProviders.aws)),
    ('createHashedDevicesDataset', 'true')
]
additional_args_option_pairs_list = [('conf', 'spark.network.timeout=3600s'), ('conf', 'spark.yarn.max.executor.failures=1000'),
                                     ('conf', 'spark.yarn.executor.failuresValidityInterval=1h'), ('conf', 'spark.yarn.maxAppAttempts=1'),
                                     ('conf', 'spark.shuffle.consolidateFiles=true')]

# Active Ids Aggregation Step
active_ids_aggregation_task_name = 'active-ids-aggregation'
active_ids_aggregation_class_name = 'com.thetradedesk.jobs.activecounts.countsredesign.activeidsaggregation.ActiveIdsAggregation'

active_ids_aggregation_cluster = EmrClusterTask(
    name=active_ids_aggregation_task_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=core_instance_types,
        on_demand_weighted_capacity=36,
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    custom_java_version=17,
    region_name="us-east-1",
    retries=0
)

active_ids_aggregation_task = EmrJobTask(
    name=active_ids_aggregation_task_name,
    executable_path=aws_jar,
    class_name=active_ids_aggregation_class_name,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + [('ordering', 'GeoThenIdType')],
    additional_args_option_pairs_list=additional_args_option_pairs_list,
    timeout_timedelta=timedelta(hours=1),
    configure_cluster_automatically=True,
)

active_ids_aggregation_cluster.add_sequential_body_task(active_ids_aggregation_task)

# Devices Refresh Step
devices_refresh_task_name = 'devices-refresh'
devices_refresh_class_name = 'com.thetradedesk.jobs.activecounts.countsredesign.devicesrefresh.DevicesRefresh'

devices_refresh_cluster = EmrClusterTask(
    name=devices_refresh_task_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=core_instance_types,
        on_demand_weighted_capacity=4,
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    custom_java_version=17,
    region_name="us-east-1",
    retries=0
)

devices_refresh_task = EmrJobTask(
    name=devices_refresh_task_name,
    executable_path=aws_jar,
    class_name=devices_refresh_class_name,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + [('graniteHosts', granite_hosts), ("innerParallelism", 20)],
    additional_args_option_pairs_list=additional_args_option_pairs_list,
    timeout_timedelta=timedelta(hours=12),
    configure_cluster_automatically=True,
)

devices_refresh_cluster.add_sequential_body_task(devices_refresh_task)

# Devices Dimension Refresh Step
devices_dimension_refresh_cluster_name = 'devices-dimension-refresh'
dimension_refresh_class_name = 'com.thetradedesk.jobs.activecounts.countsredesign.dimensionrefresh.DimensionRefresh'

devices_dimension_refresh_core_instance_types = [
    R8g.r8g_4xlarge().with_fleet_weighted_capacity(4),
    R8g.r8g_8xlarge().with_fleet_weighted_capacity(8),
]

devices_dimension_refresh_cluster = EmrClusterTask(
    name=devices_dimension_refresh_cluster_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=devices_dimension_refresh_core_instance_types,
        on_demand_weighted_capacity=8,
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    custom_java_version=17,
    region_name="us-east-1",
    retries=0
)


def add_devices_dimension_refresh_task(task_name, eldorado_config_options, cluster_calc_defaults=ClusterCalcDefaults()):

    devices_dimension_refresh_task = EmrJobTask(
        name=task_name,
        executable_path=aws_jar,
        class_name=dimension_refresh_class_name,
        eldorado_config_option_pairs_list=eldorado_config_option_pairs_list + eldorado_config_options,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
        timeout_timedelta=timedelta(hours=1),
        configure_cluster_automatically=True,
        cluster_calc_defaults=cluster_calc_defaults,
    )

    devices_dimension_refresh_cluster.add_parallel_body_task(devices_dimension_refresh_task)


add_devices_dimension_refresh_task(
    'main-metadata', [('source', str(Source.MAIN)), ('dataType', str(DataType.METADATA)),
                      ('ttd.DimensionTargetingDataDataSet.isInChain', 'true')],
    ClusterCalcDefaults(min_executor_memory=113, max_cores_executor=15)
)
add_devices_dimension_refresh_task('ipaws-targetingdata', [('source', str(Source.IPAWS)), ('dataType', str(DataType.TARGETINGDATA))])
add_devices_dimension_refresh_task('china-targetingdata', [('source', str(Source.CHINA)), ('dataType', str(DataType.TARGETINGDATA))])

# Rotate Generation Ring Step
rotate_generation_ring_task_name = 'rotate-generation-ring'
rotate_generation_ring_class_name = 'com.thetradedesk.jobs.activecounts.countsredesign.cardinalityservice.RotateGenerationRing'

rotate_generation_ring_cluster = EmrClusterTask(
    name=rotate_generation_ring_task_name,
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=master_instance_types,
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
    custom_java_version=17,
    region_name="us-east-1",
    retries=0
)

rotate_generation_ring_task = EmrJobTask(
    name=rotate_generation_ring_task_name,
    executable_path=aws_jar,
    class_name=rotate_generation_ring_class_name,
    eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
    additional_args_option_pairs_list=additional_args_option_pairs_list,
    timeout_timedelta=timedelta(hours=1),
)

rotate_generation_ring_cluster.add_sequential_body_task(rotate_generation_ring_task)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> recency_operator_step >> active_ids_aggregation_cluster >> devices_refresh_cluster >> rotate_generation_ring_cluster >> final_dag_check
active_ids_aggregation_cluster >> devices_dimension_refresh_cluster >> rotate_generation_ring_cluster
rotate_generation_ring_cluster >> final_dag_check
