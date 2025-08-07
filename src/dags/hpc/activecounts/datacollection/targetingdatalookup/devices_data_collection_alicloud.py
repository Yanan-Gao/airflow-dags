from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDataName, CountsDatasources
from datasources.sources.sib_datasources import SibDatasources
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

# General Variables
dag_name = 'devices-data-collection-alicloud'
cadence_in_hours = 24

# Prod Variables
job_start_date = datetime(2024, 7, 14, 1, 0)
schedule = '0 21 * * *'
ali_jar = constants.HPC_ALI_EL_DORADO_JAR_URL

# Test Variables
# schedule = None
# ali_jar = 'oss://ttd-build-artefacts/eldorado/mergerequests/kcc-HPC-4368-generate-china-dimension-targeting-data/latest/eldorado-hpc-assembly.jar'

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

devices_data_collection_step_emr_cluster_name = 'counts-devices-data-collection'
devices_data_collection_step_spark_class_name = 'com.thetradedesk.jobs.activecounts.datacollection.targetingdatalookup.DevicesDataCollection'

devices_data_collection_cluster = AliCloudClusterTask(
    name=devices_data_collection_step_emr_cluster_name,
    master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
    )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_2X()).with_node_count(5).with_data_disk_count(1)
    .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
)

# Devices Data Collection Step

devices_data_collection_step_job_name = 'devices-data-collection'

devices_data_collection_step_el_dorado_config_options = [
    ('availProcessingDateHour', avails_partition),
    ('coldStorageReadPartitions', '300'),
    ('aerospikeAddress', constants.CHINA_COLD_STORAGE_ADDRESS),
    ('namespace', constants.CHINA_COLD_STORAGE_NAMESPACE),
    ('redisHost', 'gautam-rate-limiting-redis-test.hoonr9.ng.0001.use1.cache.amazonaws.com'),
    ('redisPort', '6379'),
    ("ttd.ds.default.storageProvider", "alicloud"),
]

devices_data_collection_emr_step = AliCloudJobTask(
    name=devices_data_collection_step_job_name,
    class_name=devices_data_collection_step_spark_class_name,
    jar_path=ali_jar,
    eldorado_config_option_pairs_list=devices_data_collection_step_el_dorado_config_options,
    configure_cluster_automatically=True,
    command_line_arguments=['--version']
)

devices_data_collection_cluster.add_parallel_body_task(devices_data_collection_emr_step)

# Copy the devices data collection dataset from alicloud to s3

avails_partition_formatted = "{{ task_instance.xcom_pull(task_ids='recency_check', key='" + CountsDataName.ACTIVE_TDID_DAID + "').strftime(\"%Y-%m-%d %H:00:00\") }}"


def get_alicloud_to_aws_transfer_task(task_name: str, dataset: HourGeneratedDataset) -> DatasetTransferTask:
    return DatasetTransferTask(
        name=task_name,
        dataset=dataset,
        src_cloud_provider=CloudProviders.ali,
        dst_cloud_provider=CloudProviders.aws,
        partitioning_args=dataset.get_partitioning_args(ds_date=avails_partition_formatted),
        prepare_finalise_timeout=timedelta(minutes=30),
    )


devices_data_collection_transfer_task = get_alicloud_to_aws_transfer_task(
    task_name="devices_data_collection_transfer_task", dataset=SibDatasources.devices_active_counts_data_collection_alicloud
)

user_id_order_data_transfer_task = get_alicloud_to_aws_transfer_task(
    task_name="user_id_order_data_transfer_task", dataset=CountsDatasources.get_counts_dataset(CountsDataName.USER_ID_ORDER_DATA_CHINA)
)

targeting_data_dimensions_transfer_task = get_alicloud_to_aws_transfer_task(
    task_name="targeting_data_dimensions_transfer_task", dataset=CountsDatasources.get_counts_dataset(CountsDataName.TARGETING_DATA_CHINA)
)

meta_data_dimensions_transfer_task = get_alicloud_to_aws_transfer_task(
    task_name="meta_data_dimensions_transfer_task", dataset=CountsDatasources.get_counts_dataset(CountsDataName.META_DATA_CHINA)
)

###########################################
#   Dependencies
###########################################
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

dag >> recency_operator_step >> devices_data_collection_cluster >> devices_data_collection_transfer_task >> user_id_order_data_transfer_task >> targeting_data_dimensions_transfer_task >> meta_data_dimensions_transfer_task >> final_dag_check
