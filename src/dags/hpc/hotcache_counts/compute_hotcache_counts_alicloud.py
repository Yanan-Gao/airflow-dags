from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

import dags.hpc.constants as constants
import dags.hpc.utils as hpc_utils
from dags.hpc.counts_datasources import CountsDatasources
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.eldorado.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

dag_name = 'compute-hotcache-counts-alicloud'
cadence_in_hours = 24
schedule_interval = timedelta(hours=cadence_in_hours)
dagrun_timeout = timedelta(hours=12)
eldorado_jar_path = constants.HPC_ALI_EL_DORADO_JAR_URL

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 9, 18),
    schedule_interval=schedule_interval,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/MgACG',
    tags=[hpc.jira_team],
    max_active_runs=1,
    dagrun_timeout=dagrun_timeout,
    run_only_latest=True
)
adag = dag.airflow_dag

###########################################
# Compute HotCache Counts
###########################################
partition_factor = "8"
sample_mod = 100

datacenter_ip_mapping = {
    "cn4": constants.CN4_HC_ADDRESS,
}

compute_hotcache_counts_master_instance_type = ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(1)
compute_hotcache_counts_core_instance_type = ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2)

compute_hotcache_counts_class_name = 'com.thetradedesk.jobs.hotcachecounts.ComputeHotCacheCounts'
current_date = datetime.today().strftime('%Y%m%d')

hotcache_counts_write_prefix_with_date = f'{constants.HOTCACHE_COUNTS_CHINA_WRITE_PATH_PREFIX}/date={current_date}/'
hotcache_counts_full_write_path = f'oss://{constants.OSS_COUNTS_BUCKET}/{hotcache_counts_write_prefix_with_date}'

compute_hotcache_counts_job_el_dorado_config_options = [("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                                        ('cadenceInHours', cadence_in_hours), ('partitionFactor', partition_factor),
                                                        ('parquetStorageLoc', hotcache_counts_full_write_path), ('sampleMod', sample_mod),
                                                        ('recordsPerSecond', 10000)]

final_dag_checker = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Creating cluster and task for each DC and running them in parallel
for dc, seed_address in datacenter_ip_mapping.items():
    compute_hotcache_counts_cluster = AliCloudClusterTask(
        name=f'compute-hotcache-counts-cluster-{dc}',
        master_instance_type=compute_hotcache_counts_master_instance_type,
        core_instance_type=compute_hotcache_counts_core_instance_type,
        cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
        retries=1,
        retry_delay=timedelta(minutes=10)
    )

    compute_hotcache_counts_task = AliCloudJobTask(
        name=f'compute-hotcache-counts-job-{dc}',
        class_name=compute_hotcache_counts_class_name,
        cluster_spec=compute_hotcache_counts_cluster.cluster_specs,
        eldorado_config_option_pairs_list=compute_hotcache_counts_job_el_dorado_config_options + [
            ('datacenter', dc),
            ('address', seed_address),
        ],
        configure_cluster_automatically=True,
        command_line_arguments=['--version'],
        jar_path=eldorado_jar_path
    )

    compute_hotcache_counts_cluster.add_parallel_body_task(compute_hotcache_counts_task)

    # this is needed because alicloud spark jobs write some content into the success files, which is currently breaking the vertica load
    write_empty_success_file_task = OpTask(
        op=PythonOperator(
            task_id="write-empty-success-file-task",
            python_callable=hpc_utils.write_empty_success_file,
            op_kwargs={
                "cloud_provider": CloudProviders.ali,
                "bucket_name": constants.OSS_COUNTS_BUCKET,
                "folder_prefix": hotcache_counts_write_prefix_with_date
            }
        )
    )

    copy_hotcache_counts_from_oss_to_s3_task = DatasetTransferTask(
        name=f"copy-hotcache-counts-from-oss-to-s3-{dc}",
        dataset=CountsDatasources.hotcache_counts,
        src_cloud_provider=CloudProviders.ali,
        dst_cloud_provider=CloudProviders.aws,
        partitioning_args=CountsDatasources.hotcache_counts.get_partitioning_args(
            ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d\") }}'
        )
    )

    dag >> compute_hotcache_counts_cluster >> write_empty_success_file_task >> copy_hotcache_counts_from_oss_to_s3_task >> final_dag_checker
