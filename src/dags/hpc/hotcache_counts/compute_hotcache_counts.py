from datetime import datetime, timedelta

import dags.hpc.constants as constants
from dags.hpc.counts_datasources import CountsDatasources
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask

dag_name = 'compute-hotcache-counts'
cadence_in_hours = 24
schedule_interval = timedelta(hours=cadence_in_hours)
dagrun_timeout = timedelta(hours=12)
eldorado_jar_path = constants.HPC_AZURE_EL_DORADO_JAR_URL

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 6, 28),
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
sample_mod = 1000

datacenter_ip_mapping = {
    "ca2": constants.CA2_HC_ADDRESS,
    "ny1": constants.NY1_HC_ADDRESS,
    "va6": constants.VA6_HC_ADDRESS,
}

compute_hotcache_counts_headnode_type = HDIInstanceTypes.Standard_D4A_v4()
compute_hotcache_counts_workernode_type = HDIInstanceTypes.Standard_D32A_v4()
compute_hotcache_counts_num_workernode = 8
compute_hotcache_counts_disks_per_node = 1

compute_hotcache_counts_class_name = 'com.thetradedesk.jobs.hotcachecounts.ComputeHotCacheCounts'
current_date = datetime.today().strftime('%Y%m%d')

compute_hotcache_counts_job_el_dorado_config_options = [
    ("datetime", "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}"), ('cadenceInHours', cadence_in_hours),
    ('partitionFactor', partition_factor), ('parquetStorageLoc', f'{constants.HOTCACHE_COUNTS_WRITE_PATH}/date={current_date}/'),
    ('sampleMod', sample_mod), ('recordsPerSecond', 10000), ('openlineage.enable', 'false')
]

final_dag_checker = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Creating cluster and task for each DC and running them in parallel
for dc, seed_address in datacenter_ip_mapping.items():
    compute_hotcache_counts_cluster = HDIClusterTask(
        name=f'compute-hotcache-counts-cluster-{dc}',
        vm_config=HDIVMConfig(
            headnode_type=compute_hotcache_counts_headnode_type,
            workernode_type=compute_hotcache_counts_workernode_type,
            num_workernode=compute_hotcache_counts_num_workernode,
            disks_per_node=compute_hotcache_counts_disks_per_node
        ),
        cluster_version=HDIClusterVersions.AzureHdiSpark33,
        cluster_tags=constants.DEFAULT_CLUSTER_TAGS,
        retries=1,
        retry_delay=timedelta(minutes=10),
        # at the time of adding this, openlineage seems to be causing an issue with creating clusters
        enable_openlineage=False
    )

    compute_hotcache_counts_task = HDIJobTask(
        name=f'compute-hotcache-counts-job-{dc}',
        class_name=compute_hotcache_counts_class_name,
        cluster_specs=compute_hotcache_counts_cluster.cluster_specs,
        eldorado_config_option_pairs_list=compute_hotcache_counts_job_el_dorado_config_options + [
            ('datacenter', dc),
            ('address', seed_address),
        ],
        configure_cluster_automatically=True,
        command_line_arguments=['--version'],
        jar_path=eldorado_jar_path
    )

    compute_hotcache_counts_cluster.add_parallel_body_task(compute_hotcache_counts_task)

    copy_hotcache_counts_from_azure_to_s3_task = DatasetTransferTask(
        name=f"copy-hotcache-counts-from-azure-to-s3-{dc}",
        dataset=CountsDatasources.hotcache_counts,
        src_cloud_provider=CloudProviders.azure,
        dst_cloud_provider=CloudProviders.aws,
        partitioning_args=CountsDatasources.hotcache_counts.get_partitioning_args(
            ds_date='{{ dag_run.start_date.strftime(\"%Y-%m-%d\") }}'
        )
    )

    dag >> compute_hotcache_counts_cluster >> copy_hotcache_counts_from_azure_to_s3_task >> final_dag_checker
