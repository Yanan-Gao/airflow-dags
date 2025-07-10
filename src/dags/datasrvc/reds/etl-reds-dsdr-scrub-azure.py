"""
Perform REDS dsdr scrubbing
"""

from datetime import datetime, timedelta

from dags.datasrvc.reds.dsdr_scrub_builder import DsdrStepBuilder, get_azure_container
from dags.datasrvc.reds.redsfeed import RedsFeed
from dags.datasrvc.utils.common import is_prod
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.el_dorado.v2.hdi import HDIClusterTask

from ttd.eldorado.base import TtdDag
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.tasks.op import OpTask

reds_sa = 'eastusttdreds'

testing = not is_prod()


def choose(prod, test):
    return test if testing else prod


prefix = choose(prod='', test='dev-')
job_name = f'{prefix}etl-reds-dsdr-scrub-azure'
storage_conn_id = 'azure_reds_sp'
provisioning_conn_id = 'provdb-readonly'

partner_dsdr_bucket = choose(prod='ttd-data-subject-requests', test='thetradedesk-useast-partner-datafeed-test2')

# partner_dsdr_bucket is useless for azure blog
scrub_parent_bucket = choose(prod='', test=f'dsdr-scrubbing@{reds_sa}')
scrub_backup_bucket = choose(prod='partner-datafeed-backup', test=f'dsdr-scrubbing@{reds_sa}')
scrub_metadata_bucket = choose(prod='dsdr-scrubbing', test=f'dsdr-scrubbing@{reds_sa}')
scrub_jar_bucket = choose(prod='ttd-build-artefacts', test='ttd-build-artefacts')
scrub_partitions = '100'
dsdr_prefix = 'reds-dsdr-scrubbing'
biz_mode = 'reds'
scrub_test_mode = 'false'
scrub_timeout_in_minutes = '240'
scrub_parallelism = '20'

pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 1),
    run_only_latest=True,
    schedule_interval=timedelta(days=7),
    retries=3,
    retry_delay=timedelta(minutes=30),
    slack_channel="#scrum-data-services-alarms",
    tags=['DATASRVC', 'REDS', 'DSDR']
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag

##################################
# Spark DAG Configurations
##################################

cluster_task = HDIClusterTask(
    name='redscluster',
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D13_v2(),
        workernode_type=HDIInstanceTypes.Standard_D5_v2(),
        num_workernode=6,
        disks_per_node=1
    ),
    cluster_version=HDIClusterVersions.AzureHdiSpark33,
    enable_openlineage=False
)

step_builder = DsdrStepBuilder(
    job_name=job_name,
    dag=dag,
    env_prefix=prefix,
    dsdr_bucket=partner_dsdr_bucket,
    parent_bucket=scrub_parent_bucket,
    backup_bucket=scrub_backup_bucket,
    workitem_meta_bucket=scrub_metadata_bucket,
    jar_bucket=scrub_jar_bucket,
    dsdr_root=dsdr_prefix,
    storage_conn_id=storage_conn_id,
    feed_class=RedsFeed,
    feed_query_conn_id=provisioning_conn_id,
    biz_mode=biz_mode,
    scrub_partitions=scrub_partitions,
    cloud_provider='azure',
    work_item_override_bucket=choose(None, get_azure_container(scrub_parent_bucket)),
    parallelism=scrub_parallelism,
    timeout_in_mins=scrub_timeout_in_minutes,
    test_mode=scrub_test_mode,
    prod_test_folder_prefix=choose(prod="env=prod", test="env=test")
)

step_get_id = OpTask(op=step_builder.step_get_dsdr_id_graph())
step_get_feeds = OpTask(op=step_builder.step_get_available_feeds())
step_check = OpTask(op=step_builder.step_shortcircuit_feed_check())
step_prepare = OpTask(op=step_builder.step_prepare_work_item())
step_validate = OpTask(op=step_builder.step_validate_dsdr_scrub_output())

cluster_task.add_sequential_body_task(step_builder.get_hdi_job(cluster_task))

pipeline >> cluster_task

pipeline >> step_get_id >> step_get_feeds >> step_check >> step_prepare >> cluster_task

cluster_task >> step_validate
