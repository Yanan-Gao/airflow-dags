"""
Perform ExFeed dsdr scrubbing
"""
import copy
from datetime import datetime, timedelta

from dags.datasrvc.reds.dsdr_scrub_builder import dsdr_scrubber_application_configuration, DsdrStepBuilder
from dags.datasrvc.reds.exposurefeed_dsdr import ExposureFeedDsdr
from dags.datasrvc.utils.common import is_prod
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

testing = not is_prod()
# This flas is passed to a spark job: if it is set to false - it would do the actual scrubbing
scrub_test_mode = 'false'


def choose(prod, test):
    return test if testing else prod


prefix = choose(prod='', test='dev-')
partner_dsdr_bucket = choose(prod='ttd-data-subject-requests', test='thetradedesk-useast-partner-datafeed-test2')
scrub_parent_bucket = choose(prod='thetradedesk-useast-partner-datafeed', test='thetradedesk-useast-partner-datafeed')
scrub_backup_bucket = choose(
    prod='thetradedesk-useast-partner-datafeed-scrubber-new-source', test='thetradedesk-useast-partner-datafeed-test'
)
scrub_metadata_bucket = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_jar_bucket = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
work_item_override_bucket = choose(prod=None, test=None)
scrub_partitions = '100'
scrub_parallelism = '20'
scrub_timeout_in_minutes = '240'
scrub_enable_backup = 'true'
dsdr_prefix = 'exfeed-dsdr-scrubbing'
biz_mode = 'exfeed'
lookback_days = '189'  # 90 days of back-fill data + 99 days of retention

##################################
# Job Configurations
##################################
job_name = f'{prefix}etl-exposure-feed-dsdr-scrub'
feed_query_conn_id = 'schedreportingdb-readonly'

##################################
# DAG Configurations
##################################

pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 6, 1),
    run_only_latest=True,
    schedule_interval=timedelta(days=7),
    max_active_runs=1,
    retries=3,
    retry_delay=timedelta(minutes=30),
    tags=['DATASRVC', 'Exposure', 'DSDR'],
    slack_channel='#scrum-data-services-alarms',
    slack_tags=DATASRVC.team.jira_team,
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=job_name, step_name='parent dagrun', slack_channel='#scrum-data-services-alarms'),
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag

##################################
# Spark DAG Configurations
##################################

cluster = EmrClusterTask(
    name=f'{prefix}exfeed_dsdr_scrub_cluster',
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags={
        'Team': DATASRVC.team.jira_team,
        'long_running': 'true'
    },
    cluster_auto_terminates=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_1,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=8,
    ),
    additional_application_configurations=copy.deepcopy(dsdr_scrubber_application_configuration),
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
    feed_class=ExposureFeedDsdr,
    feed_query_conn_id=feed_query_conn_id,
    biz_mode=biz_mode,
    scrub_partitions=scrub_partitions,
    parallelism=scrub_parallelism,
    timeout_in_mins=scrub_timeout_in_minutes,
    test_mode=scrub_test_mode,
    lookback_days=lookback_days,
    work_item_override_bucket=work_item_override_bucket,
    prod_test_folder_prefix=choose(prod="env=prod", test="env=test")
)

cluster.add_sequential_body_task(step_builder.step_dsdr_scrub(cluster))

step_dsdr_scrub = OpTask(op=step_builder.step_dsdr_scrub(cluster))
step_get_dsdr_id_graph = OpTask(op=step_builder.step_get_dsdr_id_graph())
step_get_available_feeds = OpTask(op=step_builder.step_get_available_feeds())
step_shortcircuit_feed_check = OpTask(op=step_builder.step_shortcircuit_feed_check())
step_prepare_work_item = OpTask(op=step_builder.step_prepare_work_item())
step_validate_dsdr_scrub_output = OpTask(op=step_builder.step_validate_dsdr_scrub_output())

pipeline >> step_get_dsdr_id_graph >> step_get_available_feeds >> step_shortcircuit_feed_check >> step_prepare_work_item >> cluster >> step_validate_dsdr_scrub_output
