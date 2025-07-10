import json
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
import logging

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import DATASRVC
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

testing = False


def choose(prod, test):
    return test if testing else prod


prefix = choose(prod='', test='dev-')
aws_conn_id = 'aws_default'
job_name = f'{prefix}chk-reds-retention'
searchbuckets = choose(
    prod=['thetradedesk-useast-partner-datafeed', 'thetradedesk-useast-partner-datafeed-scrubber-new-source'],
    test=['thetradedesk-useast-partner-datafeed-test2']
)
bucket_workitem = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_bucket_jar = choose(prod='thetradedesk-useast-qubole', test='thetradedesk-useast-partner-datafeed-test2')
scrub_partitions = '100'

pipeline = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 8, 1),
    on_failure_callback=dag_post_to_slack_callback(dag_name=job_name, step_name='', slack_channel='#scrum-data-services-alarms'),
    schedule_interval='30 4 * * *',
    max_active_runs=2,
    retries=0,
    retry_delay=timedelta(minutes=5),
    tags=['DATASRVC'],
    run_only_latest=True
)

# Get the actual DAG so we can add Operators
dag = pipeline.airflow_dag


def _create_spark_config(**context):
    dt = context['logical_date']
    now_dt = datetime.now(timezone.utc)
    now = now_dt.strftime('%Y-%m-%dT%H-%M-%S')
    date = str(dt.date())
    hour = str(dt.hour)
    key = f's3://{bucket_workitem}/reds-data-retention/{dt.year}/{dt.month}/{dt.day}/{prefix}{date}T{hour}-{now}.json'
    out = key.replace('.json', '.search.result.json')

    context['ti'].xcom_push(key='workitem_output', value=out)


def _validate_spark_output(**context):
    out = context['ti'].xcom_pull(dag_id=job_name, task_ids='create_spark_config', key='workitem_output')
    logging.info(f'Validating output for {out}')
    # S3 connection
    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    results = [json.loads(line) for line in s3_hook.read_key(out).splitlines()]
    logging.info(f'Successfully run retention search task for {len(results)} feed work items')

    # raise exception if there are any failures
    if results:
        count = len(results)
        for i, feed in enumerate(results, start=1):
            print(
                f'({i}/{count}) Detected reds feed should be deleted: prefix={feed.get("Prefix", "Unknown")} sourceBucket={feed.get("SourceBucket", "Unknown")}'
            )
        raise AirflowFailException(f'Found {count} feeds that violate retention policy')
    logging.info('Found none feed violates retention policy')


master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7g.m7g_2xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=4,
)

reds_retention_search_cluster = EmrClusterTask(
    name=f'{prefix}reds_retention_search_cluster',
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={
        'Team': DATASRVC.team.jira_team,
    },
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_auto_terminates=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3_1
)

reds_retention_search_step = EmrJobTask(
    cluster_specs=reds_retention_search_cluster.cluster_specs,
    name=f'{prefix}reds_retention_search_step',
    class_name='com.thetradedesk.reds.retention.RedsS3Deleter',
    executable_path=f's3://{scrub_bucket_jar}/reds-retention/uber-REDS_S3_Data_Retention-2.0-SNAPSHOT.jar',
    command_line_arguments=[
        '--partitions', scrub_partitions, '--testMode', 'false', '--mode', 'search', '--searchModeBucket', ",".join(searchbuckets),
        '--outputUrl', f'{{{{ task_instance.xcom_pull(dag_id="{job_name}", task_ids="create_spark_config", key="workitem_output") }}}}'
    ],
    configure_cluster_automatically=True
)

create_spark_config = OpTask(
    op=PythonOperator(
        task_id='create_spark_config',
        python_callable=_create_spark_config,
        provide_context=True,
        dag=dag,
    )
)

validate_spark_output = OpTask(
    op=PythonOperator(
        task_id='validate_spark_output',
        python_callable=_validate_spark_output,
        provide_context=True,
        dag=dag,
    )
)

reds_retention_search_cluster.add_sequential_body_task(reds_retention_search_step)

pipeline >> create_spark_config >> reds_retention_search_cluster >> validate_spark_output
