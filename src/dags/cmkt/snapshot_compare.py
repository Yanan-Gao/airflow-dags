import logging
import pathlib

from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance
import boto3
from datetime import timedelta, datetime

from dags.cmkt.elasticsearch.common import get_common_eldorado_config, cmkt_spark_aws_executable_path, emr_additional_args_option_pairs_list
from dags.cmkt.elasticsearch.cluster_tasks import new_cluster_task
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import cmkt
from ttd.tasks.base import BaseTask
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

_logger = logging.getLogger(__name__)

_job_start_date = datetime(2025, 4, 8)

_env_name = "Production"
_slack_channel = "#scrum-cmkt-alarms"
(_s3_bucket, _s3_env_prefix) = ("ttd-product-catalog-elasticsearch", pathlib.PurePosixPath("env=prod"))
_cleanroom_s3_prefix = "cleanroom-output-20250410-1645"

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    _env_name = "Sandbox"
    _slack_channel = "#cmkt-test-alarms"
    (_s3_bucket, _s3_env_prefix) = ("ttd-product-catalog-elasticsearch-sandbox", pathlib.PurePosixPath("env=test"))

_name = "cmkt-elasticsearch-snapshot-compare"

_snapshot_type = "MerchantProduct"
_primary_key_column = "ProductId"

_dag = TtdDag(
    dag_id=_name,
    slack_channel=_slack_channel,
    start_date=_job_start_date,
    tags=[cmkt.jira_team],
    enable_slack_alert=False,
    run_only_latest=True,
    retry_delay=timedelta(minutes=1),
)

# region Tasks


def new_snapshot_compare_cluster_task(name: str) -> BaseTask:
    cluster = new_cluster_task(
        name, [
            (M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)),
        ], 1 + 3
    )  # 1 (driver) + n (executors)
    pipeline_step = new_snapshot_compare_job_task()
    cluster.add_parallel_body_task(pipeline_step)

    determine_comparison_args_task = new_determine_comparison_args_task()
    determine_comparison_args_task >> cluster

    return determine_comparison_args_task


def determine_comparison_args(snapshot_type: str, primary_key_column: str, cleanroom_s3_prefix: str, task_instance: TaskInstance):
    snapshot_prefix = _s3_env_prefix / snapshot_type
    cleanroom_prefix = _s3_env_prefix / cleanroom_s3_prefix / "type=full" / "v=1"
    found_candidate = determine_candidate_prefix(snapshot_prefix, cleanroom_prefix)

    if not found_candidate:
        raise ValueError("No suitable prefixes found")

    task_instance.xcom_push(key='generated_snapshot_prefix', value=f"s3://{_s3_bucket / snapshot_prefix / found_candidate}")
    task_instance.xcom_push(key='expected_prefix', value=f"s3://{_s3_bucket / cleanroom_prefix / found_candidate}")
    task_instance.xcom_push(key='primary_key_column', value=primary_key_column)


def new_determine_comparison_args_task():
    return OpTask(
        op=PythonOperator(
            task_id=determine_comparison_args.__name__,
            python_callable=determine_comparison_args,
            provide_context=True,
            dag=_airflow_dag,
            op_kwargs={
                "snapshot_type": "{{dag_run.conf.get('snapshot_type', '%s')}}" % _snapshot_type,
                "primary_key_column": "{{dag_run.conf.get('primary_key_column', '%s')}}" % _primary_key_column,
                "cleanroom_s3_prefix": "{{dag_run.conf.get('cleanroom_s3_prefix', '%s')}}" % _cleanroom_s3_prefix,
            }
        )
    )


def new_snapshot_compare_job_task() -> EmrJobTask:
    task_name = "snapshot-compare"

    pipeline_step = EmrJobTask(
        name=task_name,
        command_line_arguments=[
            "--expectedPrefix={{task_instance.xcom_pull(task_ids='%s', key='expected_prefix')}}" % determine_comparison_args.__name__,
            "--generatedSnapshotPrefix={{task_instance.xcom_pull(task_ids='%s', key='generated_snapshot_prefix')}}" %
            determine_comparison_args.__name__,
            "--primaryKeyColumn={{task_instance.xcom_pull(task_ids='%s', key='primary_key_column')}}" % determine_comparison_args.__name__,
            "--ignoreSchemaDifference=true",
        ],
        executable_path=cmkt_spark_aws_executable_path,
        class_name="jobs.cmkt.elasticsearch.snapshotcompare.SnapshotComparerJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=get_common_eldorado_config(),
        # source: dataproc/monitoring/spark_canary_job.py
        cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        additional_args_option_pairs_list=emr_additional_args_option_pairs_list,
        timeout_timedelta=timedelta(hours=3),
        retries=0,
    )

    return pipeline_step


# endregion

# region Utilities


def list_s3_subprefixes(bucket_name, prefix='', depth=1):
    """"
    Given an S3 bucket and prefix, list the child prefixes under it at a specific depth.

    Eg. given these contents
    - s3://bucket
    - s3://bucket/prefix
    - s3://bucket/prefix/date=20250403
    - s3://bucket/prefix/date=20250403/time=112233
    - s3://bucket/prefix/date=20250403/time=112233/part-1.parquet

    list_s3_subprefixes("bucket", "prefix", depth=2) returns

    [
      "date=20250403/time=112233/"
    ]

    """
    if len(prefix) >= 1 and prefix[-1] != '/':
        prefix = prefix + '/'
    s3_client = boto3.client('s3')

    subprefixes = set()

    def handle_list_s3_subprefixes(current_depth, current_prefix):
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=current_prefix, Delimiter='/')
        for obj in response.get('CommonPrefixes', []):
            if current_depth == 1:
                subprefixes.add(obj['Prefix'])
            else:
                handle_list_s3_subprefixes(current_depth - 1, obj['Prefix'])

    handle_list_s3_subprefixes(depth, prefix)

    return [subprefix.replace(prefix, "", 1) for subprefix in subprefixes]


def determine_candidate_prefix(snapshot_prefix: pathlib.PurePosixPath, cleanroom_prefix: pathlib.PurePosixPath) -> str | None:
    """
    Problem statement:
    - given m snapshots in {bucket}/{snapshotType}/
        - date=DATE1/time=TIME1,
        - date=DATE2/time=TIME2,
        - ...
    - and given n expected subprefixes in {bucket}/{cleanroom-output-prefix}/
        - date=DATE2/time=TIME2,

    find a common date=.../time=... that is present among m and n.

    We will give "priority" to cleanroom subprefixes; that is, we will iterate through available cleanroom subprefixes, then try to find a matching snapshot.
    """
    available_snapshot_prefixes = list_s3_subprefixes(_s3_bucket, str(snapshot_prefix), depth=2)
    available_cleanroom_prefixes = list_s3_subprefixes(_s3_bucket, str(cleanroom_prefix), depth=2)

    _logger.info("Available prefixes under snapshot prefix %s: %r", snapshot_prefix, available_snapshot_prefixes)
    _logger.info("Available prefixes under cleanroom prefix %s: %r", cleanroom_prefix, available_cleanroom_prefixes)

    found_candidate: str | None = None

    for candidate in sorted(available_cleanroom_prefixes, reverse=True):
        if candidate in available_snapshot_prefixes:
            found_candidate = candidate
            break

    return found_candidate


# endregion

_airflow_dag = _dag.airflow_dag

_cluster_task = new_snapshot_compare_cluster_task(_name)
_dag >> _cluster_task
