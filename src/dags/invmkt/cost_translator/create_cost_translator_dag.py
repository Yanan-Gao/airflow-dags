from datetime import timedelta

from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from pendulum import DateTime

from dags.hpc.cloud_storage_to_sql_db_task.cloud_storage_to_sql_db_task import create_cloud_storage_to_sql_db_task, CloudStorageToSqlColumn

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import hpc

from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE

JAR_PATH = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/cost_translator-assembly.jar"
####################################################

STORAGE_BUCKET = 'ttd-cost-translator'
S3_DEST_URI_BUCKET_KEY = 's3_dest_uri_bucket'
S3_DEST_URI_PREFIX_KEY = 's3_uri_prefix'
S3_DEST_URI_KEY = 's3_dest_uri'
CAPTURE_S3_BUCKET_PATH_TASK_ID = 'capture_run_date_snapshot'


def create_python_capture_task():

    def capture_s3_bucket_path(task_instance: TaskInstance, execution_date: DateTime, **kwargs):
        date = execution_date.strftime("%Y%m%d")
        storage_prefix = f'Prod/%s/v=1/date={date}'
        s3_destination_uri = f's3://{STORAGE_BUCKET}/{storage_prefix}'
        print(f"S3 Destination URI: {s3_destination_uri}")
        task_instance.xcom_push(S3_DEST_URI_BUCKET_KEY, STORAGE_BUCKET)
        task_instance.xcom_push(S3_DEST_URI_PREFIX_KEY, storage_prefix)
        task_instance.xcom_push(S3_DEST_URI_KEY, s3_destination_uri)

    return PythonOperator(task_id=CAPTURE_S3_BUCKET_PATH_TASK_ID, python_callable=capture_s3_bucket_path, provide_context=True)


def create_emr_tasks(name: str, emr_classes: dict[str, str]) -> tuple[EmrClusterTask, list[EmrJobTask]]:
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            I3.i3_4xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1,
    )

    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            I3.i3_16xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=7,
    )
    cluster_tags = {
        'Team': INVENTORY_MARKETPLACE.team.jira_team,
    }

    cluster = EmrClusterTask(
        name=name,
        cluster_tags=cluster_tags,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        enable_prometheus_monitoring=True,
        cluster_auto_termination_idle_timeout_seconds=60 * 60,
    )

    TIME_WINDOW = 60
    TRAINING_SHIFT = 15

    def cost_translator_emr(name: str, className: str) -> EmrJobTask:
        return EmrJobTask(
            name=name,
            class_name=f"com.thetradedesk.ds.libs.CostTranslator.{className}",
            executable_path=JAR_PATH,
            timeout_timedelta=timedelta(hours=3.5),
            additional_args_option_pairs_list=[("executor-memory", "80g"), ("executor-cores", "8"), ('conf', 'spark.driver.cores=42'),
                                               ('conf', 'spark.driver.memory=140g'), ('conf', 'spark.driver.maxResultSize=140g'),
                                               ('conf', 'spark.executor.memory=80g'), ('conf', 'spark.sql.shuffle.partitions=7000'),
                                               ('conf', 'spark.default.parallelism=7000'),
                                               ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                                               ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC")],
            eldorado_config_option_pairs_list=[
                ('S3DateKey', f"{{{{ task_instance.xcom_pull(task_ids='{CAPTURE_S3_BUCKET_PATH_TASK_ID}', key='{S3_DEST_URI_KEY}') }}}}"),
                ('TimeWindowKey', TIME_WINDOW), ('TrainingShiftKey', TRAINING_SHIFT)
            ]
        )

    cost_translator_tasks = [cost_translator_emr(name, class_name) for (name, class_name) in emr_classes.items()]
    for task in cost_translator_tasks:
        cluster.add_parallel_body_task(task)

    return (cluster, cost_translator_tasks)


PROPAGATE_TO_SQL_TASK_NAME = "propagate_cost_ratios_to_sql_server"
PROPAGATE_TASK_BATCH_SIZE = int(5e5)
PROPAGATE_TIMEOUT_IN_SECONDS = int(1e5)


def propagation_task(table, columns):
    return create_cloud_storage_to_sql_db_task(
        name=f'{PROPAGATE_TO_SQL_TASK_NAME}_{table}',
        scrum_team=hpc,
        dataset_name=f'{table}',
        destination_database_name='CostRatio',
        destination_table_schema_name='dbo',
        destination_table_name=table,
        column_mapping=[CloudStorageToSqlColumn(column, column) for column in columns],
        staging_database_name='CostRatio',
        staging_table_schema_name='staging',
        insert_to_staging_table_batch_size=PROPAGATE_TASK_BATCH_SIZE,
        insert_to_staging_table_timeout=int(PROPAGATE_TIMEOUT_IN_SECONDS / 1.5),
        storage_bucket=f"{{{{ task_instance.xcom_pull(task_ids='{CAPTURE_S3_BUCKET_PATH_TASK_ID}', key='{S3_DEST_URI_BUCKET_KEY}') }}}}",
        storage_key_prefix=(
            f"{{{{ task_instance.xcom_pull(task_ids='{CAPTURE_S3_BUCKET_PATH_TASK_ID}', key='{S3_DEST_URI_PREFIX_KEY}')%'{table}' }}}}"
        ),
        task_execution_timeout=timedelta(seconds=PROPAGATE_TIMEOUT_IN_SECONDS),
        use_partition_switch=True
    )
