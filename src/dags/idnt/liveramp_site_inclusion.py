from airflow import DAG
from datetime import datetime
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables, Directories, Tags
from dags.idnt.util.s3_utils import get_latest_etl_date_from_path
from dags.idnt.vendors.vendor_alerts import LateVendor
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.tasks.op import OpTask
from typing import Callable, Optional, cast
import logging
from ttd.interop.logworkflow_callables import ExecuteOnDemandDataMove

input_path = "s3://thetradedesk-useast-data-import/liveramp/eudomains"
test_folder = Directories.test_dir_no_prod()
output_path = f"s3://ttd-identity/liveramp{test_folder}/eudomains"
class_name = "com.thetradedesk.etl.misc.LiverampSiteInclusionListConverter"
date_format = "%Y%m%d"
input_path_generator = cast(
    Callable[[Optional[datetime]], str], lambda d=None: input_path + (f"/{d.strftime(date_format)}/" if d is not None else "")
)
ttd_env = Tags.environment()

# LWDB configs
# NOTE: The sandbox connection currently isn't supported, so logworkflow task will fail when testing this dag in the prodTest environment.
# This is set on purpose to avoid triggering DataMover in a prod test.
logworkflow_connection = "lwdb"
logworkflow_sandbox_connection = "sandbox-lwdb"
logworkflow_connection_open_gate = logworkflow_connection if ttd_env == "prod" \
    else logworkflow_sandbox_connection
liveramp_site_inclusion_sql_import_task_id = 1000829

dag = DagHelpers.identity_dag(
    dag_id="liveramp_site_inclusion",
    schedule_interval="0 0 * * *",  # daily
    start_date=datetime(2025, 7, 16),
    run_only_latest=True
)


def check_for_input_date(execution_date: datetime, **context) -> bool:
    """Returns if a new file should be processed.

    Args:
        execution_date: Execution date of the DAG run.

    Returns:
        bool: True if file should be processed, False otherwise.
    """
    hook = AwsCloudStorage(conn_id='aws_default')
    last_etl_date = get_latest_etl_date_from_path(hook, output_path, execution_date, date_format=date_format)
    logging.info(f"Latest ETL date: {last_etl_date}")
    latest_input_date = LateVendor.get_latest_input_date(
        current_date=execution_date, last_etl_date=last_etl_date, input_path_generator=input_path_generator
    )
    logging.info(f"Latest Input date: {latest_input_date}")

    if latest_input_date is not None:
        # Push latest_etl_date to xcom to use when creating task
        context["ti"].xcom_push(key="latest_input_date", value=latest_input_date.strftime(date_format))
        return True

    return False


def get_cluster_with_created_task(main_dag, **context):
    """Creates cluster with task, with appropriate date.
        """
    cluster = IdentityClusters.get_cluster("liveramp_site_inclusion_cluster", main_dag, 32, ComputeType.STORAGE, cpu_bounds=(32, 32))
    task = IdentityClusters.task(
        class_name=class_name,
        eldorado_configs=[
            ('LOCAL', 'false'),
            ('DATE', "{{ task_instance.xcom_pull(task_ids='liveramp_site_inclusion_should_skip', key='latest_input_date') }}"),
            ('WRITE_PATH', output_path),
        ],
        executable_path=Executables.etl_repo_executable
    )
    cluster.add_sequential_body_task(task)

    return cluster


logworkflow_open_liveramp_site_inclusion_sql_import_gate = OpTask(
    op=PythonOperator(
        dag=dag.airflow_dag,
        python_callable=ExecuteOnDemandDataMove,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection_open_gate,
            'sproc_arguments': {
                'taskId': liveramp_site_inclusion_sql_import_task_id,
                'prefix': """date={{ task_instance.xcom_pull(task_ids='liveramp_site_inclusion_should_skip', key='latest_input_date') }}/"""
            }
        },
        task_id="logworkflow_open_liveramp_site_inclusion_sql_import_gate",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
)

should_skip = OpTask(
    op=ShortCircuitOperator(task_id="liveramp_site_inclusion_should_skip", python_callable=check_for_input_date, provide_context=True)
)

cluster = get_cluster_with_created_task(dag)

dag >> should_skip >> cluster >> logworkflow_open_liveramp_site_inclusion_sql_import_gate

final_dag: DAG = dag.airflow_dag
