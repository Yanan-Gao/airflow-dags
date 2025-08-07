# from airflow import DAG
from datetime import datetime, timedelta
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
from airflow.operators.python_operator import ShortCircuitOperator
from dags.idnt.util.s3_utils import count_number_of_nested_folders_in_prefix
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dataclasses import dataclass
from dags.idnt.statics import RunTimes

job_schedule_interval = '0 8 * * *'  # Have it run once a day.
job_start_date = datetime(2024, 8, 6, 2, 0)
ttd_env = TtdEnvFactory.get_from_system().execution_env
scoring_output_suffix = '' if str(ttd_env) == 'prod' else '/test'

dag = DagHelpers.identity_dag(
    dag_id="scoring_service_per_vendor",
    schedule_interval=job_schedule_interval,
    start_date=job_start_date,
    retries=1,
    retry_delay=timedelta(minutes=30),
    run_only_latest=True
)


@dataclass
class VendorConfig:
    """Class for keeping track of vendors and their related configs."""
    vendor_name: str
    vendor_input_path: str
    final_output_path: str
    bucket: str


# initializing vendors
vendor_configs = [
    VendorConfig('tapad', 'scoring', 'scoring', 'thetradedesk-fileshare'),
    VendorConfig('id5', 'graph/id5.io/scoring', 'graph/id5.io/scoring', 'thetradedesk-useast-data-import'),
    VendorConfig('identitylink', 'liveramp-idlink/scoring', 'liveramp-idlink/scoring.output', 'thetradedesk-fileshare'),
]


def should_process_vendor(**kwargs):
    vendor_config = kwargs['vendor']
    execute_date = kwargs['logical_date']

    client_output_count = count_number_of_nested_folders_in_prefix(
        bucket=vendor_config.bucket,
        prefix=f"{vendor_config.final_output_path}{scoring_output_suffix}/{vendor_config.vendor_name}/IA-evaluation-output/",
        execute_date=execute_date,
        search_success_prefix=True
    )
    client_input_count = count_number_of_nested_folders_in_prefix(
        bucket=vendor_config.bucket,
        prefix=f"{vendor_config.vendor_input_path}/{vendor_config.vendor_name}/",
        execute_date=execute_date,
        search_success_prefix=False
    )

    return client_output_count != client_input_count


def add_taskgroup_for_vendor(vendor_config):
    should_process_vendor_task = OpTask(
        op=ShortCircuitOperator(
            task_id=f'should_process_vendor_task_{vendor_config.vendor_name}',
            python_callable=should_process_vendor,
            dag=dag.airflow_dag,
            op_kwargs={'vendor': vendor_config},
            provide_context=True,
            retries=1
        )
    )

    cluster = IdentityClusters.get_cluster(
        f'scoring_service_cluster_{vendor_config.vendor_name}',
        dag,
        4000,
        ComputeType.STORAGE,
        use_delta=False,
        emr_release_label=Executables.emr_version_6
    )

    task_descriptor_etl = IdentityClusters.task(
        class_name="com.thetradedesk.etl.logformats.ScoringServiceETL",
        eldorado_configs=[("VENDOR_NAME", vendor_config.vendor_name),
                          ("INPUT_PATH", f"s3://{vendor_config.bucket}/{vendor_config.vendor_input_path}"),
                          ("OUTPUT_PATH",
                           f's3://ttd-identity/sxd-etl/IA-graph-evaluation{scoring_output_suffix}'), ("NUM_PARTITIONS", '400'),
                          ("STATS_PATH", f's3://ttd-identity/sxd-etl/IA-graph-evaluation{scoring_output_suffix}')],
        executable_path=Executables.etl_repo_executable
    )

    task_descriptor_scoring = IdentityClusters.task(
        class_name="com.thetradedesk.etl.scoring.ScoringServiceDriver",
        eldorado_configs=[("VENDOR_NAME", vendor_config.vendor_name),
                          ("INPUT_PATH", f's3://ttd-identity/sxd-etl/IA-graph-evaluation{scoring_output_suffix}'),
                          ("FINAL_OUTPUT_PATH", f's3://{vendor_config.bucket}/{vendor_config.final_output_path}{scoring_output_suffix}'),
                          ("STATS_PATH", f's3://ttd-identity/sxd-etl/IA-graph-evaluation{scoring_output_suffix}'),
                          ("DETERMINISTIC_GRAPHS_BASE_PATH", 's3://thetradedesk-useast-data-import/sxd-etl/deterministic'),
                          ("USE_UTS_INSTEAD_BENCHMARKS", 'true'),
                          ("UTS_PATH", 's3://thetradedesk-useast-data-import/sxd-etl/deterministic/uts_graph/devices/v=1'),
                          ("DATE", RunTimes.previous_full_day)],
        executable_path=Executables.etl_repo_executable
    )

    cluster.add_sequential_body_task(task_descriptor_etl)
    cluster.add_sequential_body_task(task_descriptor_scoring)

    dag >> should_process_vendor_task >> cluster


# iterate over vendors
for vendor in vendor_configs:
    add_taskgroup_for_vendor(vendor)

scoring_service_dag = dag.airflow_dag
