from datetime import datetime, timedelta
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

# There are two run times:
# At the beginning of month or end of month
# For data at beginning of the month, we want to set a "target_date" (in the C# code) to be the current month.
# For data at they end of the month, that should target the next month.
# All of this should be handled in the C# code, but we pass in "run_time" and have a cutoff in the C# code (currently the 19th) to determine when the end of month or beginning is.
# So if run_time is after the 19th, we target next month. Else we target the current month.
# So if there are errors or we need to re-run stuff, we should instead just manually set the "before", "after" and "date" options in the code below, making sure run_time is set to either before or after the 19th.

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 16 1-6,25-31 * *"
    retry_delay = timedelta(minutes=30)
    input_directory = "s3://thetradedesk-useast-data-import/liveramp-nebulaid-rampid-mappings"
else:
    schedule_interval = "*/15 * * * *"
    retry_delay = timedelta(minutes=10)
    input_directory = "s3://ttd-adhoc-measure-useast/env=dev/disney-id-integration/nebulaid-rampid-mappings"

start_date = datetime(2025, 1, 5)
load_mappings_idl_task_id = "load-mappings-idl"
run_time = "{{ data_interval_end.strftime(\"%Y-%m-%d\") }}"


def get_mapping_files_begin_timestamp(dag_run, data_interval_end) -> str:
    return Utils.get_load_mappings_files_begin_timestamp(dag_run, load_mappings_idl_task_id,
                                                         data_interval_end - timedelta(days=10)).strftime(Utils.TIMESTAMP_FORMAT)


# note the timedelta of 10 days. This doesn't matter too much, since we properly handle the monthly cutoff in the code.


def get_mapping_files_end_timestamp(data_interval_end) -> str:
    return (data_interval_end - timedelta(minutes=2)).strftime(Utils.TIMESTAMP_FORMAT)


# DAG
load_mappings_idl_dag = Utils.create_dag(
    dag_id="disney-id-integration-load-mappings-idl",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = load_mappings_idl_dag.airflow_dag

dag.user_defined_macros.update({
    "get_mapping_files_begin_timestamp": get_mapping_files_begin_timestamp,
    "get_mapping_files_end_timestamp": get_mapping_files_end_timestamp,
})

load_mappings_idl_task = Utils.create_pod_operator(
    dag=dag,
    name="load-mappings-idl",
    task_id=load_mappings_idl_task_id,
    sumologic_source_category="disneyidintegration-load-mappings-idl",
    prometheus_job="disneyidintegration-load-mappings-idl",
    execution_timeout=timedelta(hours=6),
    arguments=[
        "load-mappings-idl", "--input", input_directory, "--after", "{{ get_mapping_files_begin_timestamp(dag_run, data_interval_end) }}",
        "--before", "{{ get_mapping_files_end_timestamp(data_interval_end) }}", "--date", run_time
    ],
)

load_mappings_idl_dag >> load_mappings_idl_task
