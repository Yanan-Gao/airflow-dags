from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

from ttd.kubernetes.pod_resources import PodResources
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.disney_id_integration.utils import Utils

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = "0 20 1,27,L * *"
    retry_delay = timedelta(minutes=30)
    collect_lookback_days = 30
else:
    schedule_interval = "0 * * * *"
    retry_delay = timedelta(minutes=10)
    collect_lookback_days = 1

start_date = datetime(2024, 5, 1)
next_month_cutover_day = 20
mappings_uid2_lookback_periods = 1
collect_task_id = "collect"
conversion_logs_dir = Utils.get_nebula_conversion_logs_dir()


def get_collected_dir(job) -> str:
    return f"{Utils.get_application_dir()}/{job}/target_date={{{{ get_target_date(task_instance) }}}}/execution_time={{{{ get_execution_time_str(data_interval_end) }}}}"


collected_from_mappings_dir = get_collected_dir("collect-cutover-from-mappings")
collected_dir = get_collected_dir("collect-cutover")

ARG_TARGET_DATE = "target_date"
ARG_MAPPINGS_UID2_CUTOVER_PERIODS = "mappings_uid2_cutover_periods"
ARG_COLLECT_LOGS_BEGIN = "collect_logs_after"
ARG_COLLECT_LOGS_END = "collect_logs_before"


def prepare_cutover_arguments(dag_run, task_instance, data_interval_end):
    execution_time = Utils.get_execution_time(data_interval_end)

    if dag_run.run_id.startswith('manual'):
        if dag_run.conf is None or \
                ARG_TARGET_DATE not in dag_run.conf or \
                ARG_COLLECT_LOGS_BEGIN not in dag_run.conf or \
                ARG_COLLECT_LOGS_END not in dag_run.conf:
            print(
                f"""
                Manually trigger this DAG need to provide all required arguments:

                Arguments:                              required?   format                      description
                {ARG_TARGET_DATE}:                      [required]  {Utils.DATE_FORMAT}         Target date
                {ARG_MAPPINGS_UID2_CUTOVER_PERIODS}:    [required]  Integer                     Periods of UID2 mappings used for cutover
                {ARG_COLLECT_LOGS_BEGIN}:               [required]  {Utils.TIMESTAMP_FORMAT}    Begin time of Nebula ID conversion logs used for cutover
                {ARG_COLLECT_LOGS_END}:                 [required]  {Utils.TIMESTAMP_FORMAT}    End time of Nebula ID conversion logs used for cutover
                """
            )
            raise AirflowFailException()

        target_date = datetime.strptime(dag_run.conf[ARG_TARGET_DATE], Utils.DATE_FORMAT)
        mappings_uid2_periods = int(dag_run.conf[ARG_MAPPINGS_UID2_CUTOVER_PERIODS])
        collect_logs_begin = datetime.strptime(dag_run.conf[ARG_COLLECT_LOGS_BEGIN], Utils.TIMESTAMP_FORMAT)
        collect_logs_end = datetime.strptime(dag_run.conf[ARG_COLLECT_LOGS_END], Utils.TIMESTAMP_FORMAT)
    else:
        mappings_uid2_periods = mappings_uid2_lookback_periods

        target_date = datetime(execution_time.year, execution_time.month, 1)
        if execution_time.day > next_month_cutover_day:  # if run day is greater than this, cutover is for next month
            target_date = target_date + relativedelta(months=1)

        collect_logs_begin = target_date if target_date < execution_time else execution_time
        collect_logs_begin = collect_logs_begin - timedelta(days=collect_lookback_days) - timedelta(minutes=2)  # do a full import
        if execution_time.day > next_month_cutover_day:  # if run day is greater than this, cutover is for next month
            collect_logs_begin = Utils.get_collect_files_begin_timestamp(
                dag_run, collect_task_id, collect_logs_begin
            )  # continue from last run

        collect_logs_end = execution_time - timedelta(minutes=2)
        if collect_logs_end > target_date:
            collect_logs_end = target_date

    # same to xcom
    print(f"{ARG_TARGET_DATE}: {target_date}")
    print(f"{ARG_MAPPINGS_UID2_CUTOVER_PERIODS}: {mappings_uid2_periods}")
    print(f"{ARG_COLLECT_LOGS_BEGIN}: {collect_logs_begin}")
    print(f"{ARG_COLLECT_LOGS_END}: {collect_logs_end}")

    task_instance.xcom_push(key=ARG_TARGET_DATE, value=target_date.strftime(Utils.DATE_FORMAT))
    task_instance.xcom_push(key=ARG_MAPPINGS_UID2_CUTOVER_PERIODS, value=mappings_uid2_periods)
    task_instance.xcom_push(key=ARG_COLLECT_LOGS_BEGIN, value=collect_logs_begin.strftime(Utils.TIMESTAMP_FORMAT))
    task_instance.xcom_push(key=ARG_COLLECT_LOGS_END, value=collect_logs_end.strftime(Utils.TIMESTAMP_FORMAT))


# macros
def get_target_date(task_instance) -> str:
    return task_instance.xcom_pull(key=ARG_TARGET_DATE)


def get_mapping_uid2_periods(task_instance) -> str:
    return task_instance.xcom_pull(key=ARG_MAPPINGS_UID2_CUTOVER_PERIODS)


def get_collect_logs_begin_timestamp(task_instance) -> str:
    return task_instance.xcom_pull(key=ARG_COLLECT_LOGS_BEGIN)


def get_collect_logs_end_timestamp(task_instance) -> str:
    return task_instance.xcom_pull(key=ARG_COLLECT_LOGS_END)


cutover_dag = Utils.create_dag(
    dag_id="disney-id-integration-cutover",
    start_date=start_date,
    schedule_interval=schedule_interval,
    retry_delay=retry_delay,
)
dag = cutover_dag.airflow_dag

dag.user_defined_macros.update({
    "get_target_date": get_target_date,
    "get_mapping_uid2_periods": get_mapping_uid2_periods,
    "get_collect_logs_begin_timestamp": get_collect_logs_begin_timestamp,
    "get_collect_logs_end_timestamp": get_collect_logs_end_timestamp,
})

prepare_cutover_arguments_task = OpTask(
    op=PythonOperator(task_id="prepare-cutover-arguments", provide_context=True, python_callable=prepare_cutover_arguments)
)

load_mappings_uid2_cutover_task = Utils.create_pod_operator(
    dag=dag,
    name="load-mappings-uid2-cutover",
    task_id="load-mappings-uid2-cutover",
    sumologic_source_category="disneyidintegration-load-mappings-uid2-cutover",
    prometheus_job="disneyidintegration-load-mappings-uid2-cutover",
    execution_timeout=timedelta(hours=6),
    arguments=[
        "load-mappings-uid2-cutover",
        "--target-date",
        "{{ get_target_date(task_instance) }}",
        "--periods",
        "{{ get_mapping_uid2_periods(task_instance) }}",
        "--collect-output",
        collected_from_mappings_dir,
    ],
)

collect_task = Utils.create_pod_operator(
    dag=dag,
    name="collect",
    task_id=collect_task_id,
    sumologic_source_category="disneyidintegration-collect-cutover",
    prometheus_job="disneyidintegration-collect-cutover",
    execution_timeout=timedelta(hours=16),
    arguments=[
        "collect",
        "cutover",
        "--logs",
        conversion_logs_dir,
        "--after",
        "{{ get_collect_logs_begin_timestamp(task_instance) }}",
        "--before",
        "{{ get_collect_logs_end_timestamp(task_instance) }}",
        "--target",
        "{{ get_target_date(task_instance) }}",
        "--output",
        collected_dir,
    ],
)

import_task_arguments = [
    "import", "--target-date", "{{ get_target_date(task_instance) }}", "--hash", "true", "--update-cumulative-imported-count-metrics",
    "false"
]

import_task_container_resources = PodResources(
    request_cpu="1000m",
    request_memory="6G",
    request_ephemeral_storage="8G",
    limit_cpu="2000m",
    limit_memory="8G",
    limit_ephemeral_storage="10G",
)

import_from_mappings_task = Utils.create_pod_operator(
    dag=dag,
    name="import-from-mappings",
    task_id="import-from-mappings",
    sumologic_source_category="disneyidintegration-import-cutover-from-mappings",
    prometheus_job="disneyidintegration-import-cutover-from-mappings",
    execution_timeout=timedelta(hours=6),
    container_resources=import_task_container_resources,
    arguments=import_task_arguments + ["--input", collected_from_mappings_dir],
)

import_task = Utils.create_pod_operator(
    dag=dag,
    name="import",
    task_id="import",
    sumologic_source_category="disneyidintegration-import-cutover",
    prometheus_job="disneyidintegration-import-cutover",
    execution_timeout=timedelta(hours=6),
    container_resources=import_task_container_resources,
    arguments=import_task_arguments + ["--input", collected_dir],
)

cutover_dag >> prepare_cutover_arguments_task >> load_mappings_uid2_cutover_task >> collect_task >> import_task
load_mappings_uid2_cutover_task >> import_from_mappings_task >> import_task
