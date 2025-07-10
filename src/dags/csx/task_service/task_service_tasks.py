from datetime import datetime, timedelta, time
from ttd.task_service.task_service_dag import TaskServiceDagFactory, create_all_for_vertica_variant_group, \
    TaskServiceDagRegistry
from ttd.slack.slack_groups import csx
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.vertica_clusters import TaskVariantGroup
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.interop.logworkflow_callables import IsVerticaEnabled
from ttd.ttdenv import TtdEnvFactory
from airflow.operators.python_operator import ShortCircuitOperator
from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils.state import DagRunState, State
from airflow.utils import timezone
from pendulum import DateTime
import logging

job_environment = TtdEnvFactory.get_from_system()
task_failure_tolerance = timedelta(hours=3)

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="DealSpendReportTask",
        task_config_name="DealSpendReportConfig",
        scrum_team=csx,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/30 * * * *",
        resources=TaskServicePodResources.medium(),
    )
)

vertica_adgroup_daily_metadata_tasks = create_all_for_vertica_variant_group(
    vertica_variant_group=TaskVariantGroup.VerticaAws,
    task_name="VerticaAdGroupDailyMetadataTask",
    task_name_suffix="CustomInterval",
    task_config_name="VerticaAdGroupDailyMetadataTaskConfig",
    scrum_team=csx,
    start_date=datetime.now() - timedelta(hours=3),
    job_schedule_interval="0 * * * *",
    resources=TaskServicePodResources.medium(),
    telnet_commands=["try changeField VerticaAdGroupDailyMetadataTask.LetQueryRunOnCustomInterval true"]
)


def task_not_succeeded_for_too_long(dag: DAG, base_task_id: str, data_interval_start: DateTime, **kwargs):
    task = dag.get_task(base_task_id)
    ti = TaskInstance(task=task, execution_date=data_interval_start)
    prev_ti = ti.get_previous_ti(state=DagRunState.SUCCESS)
    while prev_ti and prev_ti.state != State.SUCCESS:  # skipped status is considered as success in .get_previous_ti() method
        logging.info(prev_ti)
        if prev_ti is None or (prev_ti.start_date
                               and prev_ti.start_date < datetime.utcnow().replace(tzinfo=timezone.utc) - task_failure_tolerance):
            return True
        prev_ti.task = dag.get_task(prev_ti.task_id)
        prev_ti = prev_ti.get_previous_ti()
    logging.info(prev_ti)
    if prev_ti is None or (prev_ti.start_date
                           and prev_ti.start_date < datetime.utcnow().replace(tzinfo=timezone.utc) - task_failure_tolerance):
        return True
    return False


def should_run_vertica_adgroup_daily_metadata_task(vertica_cluster_id: str, **kwargs):
    if IsVerticaEnabled(mssql_conn_id="lwdb", sproc_arguments={"verticaClusterId": vertica_cluster_id}):
        logging.info("Should run the task when vertica cluster is enabled")
        return True
    is_end_of_day = datetime.utcnow().time() > time(22, 0) and datetime.utcnow().time() < time(23, 59)
    if is_end_of_day:
        logging.info("Should run the task when it's end of the day")
        return True
    task_hasnt_succeeded_for_too_long = task_not_succeeded_for_too_long(**kwargs)
    if task_hasnt_succeeded_for_too_long:
        logging.info("Should run the task if it hasn't succeeded for too long")
        return True
    return False


for task in vertica_adgroup_daily_metadata_tasks:
    dag = task.create_dag()
    base_task_id = TaskServiceOperator.format_task_name(task.task_name, task.task_name_suffix)
    should_run_task = ShortCircuitOperator(
        task_id='should_run_vertica_adgroup_daily_metadata_task',
        python_callable=should_run_vertica_adgroup_daily_metadata_task,
        op_kwargs={
            'vertica_cluster_id': task.vertica_cluster.name,
            'base_task_id': base_task_id
        },
        provide_context=True,
        dag=dag.airflow_dag
    )
    should_run_task >> dag.airflow_dag.get_task(base_task_id)
    registry.global_vars[dag.airflow_dag.dag_id] = dag.airflow_dag
