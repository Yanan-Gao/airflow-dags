from datetime import datetime, timedelta
from ttd.eldorado.base import TtdDag

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.tasks.op import OpTask
from ttd.tasks.python import PythonOperator
from ttd.ttdenv import TtdEnvFactory
from ttd.kubernetes.pod_resources import PodResources
import logging
from airflow.models.taskinstance import TaskInstance

schedule_interval = None
max_active_runs = 5

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = timedelta(hours=1)
    max_active_runs = 1

    namespace = 'airflow2-k8s-workloads'
    connection_id = 'airflow-pod-scheduling-prod'
elif TtdEnvFactory.get_from_system() == TtdEnvFactory.staging:
    namespace = 'airflow2-staging-k8s-workloads'
    connection_id = 'airflow-pod-scheduling-staging'
else:
    namespace = 'airflow2-test-k8s-workloads'
    connection_id = 'airflow-pod-scheduling-test'

value_to_push = "XCOM_WORKING"

dag = TtdDag(
    dag_id="airflow-canary-k8s-xcom",
    schedule_interval=schedule_interval,
    max_active_runs=max_active_runs,
    tags=["Canary", "Regression", "Monitoring", "Operations"],
    run_only_latest=True,
    start_date=datetime(2024, 12, 6),
    depends_on_past=False,
    retries=2,
    retry_delay=timedelta(minutes=5),
)

push_value_cmd = ("mkdir -p /airflow/xcom/;"
                  f"echo '\"{value_to_push}\"' > /airflow/xcom/return.json")

kpo_push_task = OpTask(
    op=TtdKubernetesPodOperator(
        name="write-xcom",
        task_id="write-xcom",
        namespace=namespace,
        kubernetes_conn_id=connection_id,
        do_xcom_push=True,
        image="proxy.docker.adsrvr.org/alpine",
        cmds=["sh", "-c", push_value_cmd],
        resources=PodResources(
            request_cpu="250m",
            request_memory="128Mi",
            limit_memory="256Mi",
            limit_ephemeral_storage="512Mi",
        )
    )
)


def pull_xcom(task_instance: TaskInstance | None = None):
    logger = logging.getLogger(__name__)
    if task_instance is None:
        raise ValueError("XCOM not working. Operator did not receive task_instance object")
    value = task_instance.xcom_pull(task_ids='write-xcom')
    if value != value_to_push:
        raise ValueError(f"XCOM not working. Wrong value received: {value}")
    logger.info(f"Value received successfully: {value}")


py_pull_task = OpTask(op=PythonOperator(
    task_id="pull-xcom",
    python_callable=pull_xcom,
))

adag = dag.airflow_dag

dag >> kpo_push_task >> py_pull_task
