import time
from datetime import datetime

from airflow.operators.python import PythonOperator

from ttd.workers.worker import Workers
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.kubernetes.pod_resources import PodResources
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask


def print_hello():
    time.sleep(30)
    print("I'm running in my own Kubernetes pod!")


# `queue="k8s_executor"` causes the task to run in separate Kubernetes pod.
# You must provide an `executor_config`. For long-lived watch tasks you can use `K8sExecutorConfig.watch_task()`:
demo_watch_task = OpTask(
    op=PythonOperator(
        task_id="demo-watch-task",
        python_callable=print_hello,
        queue=Workers.k8s.queue,
        pool=Workers.k8s.pool,
        executor_config=K8sExecutorConfig.watch_task(),
    )
)

# You can also provide custom resources using the PodResources class as follows:
task_with_custom_resources = OpTask(
    op=PythonOperator(
        task_id="demo-custom-resources-task",
        python_callable=print_hello,
        queue=Workers.k8s.queue,
        pool=Workers.k8s.pool,
        executor_config=PodResources(
            request_cpu="1",
            request_memory="200Mi",
            limit_memory="300Mi",
        ).as_executor_config(),
    )
)

ttd_dag = TtdDag(
    dag_id="demo-kubernetes-executor",
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    tags=["DATAPROC", "Demo"],
)

ttd_dag >> demo_watch_task >> task_with_custom_resources

adag = ttd_dag.airflow_dag
