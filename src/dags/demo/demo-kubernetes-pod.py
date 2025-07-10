# This demo DAG uses a connection generated using the tools in local_tools/k8s-pods-scheduling

from ttd.eldorado.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.tasks.op import OpTask

dag = TtdDag(dag_id="demo-run-k8s-pod-in-external-cluster", schedule_interval=None, tags=['Demo', 'DATAPROC'])

kpo = TtdKubernetesPodOperator(
    name='my-pod',
    kubernetes_conn_id='my-conn-id-in-vault',
    namespace="my-namespace",
    image='busybox',
    cmds=['sh', '-c', 'sleep 600'],
    task_id="run_a_pod",
    resources=PodResources(
        request_cpu="1",
        request_memory="200Mi",
        limit_memory="300Mi",
    )
)

task = OpTask(op=kpo)

adag = dag.airflow_dag

dag >> task
