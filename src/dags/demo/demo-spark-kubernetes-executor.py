from datetime import datetime

from ttd.eldorado.base import TtdDag
from ttd.kubernetes.k8s_instance_types import K8sInstanceTypes
from ttd.kubernetes.spark_kubernetes_versions import SparkVersions
from ttd.kubernetes.spark_pod_config import SparkPodConfig, SparkExecutorPodConfig, SparkPodResources
from ttd.openlineage import OpenlineageConfig
from ttd.operators.ttd_spark_kubernetes_operator import TtdSparkKubernetesOperator
from ttd.tasks.op import OpTask

spark_k8s_task = OpTask(
    op=TtdSparkKubernetesOperator(
        job_name="demo-run-spark-task",
        spark_version=SparkVersions.v3_2_1,
        executable_path="local:///opt/spark/examples/jars/spark-examples_2.12-3.2.1.jar",
        class_name="org.apache.spark.examples.SparkPi",
        driver_config=SparkPodConfig(
            spark_pod_resources=SparkPodResources(request_cpu="900m", request_memory="1g", limit_memory="1.5g"),
            preferred_nodes=[(K8sInstanceTypes.aws_general_purpose(), 50)]
        ),
        executor_config=SparkExecutorPodConfig(
            instances=1,
            pod_config=SparkPodConfig(
                spark_pod_resources=SparkPodResources(request_cpu="1", request_memory="1g", limit_memory="1.5g"),
                preferred_nodes=[(K8sInstanceTypes.aws_compute_optimised(), 40)]
            )
        ),
        openlineage_config=OpenlineageConfig(enabled=False)
    )
)

ttd_dag = TtdDag(
    dag_id="demo-spark-kubernetes-executor",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["DATAPROC", "Demo"],
    default_args={"owner": "DATAPROC"},
)

ttd_dag >> spark_k8s_task

adag = ttd_dag.airflow_dag
