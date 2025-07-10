from datetime import datetime

from ttd.eldorado.base import TtdDag
from ttd.kubernetes.k8s_instance_types import K8sInstanceTypes
from ttd.kubernetes.spark_kubernetes_versions import SparkVersions
from ttd.kubernetes.spark_pod_config import SparkPodConfig, SparkPodResources, SparkExecutorPodConfig
from ttd.openlineage import OpenlineageConfig
from ttd.operators.ttd_spark_kubernetes_operator import TtdSparkKubernetesOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import ProdTestEnv

EXECUTABLE_PATH = "s3://ttd-build-artefacts/eldorado/mergerequests/hjp-DATAPROC-4287-disable-hdfs-consent-string/latest/eldorado-dataproc-assembly.jar"

spark_task = OpTask(
    op=TtdSparkKubernetesOperator(
        job_name="demo-spark-on-k8s-consentstring-task",
        spark_version=SparkVersions.v3_2_1,
        executable_path=EXECUTABLE_PATH,
        class_name="jobs.dataproc.datalake.DatalakeConsentStringFeedJob",
        driver_config=SparkPodConfig(
            spark_pod_resources=SparkPodResources(request_cpu="1", request_memory="2g", limit_memory="3g"),
            preferred_nodes=[(K8sInstanceTypes.aws_general_purpose(), 40)]
        ),
        executor_config=SparkExecutorPodConfig(
            instances=20,
            pod_config=SparkPodConfig(
                spark_pod_resources=SparkPodResources(request_cpu="4", request_memory="5g", memory_overhead="2g", limit_memory="7g"),
                preferred_nodes=[(K8sInstanceTypes.aws_compute_optimised(), 40)]
            )
        ),
        openlineage_config=OpenlineageConfig(enabled=False),
        java_options=[("dataTime", "2024-08-23T09:30:00")],
        # TODO - not all of these arguments should be necessary.
        spark_configuration=[("spark.driver.maxResultSize", "2g"), ("spark.dynamicAllocation.shuffleTracking",
                                                                    "true"), ("spark.default.parallelism", "90")],
        hadoop_configuration=[("mapreduce.outputcommitter.factory.scheme.s3a",
                               "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"), ("fs.s3a.committer.name", "partitioned")],
        environment=ProdTestEnv()
    )
)

ttd_dag = TtdDag(
    dag_id="demo-spark-on-k8s-eldorado-consentstring",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["DATAPROC", "Demo"],
    default_args={"owner": "DATAPROC"}
)

ttd_dag >> spark_task

adag = ttd_dag.airflow_dag
