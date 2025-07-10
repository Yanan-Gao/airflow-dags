from datetime import datetime

from ttd.eldorado.base import TtdDag
from ttd.eldorado.kubernetes.hdfs_task import HDFSTask
from ttd.kubernetes.k8s_instance_types import K8sInstanceTypes
from ttd.kubernetes.spark_kubernetes_versions import SparkVersions
from ttd.kubernetes.spark_pod_config import SparkPodConfig, SparkExecutorPodConfig, SparkPodResources
from ttd.openlineage import OpenlineageConfig
from ttd.operators.ttd_spark_kubernetes_operator import TtdSparkKubernetesOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import ProdTestEnv

hdfs_task = HDFSTask(job_name="demo-spark-kubernetes-clicktracker-hdfs", num_datanodes=2, size_per_datanode=75, retries=1)

spark_task = OpTask(
    op=TtdSparkKubernetesOperator(
        job_name="demo-spark-kubernetes-clicktracker-hdfs",
        spark_version=SparkVersions.v3_2_1,
        class_name="jobs.dataproc.datalake.ClickTrackerDatalakeEtlPipeline",
        executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar",
        driver_config=SparkPodConfig(
            spark_pod_resources=SparkPodResources(request_cpu="5", cores=5, request_memory="16g", limit_memory="18g"),
            preferred_nodes=[(K8sInstanceTypes.aws_general_purpose(), 50)]
        ),
        executor_config=SparkExecutorPodConfig(
            instances=10,
            pod_config=SparkPodConfig(
                spark_pod_resources=SparkPodResources(
                    request_cpu="5", cores=5, request_memory="16g", memory_overhead="2g", limit_memory="18g"
                ),
                preferred_nodes=[(K8sInstanceTypes.aws_general_purpose(), 40)]
            )
        ),
        java_options=[("coalesceSize", "100"), ("runtime", "2024-09-16T13:00:00")],
        spark_configuration=[("spark.driver.maxResultSize", "2g"), (
            "spark.default.parallelism", "50"
        ), ("spark.sql.parquet.fs.optimized.committer.optimization-enabled",
            "true"), ("spark.eventLog.dir",
                      "hdfs://spark-history-server-hdfs-namenode-svc.spark.svc:8020/user/hadoop/logs"), ("spark.eventLog.enabled", "true")],
        hadoop_configuration=[("mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
                               ), ("fs.s3a.committer.name", "partitioned"), ("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")],
        openlineage_config=OpenlineageConfig(enabled=False),
        environment=ProdTestEnv()
    )
)

hdfs_task.add_parallel_body_task(spark_task)

ttd_dag = TtdDag(
    dag_id="demo-spark-kubernetes-clicktracker-hdfs",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["DATAPROC", "Demo"],
    default_args={"owner": "DATAPROC"},
)

ttd_dag >> hdfs_task

adag = ttd_dag.airflow_dag
