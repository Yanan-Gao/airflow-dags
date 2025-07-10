from typing import Optional

from ttd.operators.create_hdfs_operator import CreateHDFSOperator
from ttd.operators.terminate_hdfs_operator import TerminateHDFSOperator
from ttd.operators.ttd_spark_kubernetes_operator import TtdSparkKubernetesOperator
from ttd.tasks.base import BaseTask
from ttd.tasks.op import OpTask
from ttd.tasks.setup_teardown import SetupTeardownTask


class HDFSTask(SetupTeardownTask):

    def __init__(
        self,
        job_name: str,
        num_datanodes: int,
        size_per_datanode: int,
        max_datanodes: Optional[int] = None,
        conn_id: str = "kubernetes_default",
        **kwargs
    ):
        self.num_datanodes = num_datanodes
        self.size_per_datanode = size_per_datanode
        self.max_datanodes = max_datanodes

        self.create_operator = CreateHDFSOperator(
            job_name=job_name, num_datanodes=num_datanodes, size_datanodes=size_per_datanode, conn_id=conn_id
        )

        setup_task = OpTask(op=self.create_operator)
        teardown_task = OpTask(op=TerminateHDFSOperator(job_name=job_name, conn_id=conn_id))
        super().__init__(task_id=f"{job_name}-hdfs-cluster", setup_task=setup_task, teardown_task=teardown_task, **kwargs)

    def add_parallel_body_task(self, body_task: BaseTask) -> "SetupTeardownTask":
        super().add_parallel_body_task(body_task)

        if isinstance(body_task, OpTask):
            op = body_task.first_airflow_op()
            if isinstance(op, TtdSparkKubernetesOperator):
                op.enable_hdfs()
                self.create_operator.hadoop_version = op.spark_version.hadoop_version

        return self
