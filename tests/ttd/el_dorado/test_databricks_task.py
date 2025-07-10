import unittest
from datetime import timedelta

import pendulum

from ttd.eldorado.base import TtdDag
from ttd.eldorado.databricks.workflow import DatabricksWorkflow


class DatabricksTaskTest(unittest.TestCase):

    def test_no_reserved_cluster_tags(self):
        dag: TtdDag = TtdDag(
            dag_id="test_dag",
            start_date=pendulum.today('UTC').add(days=-1),
            schedule_interval=timedelta(days=1),
            max_active_runs=1,
            slack_channel="none",
            retries=1,
            retry_delay=timedelta(minutes=10),
        )

        task = DatabricksWorkflow(
            "job_name", "cluster_name", {"Team": "DATAPROC"}, "worker_node_type", 1, databricks_spark_version="3.5", tasks=[]
        )

        dag >> task
        tags = task.create_and_run_job_op.json["job_clusters"][0]["new_cluster"]["custom_tags"]
        self.assertFalse(tags.__contains__("ClusterName"))
