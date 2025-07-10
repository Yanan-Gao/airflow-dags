"""
# DEPRECATED! Lives in iav2graph-generator
"""

from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters
from datetime import datetime

uid2_graph_stats_dag = DagHelpers.identity_dag(dag_id="uid2-graph-stats", schedule_interval="0 0 * * FRI", start_date=datetime(2024, 6, 21))

cluster = IdentityClusters.get_cluster("Uid2GraphStats", uid2_graph_stats_dag, 384)

cluster.add_parallel_body_task(IdentityClusters.task(class_name="jobs.identity.uid2.stats.Uid2GraphStats"))

uid2_graph_stats_dag >> cluster
uid2_graph_stats_final_dag = uid2_graph_stats_dag.airflow_dag
