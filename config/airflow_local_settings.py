from ttd.cluster_policies.dag_policy.dag_owner import assign_dag_owner
from ttd.cluster_policies.dag_policy.dag_tags import add_dag_tags
from airflow.models import DAG


def dag_policy(dag: DAG):
    assign_dag_owner(dag)
    add_dag_tags(dag)
