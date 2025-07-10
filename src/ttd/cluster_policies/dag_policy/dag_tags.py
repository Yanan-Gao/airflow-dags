from airflow.models import DAG
from typing import Set
from ttd.cluster_policies.dag_policy.dag_owner import fetch_dag_owner_from_filestructure


def get_operator_tags_from_dag(dag: DAG) -> Set[str]:

    class_to_tags = {
        'TtdEmrCreateJobFlowOperator': 'svc:EMR',
        'HDICreateClusterOperator': 'svc:HDI',
        'AliCreateClusterOperator': 'svc:Ali-EMR',
        'TaskServiceOperator': 'svc:TaskService',
        'TtdKubernetesPodOperator': 'svc:k8s',
        'TtdSparkKubernetesOperator': 'svc:k8s-Spark',
        'DatabricksCreateJobsOperator': 'svc:Databricks',
    }

    cloud_provider_tag_map = {'aws': 'ts-provider:AWS', 'azure': 'ts-provider:Azure', 'alicloud': 'ts-provider:AliCloud'}

    op_tags = set()
    for task in dag.tasks:
        task_class_name = task.__class__.__name__
        if task_class_name == 'TaskServiceOperator':
            cloud_provider = str(task.k8s_sovereign_connection_helper.cloud_provider)  # type: ignore

            if cloud_provider in cloud_provider_tag_map:
                op_tags.add(cloud_provider_tag_map[cloud_provider])

        if task_class_name == 'DatabricksCreateJobsOperator':
            task_configs: list[dict] = task.json['tasks']  # type: ignore
            if any('notebook_task' in task_config for task_config in task_configs):
                op_tags.add('res:Notebook')

        if tag := class_to_tags.get(task_class_name, None):
            op_tags.add(tag)
    return op_tags


def add_dag_tags(dag: DAG) -> None:
    new_tags = get_operator_tags_from_dag(dag)

    # Currently set up to be called after assign_dag_owner in dag_policy function.
    if dag.tasks and dag.tasks[-1].owner:
        team = dag.owner
    else:
        team = fetch_dag_owner_from_filestructure(dag)

    # Filter out existing team tag
    dag.tags = [t for t in dag.tags if t.lower() != team.lower()]

    new_tags.add(f"team:{team}")

    dag.tags.extend(new_tags)
