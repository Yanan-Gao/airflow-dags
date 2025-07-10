import copy
from datetime import datetime

import airflow
import re

from airflow.executors.executor_loader import ExecutorLoader

from airflow.models import BaseOperator
from airflow.operators.subdag import SubDagOperator

from ttd.docker import (
    DockerRunEmrTask,
    PySparkEmrTask,
    ExtendedEmrTaskVisitor,
)
from ttd.eldorado.base import TtdDag, SlackConfiguration
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.base import BaseTask
from airflow.sensors.external_task import ExternalTaskSensor

from ttd.eldorado.aws.emr_job_task import EmrJobTask
from typing import List, Optional, Dict, Set

from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.ttd_emr_add_steps_operator import TtdEmrAddStepsOperator


def gather_nodes_to_remove(node: BaseOperator, nodes_to_remove: Set[str], task_pattern: str):
    if node.task_id.startswith("create_cluster"):
        if task_pattern in node.task_id:
            return
        nodes_to_remove.add(node.task_id)
        print("Removing: ", node.task_id)

    for upstream in node.upstream_list:
        gather_nodes_to_remove(upstream, nodes_to_remove, task_pattern)  # type: ignore

    nodes_to_remove.add(node.task_id)
    return


def delete_nodes_from_dag(subdag: airflow.DAG, nodes_to_remove: Set[str]):
    for node in subdag.tasks:
        for up in nodes_to_remove:
            if up in node.upstream_task_ids:
                node.upstream_task_ids.remove(up)

            if up in node.downstream_task_ids:
                node.downstream_task_ids.remove(up)

    for node in nodes_to_remove:
        try:
            subdag.task_dict.pop(node)  # type: ignore
        except Exception as e:
            pass

    return


def find_first_occurrence(dag: airflow.DAG, pattern):
    for node in dag.topological_sort():
        if re.findall(pattern, node.task_id):
            return node


class BuildBaseTaskMapVisitor(ExtendedEmrTaskVisitor):

    def __init__(self):
        self.task_dict = {}

    @staticmethod
    def build(dag: TtdDag) -> Dict[str, BaseTask]:
        visitor = BuildBaseTaskMapVisitor()
        for d in dag.downstream:
            d.accept(visitor)

        return visitor.task_dict

    def visit_base_task(self, node: "BaseTask"):
        self.task_dict[node.task_id] = node

        for downstream in node.downstream:
            downstream.accept(self)

    def visit_chain_of_tasks(self, node: "ChainOfTasks"):
        self.task_dict[node.task_id] = node

        node.first.accept(self)
        node.last.accept(self)

        for downstream in node.downstream:
            downstream.accept(self)


class CollectDescendantsVisitor(ExtendedEmrTaskVisitor):

    def __init__(self):
        self.collected = []

    @staticmethod
    def gather_descendants_from_node(node: BaseTask) -> List[BaseTask]:
        visitor = CollectDescendantsVisitor()

        for n in node.downstream:
            n.accept(visitor)

        return visitor.collected

    def visit_base_task(self, node: "BaseTask"):
        self.collected.append(node)

        for n in node.downstream:
            n.accept(self)

    def visit_chain_of_tasks(self, node: "ChainOfTasks"):
        self.collected.append(node)

        node.first.accept(self)
        node.last.accept(self)

        for n in node.downstream:
            n.accept(self)


class JarPath:

    def __init__(self, jar_path: str):
        self.jar_path = jar_path


class DockerImage:

    def __init__(
        self,
        docker_registry: str = "internal.docker.adsrvr.org",
        docker_image_tag: str = "latest",
    ):
        self.docker_registry = docker_registry
        self.docker_image_tag = docker_image_tag


class ExecutableOverrideConfiguration:

    def __init__(self):
        self._jar = None
        self._docker_image = None

    @property
    def jar(self) -> Optional[str]:
        return self._jar

    @property
    def docker_image(self) -> Optional[DockerImage]:
        return self._docker_image

    def set_overall_jar_override(self, jar: str) -> "ExecutableOverrideConfiguration":
        self._jar = jar
        return self

    def set_docker_image_override(self, command: DockerImage) -> "ExecutableOverrideConfiguration":
        self._docker_image = command
        return self


class DagForkException(Exception):
    pass


def rewrite_executable_path(
    original_node: EmrJobTask,
    forked_dag: airflow.DAG,
    updated_executables: ExecutableOverrideConfiguration,
    dataset_overrides: Dict[str, List[str]],
):
    """
    This will take an existing node, and then find the equivalent node in the associated forked dag, and rewrite
    the operator with the overrides given in `updated_executables` and `dataset_overrides`, such that those nodes
    have the additional arguments needed.

    :param original_node:
    :param forked_dag:
    :param updated_executables:
    :param dataset_overrides:
    :return:
    """
    copied = copy.copy(original_node)

    if (isinstance(copied, DockerRunEmrTask) and updated_executables.docker_image is not None):
        # Apply updated information for docker
        copied.docker_run_builder.docker_registry = (updated_executables.docker_image.docker_registry)
        copied.docker_run_builder.docker_image_tag = (updated_executables.docker_image.docker_image_tag)

        copied.command_line_arguments = copied.docker_run_builder.build_command()

        spark_submit_args = copied._build_spark_step_args()
        formatted_steps = copied._format_spark_step(spark_submit_args)

        add_step_task = forked_dag.get_task(copied.first_airflow_op().task_id)

        if isinstance(add_step_task, TtdEmrAddStepsOperator):
            add_step_task.steps = formatted_steps

    elif isinstance(copied, PySparkEmrTask):
        raise DagForkException("Forking a node with a pyspark EMR task is currently not supported")

    # Standard jar, this should work just fine
    elif isinstance(copied, EmrJobTask) and updated_executables.jar is not None:
        copied.executable_path = updated_executables.jar

        copied.eldorado_config_option_pairs_list = ([] if copied.eldorado_config_option_pairs_list is None else
                                                    copied.eldorado_config_option_pairs_list)

        # Override the configuration for emr job tasks
        if isinstance(copied.eldorado_config_option_pairs_list, list):
            for exp, datasets in dataset_overrides.items():
                pairs = [(ds + ".experiment", exp) for ds in datasets]
                copied.eldorado_config_option_pairs_list.extend(pairs)

        spark_submit_args = copied._build_spark_step_args()
        formatted_steps = copied._format_spark_step(spark_submit_args)

        add_step_task = forked_dag.get_task(copied.first_airflow_op().task_id)

        if isinstance(add_step_task, TtdEmrAddStepsOperator):
            add_step_task.steps = formatted_steps


def update_slack_information(dag: TtdDag, slack_configuration_overrides: SlackConfiguration, subdag: airflow.DAG):
    subdag_slack_config = copy.deepcopy(dag.slack_configuration).__dict__
    subdag_slack_config.update(slack_configuration_overrides.__dict__)
    overrides = SlackConfiguration(**subdag_slack_config)
    # Update the subdag with new slack information
    subdag.on_failure_callback = overrides.into_error_callback(subdag.dag_id)
    subdag.on_success_callback = overrides.into_success_callback()


def create_sub_dag(dag, experiment_name, pattern) -> airflow.DAG:
    # Walk through and grab the sub dag operators in order
    subdag_executors_in_topological_order = []
    for node in dag.airflow_dag.topological_sort():
        if isinstance(node, SubDagOperator):
            subdag_executors_in_topological_order.append(node.executor)  # type: ignore
            node.executor = None

    # Reverse the list and pop from the back - we could just use a deque here but
    # this is most likely a list of 1 or 2 elements.
    subdag_executors_in_topological_order.reverse()
    subdag = dag.airflow_dag.sub_dag(pattern, include_downstream=True)

    for node in dag.airflow_dag.topological_sort():
        if isinstance(node, SubDagOperator):
            executor = subdag_executors_in_topological_order.pop()
            node.executor = executor

    subdag.dag_id = subdag.dag_id + "_experiment_" + experiment_name

    for node in subdag.topological_sort():
        if isinstance(node, SubDagOperator):
            node.dag = subdag
            node.subdag.dag_id = node.dag.dag_id + "." + node.task_id
            globals()[node.subdag.dag_id] = node.subdag
            node.executor = ExecutorLoader.get_default_executor()

    return subdag


def create_experiment_dag_from_node(
    dag: TtdDag,
    task_id: str,
    experiment_name: str,
    slack_configuration_overrides: SlackConfiguration,
    jar_path_or_container: ExecutableOverrideConfiguration,
    dataset_overrides: Dict[str, List[str]],
    exclude_nodes=None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Optional[airflow.DAG]:
    """
    This will return a new dag that forks from the given task id, and then directly depends on the nodes that
    it needs to above. Consider the following:

        A
        |
        B
       / \
      C  D

    If we were to fork at node B - we could share A still, so if we decided to fork at B, we should generate a
    new dag that looks like this:

     Wait-for-A
         |
         B
        / \
       C  D

    The overrides passed down will apply to that node and all children of that node.

    :param dag:
    :param task_id:
    :param experiment_name:
    :param slack_configuration_overrides:
    :param jar_path_or_container:
    :param dataset_overrides:
    :param exclude_nodes:
    :param start_date:
    :param end_date:
    :return:
    """
    if exclude_nodes is None:
        exclude_nodes = set()

    # Use this to recover original nodes
    original_task_map = BuildBaseTaskMapVisitor.build(dag)

    pattern = re.compile(task_id)

    # Walk through and unassign the executor if it is a subdag, and then otherwise reassign post walking to a fresh
    # instance of an executor
    subdag = create_sub_dag(dag, experiment_name, pattern)

    nodes_to_remove = set()

    for airflow_dag_id, node in dag.airflow_dag.task_dict.items():
        for removals in exclude_nodes:
            if removals in airflow_dag_id:
                nodes_to_remove.add(airflow_dag_id)

    # Go to the node that we'd like to fork at, and walk upstream to purge any nodes that we don't want
    # In this case, we're going to keep all nodes that create clusters
    forked_node = find_first_occurrence(subdag, pattern)
    ttd_node = original_task_map[task_id]

    final_dag_status_node = None
    for node in subdag.tasks:
        if isinstance(node, FinalDagStatusCheckOperator):
            final_dag_status_node = node

    # At the moment, the EmrJobTask is the only kind of node that we're capable of rewriting
    # Forking in other instances... isn't particularly helpful.
    if isinstance(ttd_node, EmrJobTask):
        for upstream in forked_node.upstream_list:
            gather_nodes_to_remove(upstream, nodes_to_remove, ttd_node.cluster_config.cluster_name)

    else:
        return None

    # Purge nodes that we no longer want
    delete_nodes_from_dag(subdag, nodes_to_remove)

    roots = subdag.roots

    # Setup upstream nodes
    original_node = find_first_occurrence(dag.airflow_dag, pattern)

    for upstream in original_node.upstream_list:
        wait_for_upstream_node = ExternalTaskSensor(
            task_id=f"wait-for-upstream-{upstream.task_id}",
            external_dag_id=upstream.dag_id,
            external_task_id=upstream.task_id,
            allowed_states=["success"],
            dag=subdag,
        )

        for node in roots:
            if node.task_id.startswith("create_cluster") or node.task_id.startswith("select_subnets"):
                print("Setting downstream node: ", node.task_id)
                wait_for_upstream_node.set_downstream(node)
                break

    update_slack_information(dag, slack_configuration_overrides, subdag)

    # Walk the rest of the dag, rewriting everything BELOW our current node
    rewrite_executable_path(ttd_node, subdag, jar_path_or_container, dataset_overrides)

    downstream_nodes = CollectDescendantsVisitor.gather_descendants_from_node(ttd_node)

    for node in downstream_nodes:
        if isinstance(node, EmrJobTask):
            # Ignore the ones we explicitly asked to skip
            if node.task_id in exclude_nodes:
                continue

            rewrite_executable_path(node, subdag, jar_path_or_container, dataset_overrides)

    if final_dag_status_node:
        for leaf in subdag.leaves:
            # Don't accidentally create a cycle
            if final_dag_status_node.task_id != leaf.task_id:
                leaf >> final_dag_status_node

    # Overwrite the start and end dates if we have them
    if start_date:
        subdag.start_date = start_date

    if end_date:
        subdag.end_date = end_date

    return subdag
