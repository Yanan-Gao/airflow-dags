from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters
from dags.idnt.statics import Executables

from datasources.datasources import Datasources
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask

from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from typing import List


class BidStreamDailyAgg():
    """Process bidfeedback, request, conversion daily for downstream uses.

    This aims to create a dataset similar in format to avails v2.
    """

    upstream_datasets = [Datasources.rtb_datalake.rtb_bidrequest_v5, Datasources.rtb_datalake.rtb_bidfeedback_v5]

    def __init__(self, dag: TtdDag):
        self.dag = dag

    def _name(self) -> str:
        return type(self).__name__

    def _check_dependencies(self) -> List[OpTask]:
        if len(self.upstream_datasets) > 0:
            return [DagHelpers.check_datasets(self.upstream_datasets, suffix=self._name())]
        else:
            return []

    def _get_clusters(self) -> List[EmrClusterTask]:
        cluster = IdentityClusters.get_cluster(self._name(), self.dag, 7000)
        cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.BidStreamDailyAgg")))
        return [cluster]

    def build(self) -> ChainOfTasks:

        check_tasks = self._check_dependencies()
        process_tasks = self._get_clusters()

        return ChainOfTasks(
            task_id=f"check_and_process_{self._name()}",
            tasks=check_tasks + process_tasks,
        ).as_taskgroup(f"process_{self._name()}")
