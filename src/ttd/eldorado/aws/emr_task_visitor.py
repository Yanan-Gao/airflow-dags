from __future__ import annotations
import abc
from typing import TYPE_CHECKING

from ttd.eldorado.visitor import AbstractVisitor
if TYPE_CHECKING:
    from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
    from ttd.eldorado.aws.emr_job_task import EmrJobTask


class EmrTaskVisitor(AbstractVisitor, abc.ABC):

    def visit_emr_job_task(self, node: EmrJobTask):
        self.visit_chain_of_tasks(node)

    def visit_emr_cluster_task(self, node: EmrClusterTask):
        self.visit_setup_teardown_task(node)
