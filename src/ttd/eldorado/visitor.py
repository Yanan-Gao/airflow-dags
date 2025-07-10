import abc

from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from ttd.tasks.subdag import SubDagTaskDeprecated
    from ttd.tasks.chain import ChainOfTasks
    from ttd.tasks.op import OpTask
    from ttd.tasks.base import BaseTask
    from ttd.tasks.setup_teardown import SetupTeardownTask


class AbstractVisitor(abc.ABC):

    @abc.abstractmethod
    def visit_base_task(self, node: "BaseTask"):
        pass

    def visit_optask(self, node: "OpTask"):
        self.visit_base_task(node)

    def visit_chain_of_tasks(self, node: "ChainOfTasks"):
        for task in node._tasks:
            self.visit_base_task(task)

    def visit_subdag(self, node: "SubDagTaskDeprecated"):
        self.visit_base_task(node)

    def visit_setup_teardown_task(self, node: "SetupTeardownTask"):
        for task in node.tasks:
            self.visit_base_task(task)


class CollectOpTasksVisitor(AbstractVisitor):

    def __init__(self):
        self.ops: List[OpTask] = []

    @staticmethod
    def gather_op_tasks_from_node(node: "BaseTask") -> List["OpTask"]:
        visitor = CollectOpTasksVisitor()

        node.accept(visitor)

        return visitor.ops

    def visit_base_task(self, node: "BaseTask"):
        node.accept(self)

    def visit_optask(self, node: "OpTask"):
        self.ops.append(node)
