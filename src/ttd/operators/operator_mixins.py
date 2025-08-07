from abc import ABCMeta
from queue import Queue
from typing import Generator, List

from airflow.models import BaseOperator

from ttd.monads.trye import Try, Success, Failure


class OperatorMixins(BaseOperator, metaclass=ABCMeta):

    def find_upstream_by_type(self, task_type: str) -> Try[BaseOperator]:
        """

        :param task_type: Type of the task usually it's the name of the class that is used to create operator
        :return:
        """
        op_queue: Queue[BaseOperator] = Queue()
        for op in self.upstream_list:
            op_queue.put(op)  # type: ignore

        try:
            while not op_queue.empty():
                operator = op_queue.get_nowait()
                if operator.task_type == task_type:
                    return Success(operator)
                for op in operator.upstream_list:
                    op_queue.put(op)  # type: ignore
        except Exception as ex:
            return Failure(ex)

        Failure(Exception("No operator has been found with specified task_type", task_type))

    def upstream_tasks_until_type(self, stop_at_task_types: List[str]) -> Generator[BaseOperator, None, None]:
        """
        Creates generator that iterates over operators upstream for the current one until reaches one of the specified task types.
        :param stop_at_task_types: List of the task types when iterator needs to stop.
                Usually the class name that used to create operator.
        :return:
        """
        op_queue: Queue[BaseOperator] = Queue()
        for op in self.upstream_list:
            op_queue.put(op)  # type: ignore

        while not op_queue.empty():
            operator = op_queue.get_nowait()
            if operator.task_type in stop_at_task_types:
                return
            for op in operator.upstream_list:
                op_queue.put(op)  # type: ignore
            yield operator

        raise Exception(
            "No operator has been found with specified stop_at_task_types",
            stop_at_task_types,
        )
