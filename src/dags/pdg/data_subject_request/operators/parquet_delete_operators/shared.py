from abc import ABC, abstractmethod
from typing import Dict, Sequence, Tuple

from ttd.tasks.base import BaseTask
from ttd.ttdenv import TtdEnvFactory

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

DEFAULT_ARGUMENTS_DSR_PROCESSING_JOB = {"dryRun": "false" if is_prod else "true"}

XCOM_PULL_STR = "{{{{task_instance.xcom_pull(dag_id='{dag_id}',task_ids='{task_id}', key='{key}')}}}}"


class ProcessingMode(ABC):

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def to_args(self) -> Dict[str, str]:
        pass


class DSR(ProcessingMode):

    def __init__(self, uiids: str) -> None:
        super().__init__()
        self.uiids = uiids

    def to_args(self) -> Dict[str, str]:
        return {"uiids": self.uiids}


class IParquetDeleteOperation(ABC):

    def __init__(
        self,
        dataset_configs,
        job_class_name,
        dsr_request_id: str = None,
        mode: ProcessingMode = None,
        cluster_name="datagov-dsr-delete",
        additional_args=None
    ):
        self.dsr_request_id = dsr_request_id
        self.job_class_name = job_class_name
        self.mode = mode
        self.dataset_configs = dataset_configs
        self.cluster_name = cluster_name
        self.additional_args = additional_args

    def create_parquet_delete_job_tasks(self) -> Sequence[BaseTask]:
        """
        Create delete tasks for all parquet dataset defined in dataset_configs. All tasks will be
        parallel downstream task of upstream_task.

        @param parent_dag_id Parent DAG id
        @param uiids UIIDs to delete from datasets
        @param upstream_task upstream task
        @return list of delete tasks
        """

        return [self._create_delete_job_task(name) for name in self.dataset_configs.keys()]

    @abstractmethod
    def _create_delete_job_task(self, dataset_name: str) -> BaseTask:
        """
        Create delete job task for a dataset. The job will be one downstream job of upstream_task.

        @param dataset_name Dataset name
        @param parent_dag_id Parent DAG id
        @param uiids UIIDs to delete from datasets
        @param upstream_task upstream task
        @return delete task
        """
        pass

    def _get_arguments_dsr_processing_job(
        self,
        dataset_name: str,
        log_output_path: str,
    ) -> Sequence[Tuple[str, str]]:
        """
        Get configuration options for job, including dataset_name, dsrRequestId and uiids.

        @param dataset_name Dataset name
        @param parent_dag_id Parent DAG id
        @param uiids UIIDs to delete from datasets
        @return option tuples.
        """

        res = {
            **DEFAULT_ARGUMENTS_DSR_PROCESSING_JOB,
            "datasetName": dataset_name,
            "logOutputPath": log_output_path,
        }

        if self.dsr_request_id is not None and self.dsr_request_id != "":
            res["dsrRequestId"] = self.dsr_request_id

        if self.mode is not None and self.mode.to_args() is not None:
            res = {**res, **self.mode.to_args()}
        if self.additional_args is not None:
            res = {**res, **self.additional_args}

        return list(res.items())
