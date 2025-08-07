from datetime import timedelta
from json import JSONEncoder
from typing import List, Optional, Callable

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.task_service.vertica_clusters import VerticaCluster
from dags.dprpts.agiles.pipe_builder.default_args import default_dag_args, default_task_args
import re

from ttd.timetables.delayed_timetable import DelayedIntervalSpec

flow_dag_prefix = "ts-rti-pipe-builder"
consistency_dag_prefix = "ts-rti-pipe-builder"
consistency_dag_suffix = "consistency-check"

default_batch_lookback_days = 7
default_max_batches_per_run = 10
default_task_execution_timeout = timedelta(hours=3)

vertica_ui_cluster_association = {
    VerticaCluster.USEast01: VerticaCluster.USEast02_UI,
    VerticaCluster.USWest01: VerticaCluster.USWest02_UI,
}


def get_pipeline_suffix(vertica_cluster: VerticaCluster) -> str:
    return re.sub(r'\d{2}$', '', vertica_cluster.name)


def create_flow_dag(flow_name: str, vertica_cluster: VerticaCluster) -> TtdDag:
    pipeline_suffix = get_pipeline_suffix(vertica_cluster)
    return TtdDag(
        TaskServiceOperator.format_task_name(f"{flow_dag_prefix}-{flow_name}", pipeline_suffix),
        schedule_interval=DelayedIntervalSpec(interval=timedelta(minutes=15), max_delay=timedelta(minutes=10)),
        max_active_runs=1,
        **default_dag_args,
    )


class TaskBuilder:

    def __init__(
        self,
        batch_type_id: int,
        vertica_cluster: VerticaCluster,
        phase_origin_types: List[str],
        batch_lookback_days: int,
        branch_name: Optional[str] = None,
    ):
        self._batch_type_id = batch_type_id
        self._vertica_cluster = vertica_cluster
        self._phase_origin_types = phase_origin_types
        self._batch_lookback_days = batch_lookback_days
        self._branch_name = branch_name

    def create_phase_task(
        self,
        vertica_cluster: VerticaCluster,
        phase_origin_id: str,
        depends_on: List[str],
        batch_origin_cluster: Optional[VerticaCluster] = None,
        max_batches_per_run: int = default_max_batches_per_run,
        task_execution_timeout: timedelta = default_task_execution_timeout,
    ) -> OpTask:
        # getting cluster name until digits and prefix it with '-': USEast01 -> -useast
        # also replaces cn to us as we only need one type of secrets CNEast01 -> -useast
        secret_suffix = "-" + re.sub(r'[a-z][a-z]([a-z]+)\d+(_ui)?', 'us\\1', vertica_cluster.name.lower())

        task_args = {
            "task_name": "RtiPipeBuilderPhaseTask",
            "vertica_cluster": vertica_cluster,
            "task_name_suffix": phase_origin_id,
            "task_execution_timeout": task_execution_timeout,
            "task_concurrency": 1,
            "configuration_overrides": {
                "RtiPipePhaseTaskConfig.ExportBatchType": str(self._batch_type_id),
                "RtiPipePhaseTaskConfig.OverrideConfigViaSecret": "test/use-dataops003-credentials" + secret_suffix,
                "UsePrefixedConfigurationForAwsKeys": "true",
            },
            "telnet_commands": [
                f"try changeField RtiPhaseTrackingQueryProviderUtility.BatchLookbackInDays {self._batch_lookback_days}",
            ],
            "branch_name": self._branch_name,
            **default_task_args,
        }

        if vertica_cluster == VerticaCluster.USWest02_UI:
            task_args["telnet_commands"].append(
                "try changeField VerticaCloudExportImportHelper.EnableCommandForVertica12West02Ui.Enabled true"
            )

        if vertica_cluster == VerticaCluster.USEast02_UI:
            task_args["telnet_commands"].append(
                "try changeField VerticaCloudExportImportHelper.EnableCommandForVertica12East02Ui.Enabled true"
            )

        op_task = OpTask(
            op=TaskServiceOperator(
                task_data=JSONEncoder().encode({
                    "PhaseOriginId": phase_origin_id,
                    "DependsOn": depends_on,
                    "MaxBatchesPerRun": max_batches_per_run,
                    "BatchOriginClusters": None if batch_origin_cluster is None else [batch_origin_cluster.name],
                    "PhaseOriginTypes": self._phase_origin_types,
                }),
                **task_args
            )
        )

        return op_task

    def create_aggregation_task(
        self,
        phase_origin_id: str,
        depends_on: str,
        max_batches_per_run: int = default_max_batches_per_run,
        task_execution_timeout: timedelta = default_task_execution_timeout,
    ) -> OpTask:
        return self.create_phase_task(
            vertica_cluster=self._vertica_cluster,
            phase_origin_id=phase_origin_id,
            depends_on=[depends_on],
            max_batches_per_run=max_batches_per_run,
            task_execution_timeout=task_execution_timeout,
        )

    def create_export_task(self, phase_origin_id: str, depends_on: str) -> OpTask:
        return self.create_phase_task(
            vertica_cluster=self._vertica_cluster,
            phase_origin_id=phase_origin_id,
            depends_on=[depends_on],
        )

    def create_import_task(self, phase_origin_id: str, depends_on: str) -> OpTask:
        return self.create_phase_task(
            vertica_cluster=vertica_ui_cluster_association[self._vertica_cluster],
            batch_origin_cluster=self._vertica_cluster,
            phase_origin_id=phase_origin_id,
            depends_on=[depends_on],
        )


class FlowDagBuilder:

    def __init__(
        self,
        flow_name: str,
        batch_type_id: int,
        vertica_cluster: VerticaCluster,
        phase_origin_types: List[str],
        batch_lookback_days: int = default_batch_lookback_days,
        branch_name: Optional[str] = None,
    ):
        self._flow_name = flow_name
        self._batch_type_id = batch_type_id
        self._vertica_cluster = vertica_cluster
        self._phase_origin_types = phase_origin_types
        self._batch_lookback_days = batch_lookback_days
        self._branch_name = branch_name

    def build(self, flow_dag_builder: Callable[[TtdDag, TaskBuilder], None]) -> TtdDag:
        task_builder = TaskBuilder(
            batch_type_id=self._batch_type_id,
            vertica_cluster=self._vertica_cluster,
            phase_origin_types=self._phase_origin_types,
            batch_lookback_days=self._batch_lookback_days,
            branch_name=self._branch_name
        )

        flow_dag = create_flow_dag(self._flow_name, self._vertica_cluster)
        flow_dag_builder(flow_dag, task_builder)

        return flow_dag
