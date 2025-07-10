# This file is a fork of the AirflowGenerator of TTD.Tools.RtiPipeBuilderGenerator tool
from dags.dprpts.agiles.pipe_builder.tasks_dag_builder import (
    TaskBuilder,
    FlowDagBuilder,
)
from ttd.eldorado.base import TtdDag
from ttd.task_service.vertica_clusters import (
    get_variants_for_group,
    TaskVariantGroup,
)


# running aggregation + export on ETL clusters, import - on UI cluster
def build_flow_dag(dag: TtdDag, task_builder: TaskBuilder) -> None:
    dag >> task_builder.create_aggregation_task(
        phase_origin_id="RtiPipeClickHouseBatchImport",
        depends_on="FinalAggregationAndParquetExport",
        max_batches_per_run=8,
    )


for aggregation_vertica_cluster in (get_variants_for_group(TaskVariantGroup.VerticaAws)):
    flow_dag = FlowDagBuilder(
        flow_name='click-house-agiles',
        batch_type_id=1,
        vertica_cluster=aggregation_vertica_cluster,
        phase_origin_types=["Incremental", "TrueUp", "DataCorrection", "LateData"],
        batch_lookback_days=30,
    ).build(lambda dag, task_builder: build_flow_dag(dag, task_builder))

    globals()[flow_dag.airflow_dag.dag_id] = flow_dag.airflow_dag
