# auto-generated
# this file is generated by AirflowGenerator of TTD.Tools.RtiPipeBuilderGenerator tool
from datetime import timedelta
from dags.dprpts.agiles.pipe_builder.tasks_dag_builder import TaskBuilder, FlowDagBuilder
from dags.dprpts.agiles.pipe_builder.consistency_dag_builder import ConsistencyTaskBuilder, ConsistencyDagBuilder
from ttd.eldorado.base import TtdDag
from ttd.task_service.vertica_clusters import (
    get_variants_for_group,
    TaskVariantGroup,
)


# running aggregation + export on ETL clusters, import - on UI cluster
def build_flow_dag(dag: TtdDag, task_builder: TaskBuilder) -> None:
    dag >> task_builder.create_aggregation_task(
        phase_origin_id="VerticaEtlAggregation",
        depends_on="FinalAggregationAndParquetExport",
        max_batches_per_run=30,
        task_execution_timeout=timedelta(hours=4),
    )


# manually redirect Integral checks to proper configs
def build_consistency_dag(dag: TtdDag, task_builder: ConsistencyTaskBuilder) -> None:
    dag >> task_builder.create_consistency_check(
        task_config_name="VerticaEtlRtiAgilesHourlyCheckConfig",
        task_name_suffix="rti-agiles-hourly-true-up",
    )

    dag >> task_builder.create_consistency_check(
        task_config_name="VerticaEtlRtiAgilesDailyCheckConfig",
        task_name_suffix="rti-agiles-daily-true-up",
    )

    dag >> task_builder.create_consistency_check(
        task_config_name="VerticaEtlRtiAgilesMonthlyCheckConfig",
        task_name_suffix="rti-agiles-monthly-true-up",
    )

    dag >> task_builder.create_consistency_check(
        task_config_name="VerticaEtlRtiAgilesHourlyIntegralCheckConfig",
        task_name_suffix="rti-agiles-hourly-integral",
    )

    dag >> task_builder.create_consistency_check(
        task_config_name="VerticaEtlRtiAgilesDailyIntegralCheckConfig",
        task_name_suffix="rti-agiles-daily-integral",
    )

    dag >> task_builder.create_consistency_check(
        task_config_name="VerticaEtlRtiAgilesMonthlyIntegralCheckConfig",
        task_name_suffix="rti-agiles-monthly-integral",
    )


consistency_dag = ConsistencyDagBuilder(flow_name='vertica-etl').build(lambda dag, task_builder: build_consistency_dag(dag, task_builder))

globals()[consistency_dag.airflow_dag.dag_id] = consistency_dag.airflow_dag

for aggregation_vertica_cluster in (get_variants_for_group(TaskVariantGroup.VerticaAws)):
    # handles Integral data with PhaseOriginTypeId.LateData for historical reasons in Agiles
    flow_dag = FlowDagBuilder(
        flow_name='vertica-etl',
        batch_type_id=1,
        vertica_cluster=aggregation_vertica_cluster,
        phase_origin_types=["Incremental", "TrueUp", "LateData", "DataCorrection"],
        # aligned with Agiles
        batch_lookback_days=31,
    ).build(lambda dag, task_builder: build_flow_dag(dag, task_builder))

    globals()[flow_dag.airflow_dag.dag_id] = flow_dag.airflow_dag
