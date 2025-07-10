from datetime import timedelta

from dags.dprpts.agiles.pipe_builder.default_args import default_dag_args, default_task_args
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.task_service.vertica_clusters import get_variants_for_group, TaskVariantGroup

base_task_name = "ts-rti-pipe-builder-"
max_active_runs = 1
task_execution_timeout = timedelta(hours=3)

# currently all clients of pipe builder runs aggregation on ETL clusters only
for vertica_cluster in get_variants_for_group(TaskVariantGroup.VerticaAws):
    pipeline_suffix = vertica_cluster.name
    # 1 dag per vertica cluster
    ttd_dag = TtdDag(
        base_task_name + TaskServiceOperator.format_task_name("monitor-task", pipeline_suffix),
        schedule_interval=timedelta(minutes=15),
        max_active_runs=max_active_runs,
        **default_dag_args,
    )

    secret_suffix = "-" + vertica_cluster.name.replace("01", "").lower()

    task_args = {
        "task_name_suffix": pipeline_suffix,
        "task_execution_timeout": task_execution_timeout,
        "vertica_cluster": vertica_cluster,
        "configuration_overrides": {
            "RtiPipeMonitorTask.OverrideConfigViaSecret": "test/use-dataops003-credentials" + secret_suffix
        },
        **default_task_args,
    }

    monitor_task = OpTask(op=TaskServiceOperator(task_name="RtiPipeBuilderMonitorTask", **task_args))

    ttd_dag >> monitor_task

    globals()[ttd_dag.airflow_dag.dag_id] = ttd_dag.airflow_dag
