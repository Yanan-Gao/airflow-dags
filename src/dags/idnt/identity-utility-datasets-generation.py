from airflow import DAG
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from datetime import datetime
from dags.idnt.statics import RunTimes
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from dags.idnt.identity_clusters import IdentityClusters, ComputeType

from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.tasks.op import OpTask
from airflow.operators.empty import EmptyOperator

INPUT = "input"
OUTPUT = "output"


def generate_utility_ids_agg(utility_type: str, cluster_task: EmrClusterTask) -> OpTask:

    utility_ids_agg_start = OpTask(op=EmptyOperator(task_id=f"{utility_type}_agg_start"))

    check_input_dataset = DagHelpers.check_datasets([utility_datasets_map[f"{utility_type}.{INPUT}"]])
    check_prev_utility_ids_overall_agg = DagHelpers.check_datasets([utility_datasets_map[f"{utility_type}.{OUTPUT}"]],
                                                                   ds_date=RunTimes.days_02_ago_last_hour)

    cluster_task.add_sequential_body_task(
        IdentityClusters.task(
            f"jobs.identity.alliance.v2.utilityData.{utility_type}DailyAgg",
            runDate_arg="date",
            timeout_hours=1,
        )
    )

    cluster_task.add_sequential_body_task(
        IdentityClusters.task(f"jobs.identity.alliance.v2.utilityData.{utility_type}OverallAgg", runDate_arg="date", timeout_hours=2)
    )

    utility_dataset_to_copy = utility_datasets_map[f"{utility_type}.{OUTPUT}"]
    copy_utility_ids_overall_agg_azure = DagHelpers.get_dataset_transfer_task(
        f'copy_{utility_type}_overall_agg_azure',
        utility_dataset_to_copy,
        utility_dataset_to_copy.get_partitioning_args(ds_date=RunTimes.previous_full_day),
        timeout_hrs=8
    )

    saturday_only = DagHelpers.skip_unless_day(["Saturday"], utility_type)

    utility_ids_agg_start >> check_input_dataset >> cluster_task
    utility_ids_agg_start >> check_prev_utility_ids_overall_agg >> cluster_task
    cluster_task >> saturday_only

    saturday_only >> copy_utility_ids_overall_agg_azure

    return utility_ids_agg_start


dag = DagHelpers.identity_dag(
    dag_id="identity-utility-datasets-generation", start_date=datetime(2024, 10, 29), schedule_interval="0 15 * * *"
)

# currently these utility datasets being used for IAv2
original_id_utility = "OriginalId"
avails_id_utility = "AvailsId"

utility_datasets_map = {
    f"{original_id_utility}.{INPUT}": RtbDatalakeDatasource.rtb_bidfeedback_v5.with_check_type('day'),
    f"{original_id_utility}.{OUTPUT}": IdentityDatasets.original_ids_overall_agg,
    f"{avails_id_utility}.{INPUT}": IdentityDatasets.avails_idless_daily_agg,
    f"{avails_id_utility}.{OUTPUT}": IdentityDatasets.avails_id_overall_agg
}

dag >> generate_utility_ids_agg(original_id_utility, IdentityClusters.get_cluster(f"{original_id_utility}-agg-cluster", dag, num_cores=200))

dag >> generate_utility_ids_agg(
    avails_id_utility,
    IdentityClusters.get_cluster(f"{avails_id_utility}-agg-cluster", dag, 2500, ComputeType.STORAGE, cpu_bounds=(64, 2048))
)

final_dag: DAG = dag.airflow_dag
