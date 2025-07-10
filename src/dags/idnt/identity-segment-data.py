from airflow import DAG
from dags.idnt.identity_helpers import DagHelpers
from datetime import datetime
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.tasks.chain import ChainOfTasks
from dags.idnt.statics import Executables
from dags.idnt.identity_datasets import IdentityDatasets

dag = DagHelpers.identity_dag(dag_id="identity-segment-data", start_date=datetime(2024, 10, 30), schedule_interval="0 17 * * *")


def create_segment_cluster_and_step(job_name: str, num_cores: int, timeout_hrs: int) -> EmrClusterTask:
    cluster = IdentityClusters.get_cluster(f"{job_name}Cluster", dag, num_cores, ComputeType.STORAGE, cpu_bounds=(64, 2048))

    cluster.add_parallel_body_task(
        IdentityClusters.task(
            f"jobs.segmentData.{job_name}",
            timeout_hours=timeout_hrs,
            extra_args=[("conf", "spark.databricks.delta.schema.autoMerge.enabled=true")]
        )
    )
    return cluster


def get_ttdsegments_for_graph() -> ChainOfTasks:
    """Process ttd owned segments for ttdgraph use case

    This is currently under research as we're working to unlock the value of ttd owned segments
    for better graph generation.

    Once this research is completed and the the segments are actively used in production for graph building
    consider moving this to graph generation DAG.

    Returns:
        ChainOfTasks: Grouped tasks for checking input data and processing them for id scoring for TTDGraph.
    """

    check_provisioning_segments = DagHelpers.check_datasets(
        datasets=[IdentityDatasets.targetingdatauniquesv2, IdentityDatasets.thirdpartydata],
    )

    cluster = IdentityClusters.get_cluster(
        "ttdsegments_collection_for_ttdgraph", dag, 8000, ComputeType.ARM_GENERAL, ComputeType.ARM_GENERAL
    )
    cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.TtdSegmentPipeline")))

    return ChainOfTasks(
        task_id="segment_data_processing",
        tasks=[check_provisioning_segments, cluster],
    ).as_taskgroup("check_and_process_ttd_segments_for_ttdgraph")


check_dataset = DagHelpers.check_datasets([RtbDatalakeDatasource.rtb_bidfeedback_v5.with_check_type('day')])

segment_daily_agg_cluster = create_segment_cluster_and_step("SegmentDataDailyAgg", 960, 1)
segment_overall_agg_cluster = create_segment_cluster_and_step("SegmentDataOverallAgg", 576, 1)
first_party_data_user_segments_cluster = create_segment_cluster_and_step("FirstPartyUserSegmentsAgg", 7680, 8)
third_party_data_user_segments_cluster = create_segment_cluster_and_step("ThirdPartyUserSegmentsAgg", 7680, 10)
user_segments_combined_cluster = create_segment_cluster_and_step("UserSegmentsCombined", 2880, 4)

thursday_only = DagHelpers.skip_unless_day(["Thursday"])
dag >> check_dataset >> segment_daily_agg_cluster >> thursday_only

thursday_only >> segment_overall_agg_cluster
segment_overall_agg_cluster >> first_party_data_user_segments_cluster >> user_segments_combined_cluster
segment_overall_agg_cluster >> third_party_data_user_segments_cluster >> user_segments_combined_cluster

user_segments_combined_cluster >> get_ttdsegments_for_graph()

final_dag: DAG = dag.airflow_dag
