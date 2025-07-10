from datetime import datetime
from typing import Dict, Any, List

from airflow import DAG

from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.eldorado.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.spark import ParallelTasks

from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_clusters_aliyun import IdentityClustersAliyun, AliCloudInstanceTypeConfig
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables, RunTimes, Tags


def get_dag_variables():
    executable_path = Executables.identity_repo_executable_aliyun
    start_date = datetime(2025, 5, 25, 14, 0, 0)
    end_date = None
    run_only_latest = None

    if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
        # set parameters for testing, they wouldn't be used for production
        executable_path = "oss://ttd-identity/ops/jars/identity/merge-request/yxw-idnt-5262-add-opengraph-publish-step/com/thetradedesk/idnt/identity_2.12/latest/identity-assembly.jar"
        start_date = datetime(2025, 6, 15, 14, 0, 0)
        end_date = start_date
        run_only_latest = False

    return executable_path, start_date, end_date, run_only_latest


class DagBuilder:
    cluster_factory: IdentityClusters | IdentityClustersAliyun = IdentityClustersAliyun
    executable_path, start_date, end_date, run_only_latest = get_dag_variables()

    dataset_cloud_provider: CloudProvider = CloudProviders.ali
    dag_id: str = "ttdgraph-generator-china"

    daily_processing_cluster_cores: int = 512
    weekly_input_cluster_cores: int = 512
    multi_week_cluster_cores: int = 1024
    edge_init_cluster_cores: int = 1024
    regional_clustering_cores: int = 1024
    weekly_metadata_cores: int = 64
    graph_publish_cores: int = 64

    def build(self) -> TtdDag:
        regions = {"CHINA": ["CHINA"]}

        dag: TtdDag = self.build_dag(tags=["CHINA"], slack_channel=Tags.slack_channel_alarms_testing)

        multi_week = dag \
            >> self.create_check_bidstream_task(datasets=[
                    IdentityDatasets.avails_v3_daily_agg.with_cloud(self.dataset_cloud_provider),
                    IdentityDatasets.avails_v3_daily_agg_idless.with_cloud(self.dataset_cloud_provider),
            ]) \
            >> self.create_daily_processing_task(dag, class_name=Executables.class_name("pipelines.OpenGraphDailyChina")) \
            >> self.create_weekly_inputs_cluster(dag, class_name=Executables.class_name("pipelines.OpenGraphWeeklyInputsPipelineChina")) \
            >> self.create_multi_week_cluster(dag, class_name=Executables.class_name("pipelines.OpenGraphMultiWeekPipelineChina"))

        graph_publish = multi_week \
            >> self.create_weekly_metadata(dag,
                                           class_name=Executables.class_name("pipelines.OpenGraphMetaPipelineChina")) \
            >> self.create_graph_publish_cluster(dag, class_name=Executables.class_name("pipelines.OpenGraphPublishChina"),
                                                 eldorado_configs=[("graphs.opengraph.runRelabel", "false")])  # need to set this to true (remove it, default is true) after we have at least 2 results

        multi_week \
            >> self.create_edge_init_cluster(dag, class_name=Executables.class_name("pipelines.OpenGraphEdgeInitChina")) \
            >> self.create_regional_clustering(dag, regions, class_name=Executables.class_name("pipelines.OpenGraphClusteringChina")) \
            >> graph_publish

        return dag

    def build_dag(self, **kwargs) -> TtdDag:
        args = {
            "dag_id": self.dag_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "schedule_interval": "0 14 * * SUN",
            "run_only_latest": self.run_only_latest,
        }
        args.update(kwargs)
        return DagHelpers.identity_dag(**args)

    def create_check_bidstream_task(self, **kwargs) -> OpTask:
        args = {
            # this is 0 based so we check for the past 7 days
            "lookback": 6,
            "cloud_provider": self.dataset_cloud_provider,
        }
        args.update(kwargs)
        return DagHelpers.check_datasets(**args)

    def create_daily_processing_task(self, dag: TtdDag, **kwargs) -> EmrClusterTask | AliCloudClusterTask:
        daily_processing_cluster = self.cluster_factory.get_cluster("ttdgraph_daily", dag, self.daily_processing_cluster_cores)

        for days_ago in range(7, 0, -1):
            daily_processing_cluster.add_parallel_body_task(
                self.create_cluster_task(runDate=RunTimes.date_x_days_ago(days_ago), task_name_suffix=f"_lag{days_ago}", **kwargs)
            )
        return daily_processing_cluster

    def create_weekly_inputs_cluster(self, dag: TtdDag, **kwargs) -> EmrClusterTask | AliCloudClusterTask:
        weekly_inputs_cluster = self.cluster_factory.get_cluster(
            "ttdgraph_weekly_input_cluster",
            dag,
            self.weekly_input_cluster_cores,
            ComputeType.ARM_GENERAL,
            ComputeType.ARM_GENERAL,
        )

        args = {
            # this is to disable eldoardo optimisations - this is the default
            "extra_args": [("conf", "spark.python.worker.reuse=true")],
        }
        args.update(kwargs)

        weekly_inputs_cluster.add_sequential_body_task(self.create_cluster_task(**args))
        return weekly_inputs_cluster

    def create_multi_week_cluster(self, dag: TtdDag, **kwargs) -> EmrClusterTask | AliCloudClusterTask:
        multi_week_cluster = self.cluster_factory.get_cluster(
            "ttdgraph_multiweek_cluster",
            dag,
            num_cores=self.multi_week_cluster_cores,
            core_type=AliCloudInstanceTypeConfig(memory_per_core=8, disk_per_core=128),
            master_type=ComputeType.GENERAL,
            cpu_bounds=(12, 2048),
        )

        args = {"timeout_hours": 16}
        args.update(kwargs)

        multi_week_cluster.add_sequential_body_task(self.create_cluster_task(**args))
        return multi_week_cluster

    def create_edge_init_cluster(self, dag: TtdDag, **kwargs) -> EmrClusterTask | AliCloudClusterTask:
        edge_init_cluster = self.cluster_factory.get_cluster(
            "ttdgraph_v2_edge_init_cluster",
            dag,
            num_cores=self.edge_init_cluster_cores,
            core_type=AliCloudInstanceTypeConfig(memory_per_core=8, disk_per_core=64),
            master_type=ComputeType.GENERAL,
            cpu_bounds=(12, 2048),
        )
        edge_init_cluster.add_sequential_body_task(self.create_cluster_task(**kwargs))
        return edge_init_cluster

    def create_regional_clustering(self, dag: TtdDag, regions: Dict[str, List[str]], **kwargs) -> ParallelTasks:
        clustering_jobs = [self.create_regional_clustering_job(dag, region_key, regs, **kwargs) for (region_key, regs) in regions.items()]
        return ParallelTasks("regional_clustering", clustering_jobs).as_taskgroup("regional_clustering_tasks")

    def create_regional_clustering_job(
        self, dag: TtdDag, group_name: str, regions: List[str], **kwargs
    ) -> EmrJobTask | AliCloudClusterTask:
        cluster = self.cluster_factory.get_cluster(
            f"clustering_{group_name}",
            dag,
            self.regional_clustering_cores,
            core_type=AliCloudInstanceTypeConfig(memory_per_core=8, disk_per_core=128),
            master_type=ComputeType.GENERAL,
            cpu_bounds=(8, 2048),
            parallelism_multiplier=1.0
        )

        for region in regions:
            final_cluster_args = {
                "eldorado_configs": [
                    ("graphs.opengraph.regions", region),
                ],
                "extra_args": [("conf", "spark.dynamicAllocation.maxExecutors=1000000")],
            }
            final_cluster_args.update(kwargs)
            cluster.add_parallel_body_task(self.create_cluster_task(**final_cluster_args))

        return cluster

    def create_weekly_metadata(self, dag: TtdDag, **kwargs) -> EmrClusterTask | AliCloudClusterTask:
        weekly_metadata_cluster = self.cluster_factory.get_cluster(
            "ttgraph_v2_weekly_metadata",
            dag,
            num_cores=self.weekly_metadata_cores,
            core_type=AliCloudInstanceTypeConfig(memory_per_core=8, disk_per_core=32),
            master_type=ComputeType.GENERAL,
            cpu_bounds=(8, 2048),
        )
        weekly_metadata_cluster.add_sequential_body_task(self.create_cluster_task(**kwargs))
        return weekly_metadata_cluster

    def create_graph_publish_cluster(self, dag: TtdDag, **kwargs) -> EmrClusterTask | AliCloudClusterTask:
        graph_publish_cluster = self.cluster_factory.get_cluster(
            "graph_publish_cluster",
            dag,
            num_cores=self.graph_publish_cores,
            core_type=AliCloudInstanceTypeConfig(memory_per_core=8, disk_per_core=32),
            master_type=ComputeType.GENERAL,
            cpu_bounds=(8, 2048),
        )

        graph_publish_cluster.add_sequential_body_task(self.create_cluster_task(**kwargs))
        return graph_publish_cluster

    def create_cluster_task(self, **kwargs) -> EmrJobTask | AliCloudJobTask:
        args: Dict[str, Any] = {
            "executable_path": self.executable_path,
        }
        args.update(kwargs)
        return self.cluster_factory.task(**args)


final_dag: DAG = DagBuilder().build().airflow_dag
