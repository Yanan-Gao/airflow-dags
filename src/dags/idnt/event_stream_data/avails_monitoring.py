"""
# Avails metrics monitoring pipeline for Identity

These steps monitor the quality and health of avails for Identity Graphs.

## Pipeline descriptions

### executivedashboard

Avails and bidfeedback (impression metrics) for the [Executive dashboard](https://tableau.thetradedesk.com/t/ttd-internal/views/IdentityProductMonitoring/AvailsData?%3Alinktarget=_parent&%3Aembed=yes&%3Atabs=no&%3AshowShareOptions=true#1).

### graphableids

Which ids qualify for graph building process? Used to assess inventory utility for graph building.
This is separated into weekly and daily jobs now. Daily job was necessary due to scale of datasets
for dealID grains, whilst the rest of the grains are calculated in one go on a weekly basis.

Powers the [Graphable Avails dashboard](https://prod-useast-b.online.tableau.com/t/thetradedeskcloud/views/GraphableAvails/CountryandDevice?%3Alinktarget=_parent&%3Aembed=yes&%3Atabs=yes&%3AshowShareOptions=true#2)

### AvailsTrafficMonitor

ETL job, could probably live in Identity repo, had no time to dig into this one.

### StatsCollector

A collection of avails and graph monitoring that are in desperate need for refactoring. Work
is under way to simplify and modernise these.
"""
from datetime import datetime

from airflow import DAG

from dags.idnt.identity_clusters import ComputeType, IdentityClusters
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from datasources.datasources import Datasources
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.base import BaseTask
from typing import Optional


class AvailsMonitoring:
    """Builds the pipeline including all avails monitoring related clusters.

    Attributes:
        dag (TtdDag): The DAG to add the avails monitoring tasks to.
    """

    def __init__(self, dag: TtdDag):
        self.dag = dag

    def __get_avails_data_metrics_cluster(self):
        cluster = IdentityClusters.get_cluster("AvailsDataMetricsCluster", self.dag, 3500, ComputeType.NEW_STORAGE)

        cluster.add_sequential_body_task(
            IdentityClusters.task("com.thetradedesk.idnt.identity.metrics.executivedashboard.AvailsDataMetrics", timeout_hours=10)
        )

        cluster.add_sequential_body_task(
            IdentityClusters.task("com.thetradedesk.idnt.identity.metrics.executivedashboard.ImpressionDataMetrics", timeout_hours=3)
        )

        return cluster

    def __get_stats_collector_cluster(self):
        cluster = IdentityClusters.get_cluster("StatsCollectorCluster", self.dag, 1200, ComputeType.STORAGE)

        # older jobs.identity.stats.StatsCollector workflow
        def get_stats_collector_task(stat_name: str):
            task = IdentityClusters.task("jobs.identity.stats.StatsCollector", eldorado_configs=[("runStat", stat_name)])
            # overwrite task name for uniqueness
            task.name = f"StatsCollector_{stat_name}"
            return task

        statsCollector_stages = ("Uid2AvailsAnalysis-30d", "Uid2CoverageAnalysis", "CrossDeviceStats")

        for stat_stage in statsCollector_stages:
            cluster.add_sequential_body_task(get_stats_collector_task(stat_stage))

        return cluster

    def __get_graphable_ids_clusters(self):
        """Powers Graphable Ids Dashboard

        Daily job that creates input data that powers the
        [Graphable Avails dashboard](https://prod-useast-b.online.tableau.com/t/thetradedeskcloud/views/GraphableAvails/CountryandDevice?%3Alinktarget=_parent&%3Aembed=yes&%3Atabs=yes&%3AshowShareOptions=true#2)
        """
        weekly_cluster = IdentityClusters.get_cluster("GraphableIds", self.dag, 8200, ComputeType.STORAGE)

        grains = [
            "country_ssp_pub", "country_devicetype_ssp_pub", "country_devicetype", "devicetype", "ssp_pub", "ssp_pub_devicetype",
            "ssp_pub_devicetype_devicemake", "ssp_pub_url", "ssp_pub_useragent", "ssp_pub_dealid"
        ]

        weekly_cluster.add_sequential_body_task(
            IdentityClusters.task(
                "jobs.identity.reports.graphableids.GraphableIDs",
                eldorado_configs=[('ReportsToRun', ",".join(grains))],
                timeout_hours=6,
                runDate_arg="date",
            )
        )

        weekly_branch = DagHelpers.skip_unless_day(["Sunday"])

        weekly_branch >> weekly_cluster

        return weekly_branch

    def build_pipeline(self, upstream_cluster: Optional[BaseTask] = None):
        """Builds the pipeline for avails monitoring.

        Args:
            upstream_cluster (Optional[BaseTask], optional): If this is specified, starts adding on the parts of
            this pipeline onto the task specified. If this is None, the pipeline is added directly to the
            class's dag. Defaults to None.
        """
        check_yesterday = DagHelpers.check_datasets([
            Datasources.avails.avails_7_day, Datasources.avails.avails_30_day, Datasources.common.rtb_bidfeedback_v5,
            IdentityDatasets.conversion_tracker
        ])

        if (first_step := upstream_cluster) is None:
            first_step = self.dag

        first_step >> check_yesterday

        check_yesterday >> self.__get_avails_data_metrics_cluster()
        check_yesterday >> self.__get_stats_collector_cluster()
        # TODO: migrate graphable ids to graph DAG.
        check_yesterday >> self.__get_graphable_ids_clusters()


# This DAG has been migrated to event-stream-data but remains specified here for Airflow history.
dag = DagHelpers.identity_dag(dag_id="avails-data-metrics", schedule_interval="0 8 * * *", start_date=datetime(2024, 9, 17), doc_md=__doc__)

AvailsMonitoring(dag).build_pipeline()

final_dag: DAG = dag.airflow_dag
