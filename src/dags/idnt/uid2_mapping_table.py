"""
### Unified ID 2.0 and Ramp ID mapping table

Extracts (TDID, Device, UID2, RampID) edges from bid request and feedback and aggregates those for Adbrain generation.
"""
from airflow import DAG
from datetime import datetime
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.datasets.dataset_adapter import DatasetAdapter
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from ttd.el_dorado.v2.base import TtdDag

dag: TtdDag = DagHelpers.identity_dag(
    dag_id="uid2-mapping-table", start_date=datetime(2024, 11, 21), schedule_interval="0 10 * * *", doc_md=__doc__
)

cluster = IdentityClusters.get_cluster("uid2_mapping_table_cluster", dag, 1000, ComputeType.STORAGE)

step = IdentityClusters.task(
    class_name="jobs.identity.Uid2MappingTable",
    eldorado_configs=[("MappingTablePathDailyAgg", "s3://ttd-identity/data/prod/regular/mapping-table/dailyaggV2"),
                      ("BidRequestLocation", DatasetAdapter(RtbDatalakeDatasource.rtb_bidrequest_v5).location),
                      ("BidFeedbackLocation", DatasetAdapter(RtbDatalakeDatasource.rtb_bidfeedback_v5).location)],
    runDate="{{ ds }}",
    runDate_arg="date",
    timeout_hours=3
)

cluster.add_sequential_body_task(step)

dag >> cluster

adag: DAG = dag.airflow_dag
