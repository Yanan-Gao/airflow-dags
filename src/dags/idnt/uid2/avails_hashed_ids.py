"""
Inquiro (UID2 vendor) needs a list of MAIDs we've seen in avails each month.
We can't send the raw MAIDs, so we need to build a list of raw and hashed ID pairings.
"""

from datetime import datetime, timedelta
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters
from dags.idnt.statics import RunTimes

avails_hashed_ids = DagHelpers.identity_dag(
    dag_id="avails-hashed-ids",
    schedule_interval="0 12 1 * *",
    start_date=datetime(2024, 8, 1, 12, 0, 0),
    retries=1,
    retry_delay=timedelta(hours=1)
)

cluster = IdentityClusters.get_cluster("avails-hashed-ids-cluster", avails_hashed_ids, 96)

cluster.add_parallel_body_task(
    IdentityClusters.task(
        class_name="jobs.identity.uid2.vendors.AvailsHashedIDs",
        runDate=RunTimes.current_full_day,
    )
)

avails_hashed_ids >> cluster
final_dag = avails_hashed_ids.airflow_dag
