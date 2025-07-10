from datetime import datetime, timedelta
from dags.idnt.identity_clusters import IdentityClusters
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers

uid2_avails_collect_dag = DagHelpers.identity_dag(
    dag_id='uid2-avails-collect',
    schedule_interval='0 10 * * *',
    start_date=datetime(2024, 10, 16, 10, 0),
    max_active_runs=1,
    retries=1,
    retry_delay=timedelta(minutes=30)
)

wait_for_avails_data = DagHelpers.check_datasets([
    IdentityDatasets.avails_daily,
    IdentityDatasets.avails_daily_hashed_id,
    IdentityDatasets.avails_daily_masked_ip,
])

cluster = IdentityClusters.get_cluster('uid2_avails_collect_cluster', uid2_avails_collect_dag, 1000)
cluster.add_parallel_body_task(IdentityClusters.task('jobs.identity.uid2.Uid2AvailsCollect'))

uid2_avails_collect_dag >> wait_for_avails_data >> cluster
dag = uid2_avails_collect_dag.airflow_dag
