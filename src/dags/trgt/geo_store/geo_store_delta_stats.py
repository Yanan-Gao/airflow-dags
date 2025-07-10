from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.trgt.geo_store.geo_store_clusters import GeoStoreClusters
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils
from dags.trgt.geo_store.geo_store_delta_configs import GeoStoreStatsConfig
from airflow.utils.dates import days_ago

# Note: for the adhoc full run, please note that avoid pushing Aerospike data between UTC 0 and 3.
job_name = "geo-store-delta-pipeline-stats"
paths = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_prefix_task', key='paths') }}"
config = GeoStoreStatsConfig()  # did not use any values from this config
geo_store_clusters = GeoStoreClusters(job_name, 'get_prefix_task', config)

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 3, 20),
    schedule_interval="0 4 * * *",
    max_active_runs=1,
    run_only_latest=True,
    slack_channel="#scrum-targeting-alarms",
    slack_alert_only_for_prod=True,
    tags=[targeting.jira_team]
)

get_prefix_task = OpTask(
    op=PythonOperator(
        task_id='get_prefix_task',
        python_callable=GeoStoreUtils.get_all_paths,
        op_kwargs={
            'bucket': 'ttd-geo',
            'prefix': f'env=prod/GeoStoreNg/GeoStoreGeneratedData/date={days_ago(1).strftime("%Y%m%d")}/'
        },
        provide_context=True
    )
)

stats_cluster_task = geo_store_clusters.get_stats_cluster(paths)

# Prod Dependency
dag >> get_prefix_task >> stats_cluster_task
# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_stats_dag = dag.airflow_dag
