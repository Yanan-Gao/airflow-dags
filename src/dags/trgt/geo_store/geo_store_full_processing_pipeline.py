from dags.trgt.geo_store.geo_store_clusters import GeoStoreClusters
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils
from dags.trgt.geo_store.geo_store_delta_configs import GeoStoreFullPushingConfig

# Note: for the adhoc full run, please note that avoid pushing Aerospike data between UTC 0 and 3.
job_name = "geo-store-full-processing-pipeline-v1"
push_values_to_xcom_task_id = "push_values_to_xcom_task"
check_if_exists_partition_data_task_id = "check_if_exists_partition_data_task"
config = GeoStoreFullPushingConfig()

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 10, 17),
    schedule_interval=None,
    max_active_runs=1,
    run_only_latest=True,
    slack_channel="#scrum-targeting-alarms",
    slack_alert_only_for_prod=True,
    tags=[targeting.jira_team]
)

pull_geo_targeting_data_ids_and_brands_to_s3_task = OpTask(
    op=PythonOperator(
        task_id='pull_geo_targeting_data_ids_and_brands_to_s3',
        python_callable=GeoStoreUtils.pull_geo_targeting_data_ids_and_brands_to_s3,
        op_kwargs={
            'geo_bucket': config.geo_targeting_data_bucket,
            'output_base_path': config.geo_targeting_data_prefix
        },
        provide_context=True
    )
)

get_value_prepared_task = OpTask(
    op=PythonOperator(
        task_id=push_values_to_xcom_task_id,
        python_callable=GeoStoreUtils.push_values_to_xcom,
        op_kwargs={'config': config},
        provide_context=True
    )
)

geo_store_clusters = GeoStoreClusters(job_name, push_values_to_xcom_task_id, config)
current_prefix = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + push_values_to_xcom_task_id + "', key='CurrentGeoStorePrefix') }}"
core_nums = 480
partitioning_cluster_task = geo_store_clusters.get_geo_store_cluster('partitioning_cluster', core_nums)
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.get_geo_targets_task(partitioning_cluster_task, core_nums))
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.remove_small_geo_task(partitioning_cluster_task, core_nums))
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.build_sensitive_places_task(partitioning_cluster_task, core_nums))
partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_geo_tiles_at_trunk_level_task(partitioning_cluster_task, core_nums)
)

expanding_cluster_task = geo_store_clusters.get_expanding_cluster_and_task_in_parallel(
    'expanding_cluster', config.full_build_num_files, core_nums
)

converting_cluster_task = geo_store_clusters.get_geo_store_cluster('converting_cluster', core_nums)
converting_cluster_task.add_sequential_body_task(geo_store_clusters.write_diffcache_data_task(converting_cluster_task, core_nums))
converting_cluster_task.add_sequential_body_task(geo_store_clusters.convert_aerospike_data_task(converting_cluster_task, core_nums))

pushing_cluster_task = geo_store_clusters.get_aerospike_push_cluster_and_task_in_parallel(
    'hourly_full_pushing_cluster', f'{current_prefix}/AerospikeData'
)

update_diffcache_index_db = OpTask(
    op=PythonOperator(
        task_id='update_diffcache_index_db',
        python_callable=GeoStoreUtils.update_diffcache_index_db,
        op_kwargs={'conn_id': 'ttd_geo_provdb'},
        provide_context=True
    )
)

update_diffcache_3_index_db = OpTask(
    op=PythonOperator(
        task_id='update_diffcache_3_index_db',
        python_callable=GeoStoreUtils.update_diffcache_3_index_db,
        op_kwargs={'conn_id': 'ttd_geo_provdb'},
        provide_context=True
    )
)

record_hourly_job_path_task = OpTask(
    op=PythonOperator(
        task_id="record_hourly_job_path_task",
        python_callable=GeoStoreUtils.record_hourly_job_path,
        op_kwargs={
            'push_values_to_xcom_task_id': push_values_to_xcom_task_id,
            'config': config
        },
        provide_context=True,
    )
)

record_aerospike_daily_job_path_task = OpTask(
    op=PythonOperator(
        task_id="record_daily_job_path_task",
        python_callable=GeoStoreUtils.write_to_s3_file,
        op_kwargs={
            'bucket': config.geo_bucket,
            'key': config.aerospike_daily_push_log,
            'content': str(datetime.now(timezone.utc).date())
        },
        provide_context=True,
    )
)

update_small_geo_db_task = OpTask(
    op=PythonOperator(
        task_id='update_small_geo_db',
        python_callable=GeoStoreUtils.update_small_geo_db,
        op_kwargs={
            'conn_id': 'ttd_geo_provdb',
            'path': f'{current_prefix}/SmallGeo/SmallGeoTargetingDataId'
        },
        provide_context=True
    )
)

# Prod Dependency
dag >> pull_geo_targeting_data_ids_and_brands_to_s3_task >> get_value_prepared_task >> partitioning_cluster_task

for task in expanding_cluster_task:
    partitioning_cluster_task >> task
for task in expanding_cluster_task:
    task >> converting_cluster_task

for task in pushing_cluster_task:
    converting_cluster_task >> task
for task in pushing_cluster_task:
    task >> update_diffcache_index_db

update_diffcache_index_db >> update_diffcache_3_index_db
update_diffcache_index_db >> record_hourly_job_path_task >> record_aerospike_daily_job_path_task
update_diffcache_index_db >> update_small_geo_db_task

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_full_dag = dag.airflow_dag
