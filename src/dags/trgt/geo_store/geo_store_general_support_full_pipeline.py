from dags.trgt.geo_store.geo_store_clusters import GeoStoreClusters
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils, update_small_geo_by_general_pipeline
from dags.trgt.geo_store.geo_store_general_support_configs import GeoStoreGeneralFullPushingConfig
from ttd.ttdenv import TtdEnvFactory

job_name = "geo-store-general-support-full-pipeline"
push_values_to_xcom_task_id = "push_values_to_xcom_task"
config = GeoStoreGeneralFullPushingConfig()
# do not set a schedule if it is airflow prod test
schedule_interval = "0 */3 * * *" if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else None

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 4, 23),
    schedule_interval=schedule_interval,
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
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.get_all_geo_targets_task(partitioning_cluster_task, core_nums))
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.remove_small_circles_task(partitioning_cluster_task, core_nums))
partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.build_sensitive_places_task_for_general_polygon(partitioning_cluster_task, core_nums)
)
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.get_process_circles_task(partitioning_cluster_task, core_nums))

# custom polygon
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.load_custom_polygon_task(partitioning_cluster_task, core_nums))
partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_process_custom_polygons_task(partitioning_cluster_task, core_nums)
)
partitioning_cluster_task.add_sequential_body_task(geo_store_clusters.remove_small_polygon_task(partitioning_cluster_task, core_nums))

# political polygon
partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_load_political_polygons_task(partitioning_cluster_task, core_nums)
)
partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_process_political_polygons_task(partitioning_cluster_task, core_nums)
)

# assemble
assembling_cluster_task = geo_store_clusters.get_assembling_cluster_and_task_in_parallel(
    'assembling_cluster', config.full_build_num_files, core_nums
)

converting_cluster_task = geo_store_clusters.get_geo_store_cluster('converting_cluster', core_nums)
converting_cluster_task.add_sequential_body_task(geo_store_clusters.get_shard_cell_task(converting_cluster_task, core_nums))
converting_cluster_task.add_sequential_body_task(geo_store_clusters.get_convert_aerospike_data_task(converting_cluster_task, core_nums))
converting_cluster_task.add_sequential_body_task(geo_store_clusters.get_convert_diff_cache_data_task(converting_cluster_task, core_nums))

daily_full_pushing_cluster_task = geo_store_clusters.get_general_polygon_aerospike_push_cluster_and_task_in_parallel(
    'daily_pushing_cluster', f'{current_prefix}/AerospikeGeoTile'
)

write_diff_cache_cluster_task = geo_store_clusters.get_geo_store_cluster('write_diff_cache_cluster', core_nums)
write_diff_cache_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_write_diff_cache_data_task(write_diff_cache_cluster_task, core_nums)
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

# partition(load and generate data in canonical format) > assemble
for task in assembling_cluster_task:
    partitioning_cluster_task >> task
# assemble > convert
for task in assembling_cluster_task:
    task >> converting_cluster_task
# convert > push to aerospike
for task in daily_full_pushing_cluster_task:
    converting_cluster_task >> task
# push to aerospike > write diff cache
for task in daily_full_pushing_cluster_task:
    task >> write_diff_cache_cluster_task

if update_small_geo_by_general_pipeline:
    write_diff_cache_cluster_task >> update_small_geo_db_task

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_full_dag = dag.airflow_dag
