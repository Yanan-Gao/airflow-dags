from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime
from dags.trgt.geo_store.geo_store_clusters import GeoStoreClusters
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils
from dags.trgt.geo_store.geo_store_delta_configs import GeoStoreDeltaNotPushingConfig

# This delta pipeline is for generating geo store data in prod but NOT pushing them to downstream services (geo store diffcache + provisioning index + Aerospike + provisioning small geos)
# Input data: CDC data, targeting data ids (provisioning)
# Output data: geo store diffcache, geo tiles
job_name = "geo-store-delta-pipeline-not-pushing-data-to-downstream"
push_values_to_xcom_task_id = "push_values_to_xcom_task"
should_run_aerospike_daily_push_task_id = "should_run_aerospike_daily_push"
check_if_exists_delta_data_task_id = "check_if_exists_delta_data_task"
config = GeoStoreDeltaNotPushingConfig()
cdc_drop_threshold = 0.5

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 10, 17),
    schedule_interval="0 */3 * * *",
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

push_values_to_xcom_task = OpTask(
    op=PythonOperator(
        task_id=push_values_to_xcom_task_id,
        python_callable=GeoStoreUtils.push_values_to_xcom,
        op_kwargs={'config': config},
        provide_context=True
    )
)

geo_store_clusters = GeoStoreClusters(job_name, push_values_to_xcom_task_id, config)
current_prefix = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + push_values_to_xcom_task_id + "', key='CurrentGeoStorePrefix') }}"
last_prefix = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + push_values_to_xcom_task_id + "', key='LastSuccessfulGeoStorePrefix') }}"
core_nums = 480
# single_cluster_start = OpTask(op=DummyOperator(task_id="single_cluster_start", dag=dag.airflow_dag), )
# single_cluster_task = geo_store_clusters.get_geo_store_cluster('single_cluster', core_nums)
# single_cluster_task.add_sequential_body_task(geo_store_clusters.get_geo_targets_task(single_cluster_task, core_nums))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.remove_small_geo_task(single_cluster_task, core_nums))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.build_sensitive_places_task(single_cluster_task, core_nums))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.get_geo_tiles_at_trunk_level_task(single_cluster_task, core_nums))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.expand_geo_tiles_task(single_cluster_task, core_nums, 0))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.merge_diffCache_geotiles_task(single_cluster_task, core_nums))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.write_diffcache_data_task(single_cluster_task, core_nums))
# single_cluster_task.add_sequential_body_task(geo_store_clusters.convert_aerospike_data_task(single_cluster_task, core_nums))

# multiple_clusters_start = OpTask(op=DummyOperator(task_id="multiple_clusters_start", dag=dag.airflow_dag), )
multiple_clusters_partitioning_cluster_task = geo_store_clusters.get_geo_store_cluster('multiple_clusters_partitioning_cluster', core_nums)
multiple_clusters_partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_geo_targets_task(multiple_clusters_partitioning_cluster_task, core_nums)
)
multiple_clusters_partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.remove_small_geo_task(multiple_clusters_partitioning_cluster_task, core_nums)
)
multiple_clusters_partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.build_sensitive_places_task(multiple_clusters_partitioning_cluster_task, core_nums)
)
multiple_clusters_partitioning_cluster_task.add_sequential_body_task(
    geo_store_clusters.get_geo_tiles_at_trunk_level_task(multiple_clusters_partitioning_cluster_task, core_nums)
)

multiple_clusters_expanding_cluster_task = geo_store_clusters.get_expanding_cluster_and_task_in_parallel(
    'multiple_clusters_expanding_cluster', config.delta_build_num_files, core_nums
)

multiple_clusters_converting_cluster_task = geo_store_clusters.get_geo_store_cluster('multiple_clusters_converting_cluster', core_nums)
multiple_clusters_converting_cluster_task.add_sequential_body_task(
    geo_store_clusters.merge_diffCache_geotiles_task(multiple_clusters_converting_cluster_task, core_nums)
)
multiple_clusters_converting_cluster_task.add_sequential_body_task(
    geo_store_clusters.write_diffcache_data_task(multiple_clusters_converting_cluster_task, core_nums)
)
multiple_clusters_converting_cluster_task.add_sequential_body_task(
    geo_store_clusters.convert_aerospike_data_task(multiple_clusters_converting_cluster_task, core_nums)
)

delta_pushing_cluster_task = geo_store_clusters.get_aerospike_push_cluster_and_task_in_parallel(
    'hourly_delta_pushing_cluster', f'{current_prefix}/Delta/AerospikeData'
)

daily_converting_cluster_task = geo_store_clusters.get_geo_store_cluster('daily_converting_cluster', 32)
daily_converting_cluster_task.add_sequential_body_task(
    geo_store_clusters.convert_aerospike_data_task(daily_converting_cluster_task, 32, 'true')
)

daily_full_pushing_cluster_task = geo_store_clusters.get_aerospike_push_cluster_and_task_in_parallel(
    'daily_pushing_cluster', f'{current_prefix}/AerospikeData'
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

check_if_exists_delta_data_task = OpTask(
    op=ShortCircuitOperator(
        task_id=check_if_exists_delta_data_task_id,
        python_callable=GeoStoreUtils.check_if_exists_delta_data_task,
        op_kwargs={
            'path': f'{current_prefix}/Delta/PartitionedCellIdToTargets/',
            'bucket': config.geo_bucket
        },
        dag=dag.airflow_dag,
        trigger_rule='none_failed',
        provide_context=True,
    )
)

check_cdc_data_task = OpTask(
    op=ShortCircuitOperator(
        task_id='check_cdc_data_task',
        python_callable=GeoStoreUtils.check_cdc_data_task,
        op_kwargs={
            'last_path': f'{last_prefix}/Delta/CDCMetrics/',
            'current_path': f'{current_prefix}/Delta/CDCMetrics/',
            'threshold': cdc_drop_threshold
        },
        dag=dag.airflow_dag,
        trigger_rule='none_failed',
        provide_context=True,
    )
)

# Dependency
dag >> pull_geo_targeting_data_ids_and_brands_to_s3_task >> push_values_to_xcom_task >> multiple_clusters_partitioning_cluster_task >> check_cdc_data_task >> check_if_exists_delta_data_task
for task in multiple_clusters_expanding_cluster_task:
    check_if_exists_delta_data_task >> task
for task in multiple_clusters_expanding_cluster_task:
    task >> multiple_clusters_converting_cluster_task
multiple_clusters_converting_cluster_task >> record_hourly_job_path_task

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_delta_dag = dag.airflow_dag
