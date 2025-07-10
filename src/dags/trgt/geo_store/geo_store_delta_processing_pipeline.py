from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timezone
from dags.trgt.geo_store.geo_store_clusters import GeoStoreClusters
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils, update_small_geo_by_general_pipeline
import boto3
from dags.trgt.geo_store.geo_store_delta_configs import GeoStoreDeltaPushingConfig

job_name = "geo-store-delta-processing-pipeline-v1"
push_values_to_xcom_task_id = "push_values_to_xcom_task"
should_run_aerospike_daily_push_task_id = "should_run_aerospike_daily_push"
check_if_exists_delta_data_task_id = "check_if_exists_delta_data_task"
config = GeoStoreDeltaPushingConfig()
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


def should_run_aerospike_daily_push(config, **context):
    current_utc_time = datetime.now(timezone.utc)
    current_date = current_utc_time.date().isoformat()
    current_hour = current_utc_time.hour
    s3 = boto3.client("s3")
    latest_run_date_str = s3.get_object(Bucket=config.geo_bucket, Key=config.aerospike_daily_push_log)["Body"].read().decode().strip()
    context['task_instance'].xcom_push(key="AerospikePushDate", value=current_date)
    context['task_instance'].xcom_push(key="AerospikeLastPushDate", value=latest_run_date_str)
    if current_date == latest_run_date_str:
        print("Daily push is done today.")
        return False
    if current_hour in {23, 0, 1, 2}:
        print("Cannot run daily push within 23, 0, 1, 2 UTC")
        return False
    return True


# def decide_branch(partitions):
#     if partitions <= 1:
#         return single_cluster_start.task_id
#     else:
#         return multiple_clusters_start.task_id

should_trigger_aerospike_daily_push_task = OpTask(
    op=ShortCircuitOperator(
        task_id=should_run_aerospike_daily_push_task_id,
        python_callable=should_run_aerospike_daily_push,
        op_kwargs={'config': config},
        dag=dag.airflow_dag,
        trigger_rule='none_failed',
        provide_context=True,
    )
)

record_aerospike_daily_job_path_task = OpTask(
    op=PythonOperator(
        task_id="record_daily_job_path_task",
        python_callable=GeoStoreUtils.write_to_s3_file,
        op_kwargs={
            'bucket':
            config.geo_bucket,
            'key':
            config.aerospike_daily_push_log,
            'content':
            "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + should_run_aerospike_daily_push_task_id +
            "', key='AerospikePushDate') }}"
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
# merge_branches = OpTask(op=DummyOperator(task_id="merge_branches", dag=dag.airflow_dag), )
dag >> pull_geo_targeting_data_ids_and_brands_to_s3_task >> push_values_to_xcom_task >> multiple_clusters_partitioning_cluster_task >> check_cdc_data_task >> check_if_exists_delta_data_task
for task in multiple_clusters_expanding_cluster_task:
    check_if_exists_delta_data_task >> task
for task in multiple_clusters_expanding_cluster_task:
    task >> multiple_clusters_converting_cluster_task

for task in delta_pushing_cluster_task:
    multiple_clusters_converting_cluster_task >> task

for task in delta_pushing_cluster_task:
    task >> update_diffcache_index_db

update_diffcache_index_db >> update_diffcache_3_index_db
update_diffcache_index_db >> record_hourly_job_path_task >> should_trigger_aerospike_daily_push_task >> daily_converting_cluster_task

if not update_small_geo_by_general_pipeline:
    update_diffcache_index_db >> update_small_geo_db_task

for task in daily_full_pushing_cluster_task:
    daily_converting_cluster_task >> task
for task in daily_full_pushing_cluster_task:
    task >> record_aerospike_daily_job_path_task
# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_delta_dag = dag.airflow_dag
