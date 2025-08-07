from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from ttd.ttdenv import TtdEnvFactory
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime
from dags.trgt.geo_store.geo_store_utils import GeoStoreUtils
from dags.trgt.geo_store.geo_store_config import GeoStoreConfig
from dags.trgt.geo_store.geo_store_clusters import GeoStoreClusters
import boto3

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    env = "env=test"
    mt_preferred_bucket = "ttd-geo-test"
else:
    env = "env=prod"
    mt_preferred_bucket = "ttd-geo"

file_path_in_mt_preferred = "GeoStoreNg/GeoStoreGeneratedData/LatestPrefix.txt"

job_name = "geo-store-copy-geo-tiles"
should_trigger_copy_task_id = "should_trigger_copy_task"
config = GeoStoreConfig()  # did not use any values from this config

diffcache_source_path = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + should_trigger_copy_task_id + "', key='DeltaLatestDiffCacheDataPath') }}"
diffcache_target_path = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + should_trigger_copy_task_id + "', key='MTPreferredLatestDiffCacheDataPath') }}"
tile_source_path = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + should_trigger_copy_task_id + "', key='DeltaLatestTileDataPath') }}"
tile_target_path = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + should_trigger_copy_task_id + "', key='MTPreferredLatestTileDataPath') }}"
content = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='" + should_trigger_copy_task_id + "', key='DeltaFileContent') }}"

geo_store_clusters = GeoStoreClusters(job_name, should_trigger_copy_task_id, config)

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2025, 1, 16),
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    run_only_latest=True,
    slack_channel="#scrum-targeting-alarms",
    slack_alert_only_for_prod=True,
    tags=[targeting.jira_team]
)


# MT reads file from s3a://ttd-geo/GeoStoreNg/GeoStoreGeneratedData/LatestPrefix.txt, which is hard coded in the geo config
# File content: 20250116/15
# GeoStoreLatestGeneratedDataS3RootPath = "s3a://ttd-geo/GeoStoreNg/GeoStoreGeneratedData" (hard code)
# DiffCacheSubFolderS3Path = "DiffCacheData/FileIndex=*/*" (hard code)
# S2CellMatchesSubFolderS3Path = "S2CellToFullAndPartialMatches/FileIndex=*/*" (hard code)
def should_trigger_copy(env, mt_preferred_bucket, **context):
    s3 = boto3.client("s3")
    # s3://ttd-geo/env=test/GeoStoreNg/GeoStoreGeneratedData/date=20250116/time=161213/DiffCacheData/0/0
    # s3://ttd-geo-test/GeoStoreNg/GeoStoreGeneratedData/date=20250116/time=161213/DiffCacheData/FileIndex=0/0
    latest_full_geo_tiles_log = f"{env}/GeoStoreNg/GeoStoreGeneratedData/LatestPrefix.txt"
    print(latest_full_geo_tiles_log)
    date_time_in_delta = s3.get_object(Bucket='ttd-geo', Key=latest_full_geo_tiles_log)["Body"].read().decode().strip()
    context['task_instance'].xcom_push(key="DeltaFileContent", value=date_time_in_delta)
    print(mt_preferred_bucket)
    print(file_path_in_mt_preferred)
    date_time_in_mt_preferred = s3.get_object(Bucket=mt_preferred_bucket, Key=file_path_in_mt_preferred)["Body"].read().decode().strip()
    context['task_instance'].xcom_push(key="MTPreferredFileContent", value=date_time_in_mt_preferred)

    if date_time_in_delta == date_time_in_mt_preferred:
        print("The latest data are the same.")
        return False
    else:
        delta_diffcache_path = f"s3://ttd-geo/{env}/GeoStoreNg/GeoStoreGeneratedData/{date_time_in_delta}/DiffCacheData"
        mt_preferred_diffcache_path = f"s3://{mt_preferred_bucket}/GeoStoreNg/GeoStoreGeneratedData/{date_time_in_delta}/DiffCacheData"
        delta_tile_path = f"s3://ttd-geo/{env}/GeoStoreNg/GeoStoreGeneratedData/{date_time_in_delta}/S2CellToFullAndPartialMatches"
        mt_preferred_tile_path = f"s3://{mt_preferred_bucket}/GeoStoreNg/GeoStoreGeneratedData/{date_time_in_delta}/S2CellToFullAndPartialMatches"
        context['task_instance'].xcom_push(key="DeltaLatestDiffCacheDataPath", value=delta_diffcache_path)
        context['task_instance'].xcom_push(key="MTPreferredLatestDiffCacheDataPath", value=mt_preferred_diffcache_path)
        context['task_instance'].xcom_push(key="DeltaLatestTileDataPath", value=delta_tile_path)
        context['task_instance'].xcom_push(key="MTPreferredLatestTileDataPath", value=mt_preferred_tile_path)
        print("Will sync data from delta")
        return True


should_trigger_copy = OpTask(
    op=ShortCircuitOperator(
        task_id=should_trigger_copy_task_id,
        python_callable=should_trigger_copy,
        op_kwargs={
            'env': env,
            'mt_preferred_bucket': mt_preferred_bucket
        },
        dag=dag.airflow_dag,
        trigger_rule='none_failed',
        provide_context=True,
    )
)

update_task = OpTask(
    op=PythonOperator(
        task_id="update_task",
        python_callable=GeoStoreUtils.write_to_s3_file,
        op_kwargs={
            'bucket': mt_preferred_bucket,
            'key': file_path_in_mt_preferred,
            'content': content
        },
        provide_context=True,
    )
)

copy_task = geo_store_clusters.get_geo_store_cluster('copy_cluster', 32)
copy_task.add_sequential_body_task(geo_store_clusters.get_copy_task('copy_diffcache', diffcache_source_path, diffcache_target_path))
copy_task.add_sequential_body_task(geo_store_clusters.get_copy_task('copy_geo_tiles', tile_source_path, tile_target_path))

# Dependency
dag >> should_trigger_copy >> copy_task >> update_task
# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_delta_dag = dag.airflow_dag
