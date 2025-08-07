"""
This canary DAG writes and deletes a file in S3. This verifies the SA is ok, connectivity to s3 works and also emits
 OpenLineage events (although it wouldn't fail if that is not working).
"""
from datetime import datetime, timedelta
from ttd.eldorado.base import TtdDag

from ttd.tasks.op import OpTask
from ttd.tasks.python import PythonOperator
from ttd.ttdenv import TtdEnvFactory
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.cloud_provider import CloudProviders
import logging

DST_BUCKET = "ttd-airflow-service-useast"
DST_PATH = f"env={str(TtdEnvFactory.get_from_system()).lower()}/canary/testfile"

schedule_interval = None
max_active_runs = 5

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    schedule_interval = timedelta(hours=1)
    max_active_runs = 1

dag = TtdDag(
    dag_id="aws-s3-canary",
    schedule_interval=schedule_interval,
    max_active_runs=max_active_runs,
    tags=["Canary", "Regression", "Monitoring", "Operations"],
    run_only_latest=True,
    start_date=datetime(2024, 12, 6),
    depends_on_past=False,
    retries=2,
    retry_delay=timedelta(minutes=5),
)


def create_s3_file(file_name: str, **kwargs):
    s3_hook = CloudStorageBuilder(CloudProviders.aws).build()
    destination_key = f"{DST_PATH}/{file_name}"
    full_destination = f"s3://{DST_BUCKET}/{destination_key}"
    logging.info(f"Creating file {full_destination}")
    s3_hook.put_object(bucket_name=DST_BUCKET, key=destination_key, body="", replace=True)
    logging.info(f"Created file {full_destination}")


def delete_s3_file(file_name: str, **kwargs):
    s3_hook = CloudStorageBuilder(CloudProviders.aws).build()
    destination_key = f"{DST_PATH}/{file_name}"
    full_destination = f"s3://{DST_BUCKET}/{destination_key}"
    logging.info(f"Deleting file {full_destination}")
    s3_hook.remove_objects(
        bucket_name=DST_BUCKET,
        prefix=destination_key,
    )
    logging.info(f"Deleted file {full_destination}")


s3_create = OpTask(
    op=PythonOperator(task_id='create_file', python_callable=create_s3_file, provide_context=True, op_kwargs={"file_name": "{{ run_id }}"})
)

s3_delete = OpTask(
    op=PythonOperator(task_id='delete_file', python_callable=delete_s3_file, provide_context=True, op_kwargs={"file_name": "{{ run_id }}"})
)

adag = dag.airflow_dag

dag >> s3_create >> s3_delete
