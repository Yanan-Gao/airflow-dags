"""
This module creates Airflow tasks to copy datasets from AWS to Azure.
"""
import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from typing import List

from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator

from dags.pdg.microtargeting_tasks.azure import MicrotargetingTasksAzure
from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.data_transfer.file_copier_factory import FileCopierFactory
from ttd.tasks.op import OpTask


class CopyDatasetTasks:
    COPY_GEO_STORE_DATA_TASK_ID = "copy-geo-store-data-aws-to-azure"
    COPY_US_ZIP_ADJACENCY_DATA_TASK_ID = "copy-us-zip-adjacency-data-aws-to-azure"

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

    def _copy_aws_data_to_azure(
        self, aws_bucket: str, aws_prefix: str, azure_bucket: str, azure_prefix: str, transfer_dir: str, aws_file_keys: List[str], **kwargs
    ):
        cloud_storage_aws = CloudStorageBuilder(CloudProviders.aws).set_conn_id(MicrotargetingTasksShared.AWS_CONNECTION_ID).build()
        cloud_storage_azure = CloudStorageBuilder(CloudProviders.azure).set_conn_id(MicrotargetingTasksShared.AZURE_CONNECTION_ID).build()

        copier_factory = FileCopierFactory(
            src_bucket=aws_bucket,
            dst_bucket=azure_bucket,
            src_prefix=aws_prefix,
            dst_prefix=azure_prefix,
            src_cloud_storage=cloud_storage_aws,
            dst_cloud_storage=cloud_storage_azure,
            max_try_count=3,
            try_internal_transfer=False,
            transfer_directory=transfer_dir,
        )

        try:
            with ThreadPoolExecutor() as executor:
                futures = []
                for key in aws_file_keys:
                    file_copier = copier_factory.get_copier(key)
                    futures.append(executor.submit(file_copier.copy_file))

                done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
                ex = next((thread.exception() for thread in done if thread.exception()), None)
                if ex:
                    logging.error("Failed to copy file", exc_info=ex)
                    for future in futures:
                        future.cancel()
                    raise Exception(str(ex))
        finally:
            if os.path.exists(copier_factory.tmp_dir_path):
                logging.info(f"Deleting directory: {copier_factory.tmp_dir_path}")
                shutil.rmtree(copier_factory.tmp_dir_path)
                logging.info(f"Directory deleted: {copier_factory.tmp_dir_path}")

    def _copy_geo_store_data(self, task_instance: TaskInstance, **kwargs):
        cloud_storage_aws = CloudStorageBuilder(CloudProviders.aws).set_conn_id(MicrotargetingTasksShared.AWS_CONNECTION_ID).build()
        cloud_storage_azure = CloudStorageBuilder(CloudProviders.azure).set_conn_id(MicrotargetingTasksShared.AZURE_CONNECTION_ID).build()

        geo_store_dataset_prefix = f"{task_instance.xcom_pull(dag_id=self.dag_id, task_ids=MicrotargetingTasksShared.CONTEXT_TASK_ID, key=MicrotargetingTasksShared.KEY_S3_GEO_PREFIX)}/GeoStoreNg/GeoStoreGeneratedData"
        aws_geo_bucket_name = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksShared.CONTEXT_TASK_ID, key=MicrotargetingTasksShared.KEY_S3_GEO_BUCKET_NAME
        )
        azure_bucket = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=MicrotargetingTasksAzure.KEY_AZURE_BUCKET
        )
        azure_prefix = f"{task_instance.xcom_pull(dag_id=self.dag_id, task_ids=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=MicrotargetingTasksAzure.KEY_ENV)}/GeoStoreGeneratedData"

        with tempfile.TemporaryDirectory() as temp_folder:
            latest_prefix_file = "LatestPrefix.txt"
            local_file = os.path.join(temp_folder, latest_prefix_file)

            cloud_storage_aws.download_file(
                file_path=local_file,
                key=f"{geo_store_dataset_prefix}/{latest_prefix_file}",
                bucket_name=aws_geo_bucket_name,
            )

            with open(local_file, 'r') as file:
                latest_prefix = file.read().strip()

            cloud_storage_azure.upload_file(
                file_path=local_file, key=f"{azure_prefix}/{latest_prefix_file}", bucket_name=azure_bucket, replace=True
            )

        aws_prefix = f"{geo_store_dataset_prefix}/{latest_prefix}"

        diff_cache_data_keys = cloud_storage_aws.list_keys(bucket_name=aws_geo_bucket_name, prefix=f"{aws_prefix}/DiffCacheData/")
        s2_cell_to_full_and_partial_matches_keys = cloud_storage_aws.list_keys(
            bucket_name=aws_geo_bucket_name, prefix=f"{aws_prefix}/S2CellToFullAndPartialMatches/"
        )
        all_aws_keys = diff_cache_data_keys + s2_cell_to_full_and_partial_matches_keys

        self._copy_aws_data_to_azure(
            aws_bucket=aws_geo_bucket_name,
            aws_prefix=f"{aws_prefix}/",
            azure_bucket=azure_bucket,
            azure_prefix=f"{azure_prefix}/{latest_prefix}/",
            transfer_dir="/tmp/geo_store_copy_task",
            aws_file_keys=all_aws_keys,
            **kwargs
        )

    def create_copy_geo_store_data_task(self):
        return OpTask(
            op=PythonOperator(
                task_id=self.COPY_GEO_STORE_DATA_TASK_ID, provide_context=True, python_callable=self._copy_geo_store_data, retries=1
            )
        )

    def _copy_us_zip_adjacency_data(self, task_instance: TaskInstance, **kwargs):
        cloud_storage_aws = CloudStorageBuilder(CloudProviders.aws).set_conn_id(MicrotargetingTasksShared.AWS_CONNECTION_ID).build()

        us_zip_adjacency_dataset_prefix = f"{task_instance.xcom_pull(dag_id=self.dag_id, task_ids=MicrotargetingTasksShared.CONTEXT_TASK_ID, key=MicrotargetingTasksShared.KEY_S3_GEO_PREFIX)}/USZipAdjacency/"
        aws_geo_bucket_name = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksShared.CONTEXT_TASK_ID, key=MicrotargetingTasksShared.KEY_S3_GEO_BUCKET_NAME
        )

        azure_bucket = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=MicrotargetingTasksAzure.KEY_AZURE_BUCKET
        )
        azure_prefix = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=MicrotargetingTasksAzure.KEY_ENV
        )

        aws_keys = cloud_storage_aws.list_keys(bucket_name=aws_geo_bucket_name, prefix=us_zip_adjacency_dataset_prefix)

        self._copy_aws_data_to_azure(
            aws_bucket=aws_geo_bucket_name,
            aws_prefix=us_zip_adjacency_dataset_prefix,
            azure_bucket=azure_bucket,
            azure_prefix=f"{azure_prefix}/USZipAdjacency/",
            transfer_dir="/tmp/us_zip_adjacency_copy_task",
            aws_file_keys=aws_keys,
            **kwargs
        )

    def create_copy_us_zip_adjacency_data_task(self):
        return OpTask(
            op=PythonOperator(
                task_id=self.COPY_US_ZIP_ADJACENCY_DATA_TASK_ID,
                provide_context=True,
                python_callable=self._copy_us_zip_adjacency_data,
                retries=1
            )
        )
