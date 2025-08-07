"""
This module creates Airflow tasks to copy datasets from AWS to Azure.
"""
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator

from dags.pdg.microtargeting_tasks.azure import MicrotargetingTasksAzure
from dags.pdg.microtargeting_tasks.microtargeting_pipeline_datasets import source_us_zip_adjacency_dataset, \
    dest_us_zip_adjacency_dataset, source_geo_store_diff_cache_data_dataset, dest_geo_store_diff_cache_data_dataset, \
    dest_geo_store_s2_cell_to_full_and_partial_matches_dataset, \
    source_geo_store_s2_cell_to_full_and_partial_matches_dataset
from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from dags.pdg.task_utils import xcom_pull_str
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.datasets.dataset import Dataset
from ttd.tasks.op import OpTask


class CopyDatasetTasks:
    GET_LATEST_GEO_STORE_DATA_TASK_ID = "get-latest-geo-store-data"
    COPY_GEO_STORE_DIFF_CACHE_DATA_TASK_ID = "copy-geo-store-diff-cache-data-aws-to-azure"
    COPY_GEO_STORE_S2_CELL_TO_FULL_AND_PARTIAL_MATCHES_TASK_ID = "copy-geo-store-s2-cell-to-full-and-partial-matches-aws-to-azure"
    COPY_US_ZIP_ADJACENCY_DATA_TASK_ID = "copy-us-zip-adjacency-data-aws-to-azure"

    KEY_LATEST_S3_GEO_STORE_DATA_PREFIX = "latest_s3_geo_store_data_key"

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

    def _get_latest_geo_store_data(self, task_instance: TaskInstance, **kwargs):
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

        latest_dt = datetime.strptime(latest_prefix, "date=%Y%m%d/time=%H%M%S")
        formatted_latest_prefix = latest_dt.strftime("%Y-%m-%d %H:%M:%S")

        task_instance.xcom_push(key=self.KEY_LATEST_S3_GEO_STORE_DATA_PREFIX, value=formatted_latest_prefix)

    def create_get_latest_geo_store_data_task(self):
        return OpTask(
            op=PythonOperator(
                task_id=self.GET_LATEST_GEO_STORE_DATA_TASK_ID,
                provide_context=True,
                python_callable=self._get_latest_geo_store_data,
                retries=1
            )
        )

    def create_dataset_transfer_aws_to_azure_task(
        self, task_id: str, dataset: Dataset, partitioning_args: Dict[str, Any], dst_dataset: Optional[Dataset] = None
    ):
        return DatasetTransferTask(
            name=task_id,
            dataset=dataset,
            src_cloud_provider=CloudProviders.aws,
            dst_cloud_provider=CloudProviders.azure,
            partitioning_args=partitioning_args,
            src_conn_id=MicrotargetingTasksShared.AWS_CONNECTION_ID,
            dst_conn_id=MicrotargetingTasksShared.AZURE_CONNECTION_ID,
            dst_dataset=dst_dataset,
            max_try_count=10,
            prepare_finalise_timeout=timedelta(minutes=30)
        )

    def create_copy_geo_store_diff_cache_data_task(self):
        return self.create_dataset_transfer_aws_to_azure_task(
            task_id=self.COPY_GEO_STORE_DIFF_CACHE_DATA_TASK_ID,
            dataset=source_geo_store_diff_cache_data_dataset,
            partitioning_args=source_geo_store_diff_cache_data_dataset.get_partitioning_args(
                ds_date=xcom_pull_str(
                    dag_id=self.dag_id, task_id=self.GET_LATEST_GEO_STORE_DATA_TASK_ID, key=self.KEY_LATEST_S3_GEO_STORE_DATA_PREFIX
                )
            ),
            dst_dataset=dest_geo_store_diff_cache_data_dataset
        )

    def create_copy_geo_store_s2_cell_to_full_and_partial_matches_task(self):
        return self.create_dataset_transfer_aws_to_azure_task(
            task_id=self.COPY_GEO_STORE_S2_CELL_TO_FULL_AND_PARTIAL_MATCHES_TASK_ID,
            dataset=source_geo_store_s2_cell_to_full_and_partial_matches_dataset,
            partitioning_args=source_geo_store_s2_cell_to_full_and_partial_matches_dataset.get_partitioning_args(
                ds_date=xcom_pull_str(
                    dag_id=self.dag_id, task_id=self.GET_LATEST_GEO_STORE_DATA_TASK_ID, key=self.KEY_LATEST_S3_GEO_STORE_DATA_PREFIX
                )
            ),
            dst_dataset=dest_geo_store_s2_cell_to_full_and_partial_matches_dataset
        )

    def create_copy_us_zip_adjacency_data_task(self):
        return self.create_dataset_transfer_aws_to_azure_task(
            task_id=self.COPY_US_ZIP_ADJACENCY_DATA_TASK_ID,
            dataset=source_us_zip_adjacency_dataset,
            partitioning_args=source_us_zip_adjacency_dataset.get_partitioning_args(),
            dst_dataset=dest_us_zip_adjacency_dataset
        )
