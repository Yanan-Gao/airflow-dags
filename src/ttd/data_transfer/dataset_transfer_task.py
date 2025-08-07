import dataclasses
import logging
import os
import pathlib
import shutil
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
from datetime import timedelta
from functools import cached_property
from math import ceil
from typing import Dict, Any, Optional, List, Tuple
from pendulum import DateTime

from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.utils.db import provide_session

from ttd.cloud_provider import CloudProvider
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.data_transfer.file_copier_factory import FileCopierFactory

from ttd.datasets.dataset import Dataset
from ttd.datasets.env_path_configuration import MigratingDatasetPathConfiguration, MigratedDatasetPathConfiguration
from ttd.kubernetes.ephemeral_volume_config import EphemeralVolumeConfig
from ttd.workers.worker import Workers
from ttd.kubernetes.k8s_executor_config import K8sExecutorConfig
from ttd.kubernetes.pod_resources import PodResources
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from ttd.tasks.setup_teardown import SetupTeardownTask


class DatasetTransferTask(SetupTeardownTask):
    """
    DatasetTransferTask is designed to transfer datasets between cloud storage providers, such as AWS S3,
    Azure Blob Storage and AliCloud OSS. This task is particularly useful for developers working with datasets
    that need to be copied or migrated across cloud platforms.

    The task takes care of transferring the dataset while support various configurations described below.

    To use this task, simply provide the source and destination cloud providers, dataset and its partitioning
    arguments. The dataset parameter should be an instance of the Dataset class or one of
    its descendants such as HourGeneratedDataset, DateGeneratedDataset, etc. You may need to create a custom dataset
    class if it doesn't already exist in the datasources/ folder. Partitioning arguments should be provided through the
    corresponding method of the dataset class, and you can use Jinja templates for this purpose.

    Optionally, you can configure the number of partitions to divide and distribute your dataset across
    multiple data transfer executors, potentially reducing transfer time. You can also set the maximum number of
    threads, retries, and adjust timeout settings for your particular dataset using the corresponding parameters.
    If you're transferring data within the same cloud provider, the provider's built-in methods are used by default for
    optimization. However, you can override this behaviour to use the default approach with
    Airflow Data Transfer executors. Additionally, if your goal is to entirely clear the destination
    before transferring, make sure to adjust the corresponding parameter since the default behaviour is to overwrite
    existing files.
    If your data transfer involves different regions, specify the relevant parameters to ensure proper
    execution. Make sure that the default user has the required permissions to access datasets in both the source and
    destination buckets. You may need to add these permissions to this user or create your own credentials and use them
    through Airflow connections and pass them to task. To add an Airflow connection, reach out to DATAPROC for
    assistance.

    You can find examples of usage in the demo DAG provided (demo/demo-dataset-transfer-task.py).

    :param name: Task name
    :param dataset: Dataset to be transferred. It is an instance of the Dataset class or one of its descendants
        such as HourGeneratedDataset, DateGeneratedDataset and others.
    :param src_cloud_provider: Source cloud provider (e.g. CloudProviders.aws).
    :param dst_cloud_provider: Destination cloud provider (e.g. CloudProviders.azure).
    :param partitioning_args: Arguments for dataset partitioning (e.g. date, hour).
        This parameter should be obtained by calling the get_partitioning_args method of this dataset
        and passing actual values to it. These values can be passed as Jinja templates (e.g. {{ ds }}).
        See demo job for example.
    :param src_conn_id: Airflow connection id for source cloud provider. Default value is None, in this case
        default credentials are used. If you want to add your connection, please reach out DATAPROC.
    :param dst_conn_id: Airflow connection ID for destination cloud provider. Default value is None, in this case
        default credentials are used. If you want to add your connection, please reach out DATAPROC.
    :param dst_dataset: Destination dataset where data will be copied. Default value is None, in this case
        source dataset (dataset param) is used
    :param dst_partitioning_args: Arguments for destination dataset partitioning. Default value is None, in this case
        source partitioning args (partitioning_args param) are used
    :param dst_region: Destination region for dataset,
        applicable if the data needs to be transferred to a different region
    :param num_partitions: number of transfer tasks to be parallelized across multiple executors
    :param max_threads: Maximum number of threads to be used for data transfer.
    :param max_try_count: Maximum number of retries for each file that cannot be downloaded or uploaded.
    :param transfer_timeout: Timeout for transfer files task
    :param prepare_finalise_timeout: Timeout for preparation and finalisation tasks
    :param force_indirect_copy: When false and if source and destination cloud providers are the same,
        dataset is copied using the cloud provider's built-in methods. If set to True, dataset will instead be
        copied using Airflow Data Transfer executors
    :param drop_dst: When false, destination files are overwritten. If set to True, destination will be entirely cleared
        before the data transfer
    """

    dataset_gb_size_threshold = 2
    bytes_per_gb = 1024**3
    transfer_directory = "/tmp/dataset_transfer_task"

    def __init__(
        self,
        name: str,
        dataset: Dataset,
        src_cloud_provider: CloudProvider,
        dst_cloud_provider: CloudProvider,
        partitioning_args: Dict[str, Any],
        src_conn_id: Optional[str] = None,
        dst_conn_id: Optional[str] = None,
        dst_dataset: Optional[Dataset] = None,
        dst_partitioning_args: Optional[Dict[str, Any]] = None,
        dst_region: Optional[str] = None,
        num_partitions: int = 1,
        max_threads: int = 20,
        max_try_count: int = 3,
        transfer_timeout: timedelta = timedelta(hours=2),
        prepare_finalise_timeout: timedelta = timedelta(minutes=15),
        force_indirect_copy: bool = False,
        drop_dst: bool = False,
    ):
        logging.getLogger("azure").setLevel(logging.WARNING)

        self.name = name
        self.dataset = dataset.as_read().with_cloud(src_cloud_provider)
        self.dst_dataset = (
            dst_dataset.as_write().with_cloud(dst_cloud_provider) if dst_dataset else dataset.as_write().with_cloud(dst_cloud_provider)
        )

        if dst_region is not None:
            self.dst_dataset = self.dst_dataset.with_region(dst_region)

        self.num_partitions = num_partitions
        self.max_threads = max_threads
        self.max_try_count = max_try_count

        self.src_cloud_provider = src_cloud_provider
        self.src_conn_id = src_conn_id
        self.dst_cloud_provider = dst_cloud_provider
        self.dst_conn_id = dst_conn_id
        self.force_indirect_copy = force_indirect_copy

        self.transfer_timeout = transfer_timeout
        self.prepare_finalise_timeout = prepare_finalise_timeout

        self.drop_dst = drop_dst
        self.int_transfer = False

        prepare_data_transfer_task = OpTask(
            op=PythonOperator(
                task_id=f"{self.name}.prepare_data_transfer",
                python_callable=self.prepare_data_transfer,
                provide_context=True,
                op_kwargs={
                    "partitioning_args": partitioning_args,
                    "dst_partitioning_args": dst_partitioning_args,
                },
                execution_timeout=self.prepare_finalise_timeout,
            )
        )

        if self.drop_dst:
            prepare_destination = OpTask(
                op=PythonOperator(
                    task_id=f"{name}.prepare_destination",
                    python_callable=self.prepare_destination,
                    provide_context=True,
                    execution_timeout=self.prepare_finalise_timeout,
                )
            )

            prepare_data_transfer_task = ChainOfTasks(
                task_id=f"{name}.setup_transfer",
                tasks=[prepare_data_transfer_task, prepare_destination],
            )

        complete_files_transfer_task = OpTask(
            op=PythonOperator(
                task_id=f"{name}.complete_files_transfer",
                python_callable=self.complete_files_transfer,
                provide_context=True,
                execution_timeout=self.prepare_finalise_timeout,
            )
        )

        transfer_metadata_files_task = OpTask(
            op=PythonOperator(
                task_id=f"{name}.transfer_metadata_files",
                python_callable=self.transfer_metadata_files,
                provide_context=True,
                queue=Workers.k8s.queue,
                pool=Workers.k8s.pool,
                executor_config=K8sExecutorConfig.light_data_transfer_task(),
                execution_timeout=self.prepare_finalise_timeout,
            )
        )

        super().__init__(
            task_id=name,
            setup_task=prepare_data_transfer_task,
            teardown_task=ChainOfTasks(
                task_id=f"{name}.teardown_data_transfer",
                tasks=[complete_files_transfer_task, transfer_metadata_files_task],
            ),
        )

        for i in range(self.num_partitions):
            transfer_files_task = OpTask(
                op=PythonOperator(
                    task_id=f"{name}.transfer_files_{i}",
                    python_callable=self.transfer_files,
                    op_kwargs={"partition_num": i},
                    provide_context=True,
                    queue=Workers.k8s.queue,
                    pool=Workers.k8s.pool,
                    execution_timeout=self.transfer_timeout,
                )
            )
            self.add_parallel_body_task(transfer_files_task)

    @cached_property
    def src_cloud_storage(self):
        return CloudStorageBuilder(self.src_cloud_provider).set_conn_id(self.src_conn_id).build()

    @cached_property
    def dst_cloud_storage(self):
        return CloudStorageBuilder(self.dst_cloud_provider).set_conn_id(self.dst_conn_id).build()

    def internal_transfer(self, dual_write: bool) -> bool:
        if dual_write:
            return True
        return not self.force_indirect_copy and FileCopierFactory.is_internal(self.src_cloud_storage, self.dst_cloud_storage)

    @staticmethod
    def format_prefix(prefix: str) -> str:
        return "" if not prefix else prefix if prefix.endswith("/") else prefix + "/"

    @staticmethod
    def partition_files(files: List[str], num_partitions: int) -> List[List[str]]:
        partition_size = ceil(len(files) / num_partitions)
        return [files[i:i + partition_size] for i in range(0, len(files), partition_size)]

    def _check_dataset_prefix_exists(self, dataset: Dataset, cloud_storage: CloudStorage, partitioning_args: Dict[str, Any]) -> bool:
        """
        For a dataset, returns True if its prefix exists. Else returns False.
        """
        prefix = self._get_dataset_prefix(dataset=dataset, partitioning_args=partitioning_args)
        return cloud_storage.check_for_prefix(bucket_name=dataset.bucket, delimiter="/", prefix=prefix)

    def _get_dataset_prefix(self, dataset: Dataset, partitioning_args: Dict[str, Any]) -> str:
        """
        Given a dataset, returns its formatted prefix
        """
        parsed_partitioning_args = dataset.parse_partitioning_args(**partitioning_args)
        prefix = dataset._get_full_key(**parsed_partitioning_args)
        return self.format_prefix(prefix)

    def _determine_destination_write_behavior(self, src_partitioning_args: Dict[str, Any]) -> Tuple[bool, Dataset]:
        """
        Determines write logic to destination dataset. For Migrating destination datasets, write location depends on the source dataset.
            1. If the source dataset is Migrating, checks if it exists on only new path/original path, or if on both new and original path. If the source
            exists on both, destination will be written to both. If source exists only on one path, write will be only to the one path.
            2. If the source dataset is Migrated, the write will only happen to the new path of the Migrating destination dataset.
            3. If the source dataset is Existing, the write will only happen to the original path of the Migrating destination dataset (no special logic required).
        For any other destination dataset (Migrated, Existing), write will happen to that dataset's one path.
        :param partitioning_args: partitioning args of source dataset.
        :return: dual_write, dst_dataset
        """
        dual_write = False
        dst_dataset = self.dst_dataset

        # If the destination dataset is Migrating, there may be a need for special logic for dualWrite
        if self.dst_dataset.env_path_configuration_type == MigratingDatasetPathConfiguration:
            # If source is Migrating, check which paths it exists on. Write to whichever env paths source exists on.
            if self.dataset.env_path_configuration_type == MigratingDatasetPathConfiguration:
                original_prefix_exists = self._check_dataset_prefix_exists(
                    dataset=self.dataset, cloud_storage=self.src_cloud_storage, partitioning_args=src_partitioning_args
                )
                new_prefix_exists = self._check_dataset_prefix_exists(
                    dataset=self.dataset.with_new_env_pathing,
                    cloud_storage=self.src_cloud_storage,
                    partitioning_args=src_partitioning_args
                )
                if new_prefix_exists and original_prefix_exists:
                    dual_write = True
                elif new_prefix_exists:
                    # if source only exists on new path, the destination should only be written to the new path
                    dst_dataset = self.dst_dataset.with_new_env_pathing
                elif not original_prefix_exists:
                    raise DatasetTransferTaskError(
                        "Path prefix for Migrating source dataset does not exist on either new path or original path."
                    )

            # If source is Migrated and destination is Migrating, the destination should only be written to the new path
            elif self.dataset.env_path_configuration_type == MigratedDatasetPathConfiguration:
                dst_dataset = self.dst_dataset.with_new_env_pathing
        return dual_write, dst_dataset

    def prepare_data_transfer(
        self,
        partitioning_args: Dict[str, Any],
        dst_partitioning_args: Dict[str, Any],
        execution_date: DateTime,
        **kwargs,
    ) -> Dict[str, Optional[List[Any] | str | bool]]:
        """
        Extracts partitions, src_prefix, dst_prefix, src_bucket, metadata file keys and regular file keys from source dataset.
        If the source dataset is a migrating dataset with smart_read turned on, its new path is checked for the data first.
        If the data check fails, its original path is checked for data.
        For migrating datasets with smart_read turned off, only the original path is checked.
        Migrated and existing datasets have only one path, so the check for data occurs once for such datasets.
        """

        # If the dataset is Migrating and has smart_read enabled, read precedence is given to the data in the new path
        dataset = self.dataset
        if self.dataset.smart_read_and_migrating:
            dataset = self.dataset.with_new_env_pathing

        # Set destination dataset + determine whether we want to dualWrite in our next tasks
        dual_write, dst_dataset = self._determine_destination_write_behavior(src_partitioning_args=partitioning_args)
        self.int_transfer = self.internal_transfer(dual_write)

        parsed_partitioning_args = dataset.parse_partitioning_args(**partitioning_args)
        dst_partitioning_args = (dst_partitioning_args if dst_partitioning_args else partitioning_args)
        formatted_src_prefix = self._get_dataset_prefix(dataset=dataset, partitioning_args=partitioning_args)
        formatted_dst_prefix = self._get_dataset_prefix(dataset=dst_dataset, partitioning_args=dst_partitioning_args)
        src_bucket = dataset.bucket
        dst_bucket = dst_dataset.bucket

        logging.info(f"Source path: {src_bucket}/{formatted_src_prefix}")
        logging.info(f"Destination path: {dst_dataset.bucket}/{formatted_dst_prefix}")

        # Get dual destination prefix if dual_write is True
        formatted_dual_dst_prefix = None
        if dual_write:
            formatted_dual_dst_prefix = self._get_dataset_prefix(
                dataset=self.dst_dataset.with_new_env_pathing, partitioning_args=dst_partitioning_args
            )
            logging.info(f"Second destination path: {self.dst_dataset.with_new_env_pathing.bucket}/{formatted_dual_dst_prefix}")

        prefix_exists = self.src_cloud_storage.check_for_prefix(bucket_name=src_bucket, delimiter="/", prefix=formatted_src_prefix)
        if not prefix_exists and not self.dataset.smart_read_and_migrating:
            raise DatasetTransferTaskError(f"Prefix {formatted_src_prefix} does not exist")

        # First read from src is allowed to fail for a migrating dataset with smart_read enabled, because the data may exist in the original path.
        # An additional check is done for the data in the original path.
        elif not prefix_exists and self.dataset.smart_read_and_migrating:
            logging.info(f"Source path: {src_bucket}/{formatted_src_prefix} contained no data. Trying original source path.")

            dataset = self.dataset
            src_bucket = dataset.bucket
            formatted_src_prefix = self._get_dataset_prefix(dataset=dataset, partitioning_args=partitioning_args)

            logging.info(f"Original source path: {src_bucket}/{formatted_src_prefix}")

            if not self.src_cloud_storage.check_for_prefix(bucket_name=src_bucket, delimiter="/", prefix=formatted_src_prefix):
                raise DatasetTransferTaskError(f"Prefix {formatted_src_prefix} does not exist")

        keys = self.src_cloud_storage.list_keys(bucket_name=src_bucket, prefix=formatted_src_prefix)

        file_keys = [key for key in keys if key != formatted_src_prefix]
        (
            metadata_file_keys,
            regular_file_keys,
        ) = self._extract_metadata_and_regular_file_keys(
            keys=file_keys,
            parsed_partitioning_args=parsed_partitioning_args,
            formatted_src_prefix=formatted_src_prefix,
            dataset=dataset,
        )
        self._set_pod_resources(execution_date=execution_date, regular_file_keys=regular_file_keys, bucket=src_bucket)

        partitions = self.partition_files(sorted(regular_file_keys), self.num_partitions)
        logging.info(f"partitions={partitions}")
        logging.info(f"metadata_file_keys={metadata_file_keys}")

        dual_partitions = dual_metadata_file_keys = None
        if dual_write:
            dual_regular_file_keys: List[str] = []
            dual_metadata_file_keys: List[str] = []  # type: ignore

            for regular_file_key in regular_file_keys:
                dual_regular_file_key = formatted_dst_prefix + regular_file_key.removeprefix(formatted_src_prefix)
                dual_regular_file_keys.append(dual_regular_file_key)
            for metadata_file_key in metadata_file_keys:
                dual_metadata_key = formatted_dst_prefix + metadata_file_key.removeprefix(formatted_src_prefix)
                dual_metadata_file_keys.append(dual_metadata_key)  # type: ignore
            dual_partitions = self.partition_files(sorted(dual_regular_file_keys), self.num_partitions)
            logging.info(f"second write partitions={dual_partitions}")
            logging.info(f"second write metadata_file_keys={dual_metadata_file_keys}")

        return {
            "partitions": partitions,
            "dual_partitions": dual_partitions,
            "metadata_file_keys": metadata_file_keys,
            "dual_metadata_file_keys": dual_metadata_file_keys,
            "src_bucket": src_bucket,
            "dst_bucket": dst_bucket,
            "formatted_src_prefix": formatted_src_prefix,
            "formatted_dst_prefix": formatted_dst_prefix,
            "formatted_dual_dst_prefix": formatted_dual_dst_prefix,
            "dual_write": dual_write,
        }

    def prepare_destination(self, task_instance: TaskInstance):
        transfer_details = task_instance.xcom_pull(task_ids=f"{self.name}.prepare_data_transfer")
        self._prepare_destination(transfer_details=transfer_details)

        dual_write = transfer_details["dual_write"]
        if dual_write:
            logging.info("Repeating prepare_destination task for second dataset destination.")
            self._prepare_destination(transfer_details=transfer_details, dual_write=True)

    def _prepare_destination(self, transfer_details: Any, dual_write: bool = False) -> None:
        if not self.drop_dst:
            logging.info("Destination folder cleanup is disabled (drop_dst=False), completing this step")
            return

        formatted_dst_prefix = transfer_details["formatted_dual_dst_prefix"] if dual_write else transfer_details["formatted_dst_prefix"]
        dst_dataset = self.dst_dataset if dual_write else self.dst_dataset.with_new_env_pathing
        self.dst_cloud_storage.remove_objects(bucket_name=dst_dataset.bucket, prefix=formatted_dst_prefix)
        logging.info(f"Cleanup of the prefix {formatted_dst_prefix} "
                     f"in the destination bucket {dst_dataset.bucket} has been completed")

    def transfer_files(self, partition_num: int, task_instance: TaskInstance) -> None:
        transfer_details = task_instance.xcom_pull(task_ids=f"{self.name}.prepare_data_transfer")
        self._transfer_files(partition_num=partition_num, dst_dataset=self.dst_dataset, transfer_details=transfer_details)

        dual_write = transfer_details["dual_write"]
        if dual_write:
            logging.info("Repeating transfer_files task for second dataset destination.")
            # repeat transfer files for destination dataset on new env pathing
            self._transfer_files(
                partition_num=partition_num,
                dst_dataset=self.dst_dataset.with_new_env_pathing,
                transfer_details=transfer_details,
                dual_write=True
            )

    def _transfer_files(self, partition_num: int, dst_dataset: Dataset, transfer_details: Any, dual_write: bool = False) -> None:
        partitions = transfer_details["dual_partitions"] if dual_write else transfer_details["partitions"]
        partition = partitions[partition_num] if len(partitions) > partition_num else []
        logging.info(f"partitions[{partition_num}]={partition}")

        # if this is a dual_write, we take the data copied to the first destination path as the source, to reduce overhead from
        # copying between different cloud providers.
        formatted_src_prefix = transfer_details["formatted_dst_prefix"] if dual_write else transfer_details["formatted_src_prefix"]
        src_bucket = transfer_details["dst_bucket"] if dual_write else transfer_details["src_bucket"]
        src_cloud_storage = self.dst_cloud_storage if dual_write else self.src_cloud_storage

        formatted_dst_prefix = transfer_details["formatted_dual_dst_prefix"] if dual_write else transfer_details["formatted_dst_prefix"]

        logging.info(f"Source path: {src_bucket}/{formatted_src_prefix}")
        logging.info(f"Destination path: {dst_dataset.bucket}/{formatted_dst_prefix}")

        partition_ok, partition_success_key = self._get_transfer_state(formatted_dst_prefix, [self.name, str(partition_num)], dst_dataset)
        if partition_ok:
            logging.info(f"Partition {partition_num} has already been transferred, skipping.")
            return

        internal_transfer = self.internal_transfer(dual_write=dual_write)
        copier_factory = FileCopierFactory(
            src_bucket=src_bucket,
            dst_bucket=dst_dataset.bucket,
            src_prefix=formatted_src_prefix,
            dst_prefix=formatted_dst_prefix,
            src_cloud_storage=src_cloud_storage,
            dst_cloud_storage=self.dst_cloud_storage,
            max_try_count=self.max_try_count,
            try_internal_transfer=internal_transfer,
            transfer_directory=self.transfer_directory,
        )
        if internal_transfer:
            logging.info("Copying through storage API as internal transfer")
        else:
            logging.info(f"Temporary directory for files: {copier_factory.tmp_dir_path}")

        try:
            with ThreadPoolExecutor(self.max_threads) as executor:
                futures = []
                for src_key in partition:
                    file_copier = copier_factory.get_copier(src_key)
                    futures.append(executor.submit(file_copier.copy_file))

                done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
                ex = next((thread.exception() for thread in done if thread.exception()), None)
                if ex:
                    logging.error("Failed to copy file", exc_info=ex)
                    for future in futures:
                        future.cancel()
                    raise DatasetTransferTaskError(str(ex))

                self.dst_cloud_storage.put_object(
                    bucket_name=dst_dataset.bucket,
                    key=partition_success_key,
                    body=str(len(partition)),
                    replace=True,
                )
        finally:
            if not internal_transfer:
                self._delete_tmp_dir(copier_factory.tmp_dir_path)

        logging.info("Transfer successful")

    def complete_files_transfer(self, task_instance: TaskInstance) -> None:
        transfer_details = task_instance.xcom_pull(task_ids=f"{self.name}.prepare_data_transfer")
        self._complete_files_transfer(dst_dataset=self.dst_dataset, transfer_details=transfer_details)

        dual_write = transfer_details["dual_write"]
        if dual_write:
            logging.info("Repeating complete_files_transfer task for second dataset destination.")
            self._complete_files_transfer(
                dst_dataset=self.dst_dataset.with_new_env_pathing, transfer_details=transfer_details, dual_write=True
            )

    def _complete_files_transfer(self, dst_dataset: Dataset, transfer_details: Any, dual_write: bool = False) -> None:
        formatted_dst_prefix = transfer_details["formatted_dual_dst_prefix"] if dual_write else transfer_details["formatted_dst_prefix"]

        self.dst_cloud_storage.remove_objects(
            bucket_name=dst_dataset.bucket,
            prefix=os.path.normpath(os.path.join(formatted_dst_prefix, self.name)),
        )

    def transfer_metadata_files(self, task_instance: TaskInstance) -> None:
        transfer_details = task_instance.xcom_pull(task_ids=f"{self.name}.prepare_data_transfer")
        self._transfer_metadata_files(dst_dataset=self.dst_dataset, transfer_details=transfer_details)

        dual_write = transfer_details["dual_write"]
        if dual_write:
            logging.info("Repeating transfer_metadata_files task for second dataset destination.")
            self._transfer_metadata_files(
                dst_dataset=self.dst_dataset.with_new_env_pathing, transfer_details=transfer_details, dual_write=True
            )

    def _transfer_metadata_files(self, dst_dataset: Dataset, transfer_details: Any, dual_write: bool = False) -> None:

        metadata_file_keys = transfer_details["dual_metadata_file_keys"] if dual_write else transfer_details["metadata_file_keys"]
        logging.info(f"metadata_file_keys={metadata_file_keys}")

        # if this is a dual_write, we take the data copied to the first destination path as the source, to reduce overhead from
        # copying between different cloud providers.
        formatted_src_prefix = transfer_details["formatted_dst_prefix"] if dual_write else transfer_details["formatted_src_prefix"]
        src_bucket = transfer_details["dst_bucket"] if dual_write else transfer_details["src_bucket"]
        src_cloud_storage = self.dst_cloud_storage if dual_write else self.src_cloud_storage
        formatted_dst_prefix = transfer_details["formatted_dual_dst_prefix"] if dual_write else transfer_details["formatted_dst_prefix"]

        internal_transfer = self.internal_transfer(dual_write=dual_write)
        copier_factory = FileCopierFactory(
            src_bucket=src_bucket,
            dst_bucket=dst_dataset.bucket,
            src_prefix=formatted_src_prefix,
            dst_prefix=formatted_dst_prefix,
            src_cloud_storage=src_cloud_storage,
            dst_cloud_storage=self.dst_cloud_storage,
            max_try_count=self.max_try_count,
            try_internal_transfer=internal_transfer,
            transfer_directory=self.transfer_directory,
            check_existence=False,
        )
        try:
            for metadata_file_key in metadata_file_keys:
                file_copier = copier_factory.get_copier(metadata_file_key)
                file_copier.copy_file()
        finally:
            if not internal_transfer:
                self._delete_tmp_dir(copier_factory.tmp_dir_path)

    def _get_transfer_state(self, prefix: str, key_parts: List[str], dst_dataset: Dataset) -> Tuple[bool, str]:
        key_parts[-1] = self._get_success_file_name(key_parts[-1])
        transfer_success_key = os.path.normpath(os.path.join(prefix, *key_parts))
        return (
            self.dst_cloud_storage.check_for_key(key=transfer_success_key, bucket_name=dst_dataset.bucket),
            transfer_success_key,
        )

    @staticmethod
    def _get_success_file_name(name_prefix: str) -> str:
        return f"_{name_prefix}_success"

    def _extract_metadata_and_regular_file_keys(
        self,
        keys: List[str],
        parsed_partitioning_args: Dict[str, Any],
        formatted_src_prefix: str,
        dataset: Dataset,
    ) -> Tuple[List[str], List[str]]:
        metadata_file_keys = (dataset.get_metadata_file_full_keys(**parsed_partitioning_args) or [])
        success_file_key = dataset.get_success_file_full_key(**parsed_partitioning_args)
        if success_file_key and formatted_src_prefix != self.format_prefix(success_file_key):
            metadata_file_keys.append(success_file_key)
        regular_file_keys = [key for key in keys if key not in metadata_file_keys]

        return metadata_file_keys, regular_file_keys

    def _set_pod_resources(self, execution_date: DateTime, regular_file_keys: List[str], bucket: str):
        total_size = self._get_files_total_size(file_keys=regular_file_keys, bucket=bucket)
        pod_resources = self._estimate_pod_resources_from_size(total_size / self.num_partitions)

        logging.info(f"total_size={total_size} GB")
        logging.info(f"pod_resources={dataclasses.asdict(pod_resources)}")

        self._push_pod_resources_for_tasks(pod_resources, execution_date)

    def _get_files_total_size(self, file_keys: List[str], bucket: str) -> float:
        return sum(self.src_cloud_storage.get_file_size(key=file_key, bucket_name=bucket) / self.bytes_per_gb for file_key in file_keys)

    def _estimate_pod_resources_from_size(self, size: float) -> PodResources:
        # There's nothing special about the divisors, they're mostly arbitrary to put size=100 towards the top end
        request_cpu = self._bound_and_round(size / 20, 0.5, 6)
        request_memory = self._bound_and_round(size / 20, 1, 6)
        limit_memory = self._bound_and_round(size / 15, 1.5, 10)

        if self.int_transfer:
            request_memory = self._bound_and_round(request_memory, 1, 3)
            limit_memory = self._bound_and_round(limit_memory, 1.5, 6)
            logging.info("Forcing lower pod resources for internal transfer")

        return PodResources(
            request_cpu=f"{request_cpu}",
            request_memory=f"{request_memory}Gi",
            limit_memory=f"{limit_memory}Gi",
            ephemeral_volume_config=EphemeralVolumeConfig(
                name="data-transfer-ephemeral-ebs",
                storage_request="50Gi",
                storage_class_name="fast-ssd-encrypted",
                mount_path=self.transfer_directory
            ),
        )

    def _bound_and_round(self, value: float, lower_bound: float, upper_bound: float) -> float:
        if value < lower_bound:
            return_val = lower_bound
        elif value > upper_bound:
            return_val = upper_bound
        else:
            return_val = value
        return round(return_val, 1)

    @provide_session
    def _push_pod_resources_for_tasks(
        self,
        pod_resources: PodResources,
        execution_date: DateTime,
        session: Optional[Session] = None,  # type: ignore
    ) -> None:
        for transfer_files_task in self._body_tasks:
            op = transfer_files_task.first_airflow_op()
            ti = op.get_task_instances(start_date=execution_date, end_date=execution_date, session=session)[0]

            ti.xcom_push(key="pod_resources", value=dataclasses.asdict(pod_resources), session=session)

    @staticmethod
    def _delete_tmp_dir(dir_path: str) -> None:
        if pathlib.Path(dir_path).exists():
            logging.info(f"Deleting directory: {dir_path}")
            shutil.rmtree(dir_path)
            logging.info(f"Directory deleted: {dir_path}")
        else:
            logging.info(f"Directory not found: {dir_path}")


class DatasetTransferTaskError(Exception):
    pass
