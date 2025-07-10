import copy
import logging
from abc import ABC, abstractmethod
from datetime import date
from functools import cached_property
from typing import Any, Dict, List, Optional, Tuple, TypeVar

from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.datasets.ds_side import DsSide, DsSides
from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration, ExistingDatasetPathConfiguration, MigratedDatasetPathConfiguration, MigratingDatasetPathConfiguration
)
from ttd.monads.trye import Try
from ttd.ttdenv import TtdEnv, TtdEnvFactory

default_short_date_part_format: str = "%Y%m%d"
default_date_part_format: str = "date=%Y%m%d"
default_hour_part_format: str = "hour={hour:0>2d}"
SUCCESS: str = "_SUCCESS"


def raise_(ex: Exception):
    raise ex


T = TypeVar("T", bound="Dataset")


class Dataset(ABC):

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        env_aware: bool = True,
        eldorado_class: Optional[str] = None,
        cloud_provider: CloudProvider = CloudProviders.aws,
        azure_bucket: Optional[str] = None,
        oss_bucket: Optional[str] = None,
        metadata_files: Optional[List[str]] = None,
        buckets_for_other_regions: Optional[Dict[str, str]] = None,
        env_path_configuration: EnvPathConfiguration = ExistingDatasetPathConfiguration(),
    ):
        self.bucket = bucket
        self.azure_bucket = azure_bucket
        self._path_prefix = path_prefix
        self.data_name = data_name
        self.version = version
        self.success_file_template = success_file
        self.env_aware = env_aware  # note that env_aware is deprecated for datasets with MigratedDatasetPathConfiguration, because migrated datasets are env_aware by default
        self.env = TtdEnvFactory.get_from_system()
        self.side = DsSides.read
        self.eldorado_class = eldorado_class
        self.cloud_provider = cloud_provider
        self.oss_bucket = oss_bucket
        self.metadata_files = metadata_files
        self.buckets_for_other_regions = buckets_for_other_regions
        self.env_path_configuration = env_path_configuration

    @abstractmethod
    def get_partitioning_args(self, **kwargs) -> Dict[str, Any]:
        pass

    @abstractmethod
    def parse_partitioning_args(self, **kwargs) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _get_full_key(self, **kwargs) -> str:
        pass

    def get_full_path(self, **kwargs) -> str:
        """
        Get full path to the location of particular partition (if there is partition) of dataset
        """
        return f"{self.protocol}://{self.bucket}/{self._get_full_key(**kwargs)}"

    def get_read_path(self, **kwargs) -> str:
        return self.get_full_path(**kwargs)

    @abstractmethod
    def get_dataset_path(self, **kwargs) -> str:
        pass

    @property
    def path_prefix(self) -> str:
        """
        If a dataset is migrated to new env pathing, it is always created on a env={prod}|{test}/ path scheme.
        """
        env = self._get_ds_env()
        return (
            f"env={env}/{self._path_prefix}"
            if isinstance(self.env_path_configuration, MigratedDatasetPathConfiguration) else self._path_prefix
        )

    def get_success_file_full_key(self, **kwargs) -> Optional[str]:
        success_file = self._get_success_file(**kwargs)
        return (
            None if success_file is None else
            self._get_success_file_path(**kwargs) if success_file == "" else f"{self._get_success_file_path(**kwargs)}{success_file}"
        )

    def _get_success_file_path(self, **kwargs) -> str:
        """
        Returns the path to the success file storage.
        Usually it is the path which is used by Spark to store partition and hence success file is written there.
        Usually it doesn't give much value as on every partition update success file got overwritten unless it is suffixed for every
        partition update or placed to different place per write operation.
        :param kwargs:
        :return:
        """
        return self._get_full_key(**kwargs)

    def _get_success_file(self, **kwargs) -> Optional[str]:
        return (
            None if self.success_file_template is None else f"/{self.success_file_template}" if self.success_file_template != "" else ""
        )

    @property
    def success_file(self) -> Optional[str]:
        """
        Formats and return success file part of the path if it exist.
        :return:
        """
        return self._get_success_file()

    def _get_metadata_files(self) -> Optional[List[str]]:
        return ([f"/{file}" for file in self.metadata_files] if self.metadata_files else None)

    def get_metadata_file_full_keys(self, **kwargs) -> Optional[List[str]]:
        return ([f"{self._get_full_key(**kwargs)}{file}" for file in self._get_metadata_files()] if self.metadata_files else None)

    def _get_check_paths(self, **kwargs) -> List[str]:
        return ([f"{self._get_success_file_path(**kwargs)}{self._get_success_file(**kwargs)}"]
                if self.success_file is not None else [self._get_full_key(**kwargs)])

    def _get_ds_env(self) -> str:
        return (
            self.env.dataset_read_env if self.side == DsSides.read else
            self.env.dataset_write_env if self.side == DsSides.write else raise_(TypeError(f"Side {DsSide} is not supported."))
        )

    def _get_env_path_part(self, **kwargs) -> str:
        """
        Format environment part of the path, depending on env and env_aware dataset properties.
        If a dataset is migrated, _get_env_path_part returns an empty string to prevent env duplication in dataset paths.
        Note that 'env_aware' is a deprecated variable for Migrated Datasets, because migrated datasets are env_aware by default.
        """
        if not isinstance(self.env_path_configuration, MigratedDatasetPathConfiguration):
            env_str = self._get_ds_env()
            return f"/{env_str}" if self.env_aware else ""
        return ""

    def with_env(self: T, env: TtdEnv, **kwargs) -> T:
        """
        Creates copy of current dataset configuring environment to specified value.
        :param env: Environment of dataset that need to be set
        :return: Copy of dataset instance.
        """
        if self.env == env:
            return self
        v = copy.copy(self)
        v.env = env
        return v

    def with_side(self: T, side: DsSide) -> T:
        """
        Creates copy of current dataset configuring side to specified value.
        :param side: Side (read, write) of dataset that need to be set
        :return: Copy of dataset instance.
        """
        if self.side == side:
            return self
        v = copy.copy(self)
        v.side = side
        return v

    @property
    def env_path_configuration_type(self) -> EnvPathConfiguration:
        return type(self.env_path_configuration)

    @cached_property
    def with_new_env_pathing(self: T) -> T:
        """
        Meant for use only for Migrating Datasets. For non-migrating (migrated, old), will just return itself.
        For migrating datasets, will return the Migrated version of the dataset, with bucket = new_bucket, prefix = new_prefix,
        buckets_for_other_regions = new_buckets_for_other_regions, and env_path_configuration = MigratedEnvPathConfiguration().
        If dataset has provider == azure or ali and has a valid bucket for that provider, sets the bucket instead to that.
        Used in DatasetTransferTask and self.check_data_exist().
        :return: self as migrated dataset
        """
        if isinstance(self.env_path_configuration, MigratingDatasetPathConfiguration):
            v = copy.copy(self)
            v.bucket = (
                self.azure_bucket if self.cloud_provider == CloudProviders.azure and self.azure_bucket is not None else self.oss_bucket
                if self.cloud_provider == CloudProviders.ali and self.oss_bucket is not None else self.env_path_configuration.new_bucket
            )
            v._path_prefix = v.env_path_configuration.new_path_prefix
            v.buckets_for_other_regions = (v.env_path_configuration.new_buckets_for_other_regions)
            v.env_path_configuration = MigratedDatasetPathConfiguration()
            return v
        else:
            return self

    def as_read(self: T) -> T:
        """
        Creates copy of the dataset with side equals to read.
        """
        return self.with_side(DsSides.read)

    def as_write(self: T) -> T:
        """
        Creates copy of the dataset with side equals to write.
        """
        return self.with_side(DsSides.write)

    def with_cloud(self: T, cloud_provider: CloudProvider) -> T:
        """
        Creates copy of current dataset hardcoding `cloud_provider` property to specified value.
        :param cloud_provider: Cloud provider (AWS or Azure) of the dataset
        :return: Copy of dataset instance
        """
        if self.cloud_provider == cloud_provider:
            return self
        v = copy.copy(self)

        v.bucket = (
            self.azure_bucket if cloud_provider == CloudProviders.azure and self.azure_bucket is not None else
            self.oss_bucket if cloud_provider == CloudProviders.ali and self.oss_bucket is not None else self.bucket
        )
        v.cloud_provider = cloud_provider

        return v

    def with_region(self: T, region: str) -> T:
        """
        Creates a copy of the current dataset, with an updated bucket based on the provided region.
        If the current dataset is a MigratingDataset, it also adjusts the new bucket in the env_path_configuration based on the provided region.
        Throws an exception if no mapping of region to bucket exists
        :param region: The region of the dataset
        :return: Copy of the dataset instance
        """

        region_bucket = self.buckets_for_other_regions.get(region)

        if region_bucket is None:
            raise Exception(f"Region {region} not found in mapping of buckets")

        # For migrating datasets, the new bucket has to be set based on the region as well.
        if isinstance(self.env_path_configuration, MigratingDatasetPathConfiguration):
            new_region_bucket = (self.env_path_configuration.new_buckets_for_other_regions.get(region))

            if new_region_bucket is None:
                raise Exception(f"Region {region} not found in mapping of buckets in MigratingEnvPathConfiguration env_path_configuration.")

            if (self.bucket == region_bucket and self.env_path_configuration.new_bucket == new_region_bucket):
                return self

            v = copy.copy(self)
            v.bucket = region_bucket
            v.env_path_configuration.new_bucket = new_region_bucket
            return v

        if self.bucket == region_bucket:
            return self

        v = copy.copy(self)
        v.bucket = region_bucket
        return v

    def get_root_path(self) -> str:
        """
        Get the base location where the data name path exists. Bucket + path prefix.

        :return: CloudStorage Path, Bucket + Prefix
        :rtype: str
        """
        return f"{self.protocol}://{self.bucket}/{self.path_prefix}"

    @property
    def smart_read_and_migrating(self) -> bool:
        return isinstance(self.env_path_configuration, MigratingDatasetPathConfiguration) and self.env_path_configuration.smart_read

    @abstractmethod
    def check_recent_data_exist(
        self,
        cloud_storage: CloudStorage,
        ds_date: date,
        max_lookback: int = 0,
        **kwargs,
    ) -> Try[date]:
        pass

    def check_data_exist(self, cloud_storage: CloudStorage, **kwargs) -> bool:
        """
        Check if data for a dataset exists.
        For migrating datasets with smart_read enabled, the check_data_exist is done first on the new env path of the dataset.
        If this check fails, the process repeats for the original dataset path.
        For migrating datasets with smart_read disabled, the check is only done on the original path.
        Migrated and Existing datasets have only one path configuration, so the check is done on their one path.
        :param cloud_storage: Cloud hook to query CloudStorage (AWS S3 or Azure Blob Storage)
        :return: If data exists.
        :rtype: bool
        """
        data_exists_on_new_path = False
        # if this is a smartReading, migrating dataset, we check the new path first. If that returns False, we try the old path.
        if self.smart_read_and_migrating:
            logging.info("Dataset is migrating and has smart_read enabled. Attempting to read from new env path.")
            dataset_on_new_env_path = self.with_new_env_pathing
            data_exists_on_new_path = Dataset.check_data_exist(dataset_on_new_env_path, cloud_storage=cloud_storage, **kwargs)
            if data_exists_on_new_path:
                return True
            else:
                logging.info("Read from new env path failed. Attempting to read from original path.")
        if not data_exists_on_new_path:
            paths_to_check = self._get_check_paths(**kwargs)
            logging.info(
                f'Check data existence of "{self.data_name}" at {[f"{self.protocol_logging}://{self.bucket}/{path}" for path in paths_to_check]}'
            )
            check_result = all(
                cloud_storage.check_for_key(key=path, bucket_name=self.bucket) if self.success_file is not None else cloud_storage
                .check_for_prefix(prefix=path, bucket_name=self.bucket, delimiter="/") for path in paths_to_check
            )
            if not check_result:
                logging.warning(f'Target path/file of "{self.data_name}" dataset is not found.')
                return False
        return True

    @property
    def in_chain_config(self) -> Tuple[str, str]:
        """
        Configuration tuple that is used for Spark jobs in ElDorado to indicate that this dataset is used in chain
        in the DAG and should be treated as such, meaning that in `prodTest` mode it should read from the `test` path instead of `prod`.

        :return: Configuration tuple of the format ("ttd.DataSetClassName.isInChain", "true") that can be passed as parameter
            to spark-submit command
        """
        if self.eldorado_class is None:
            raise Exception(f"eldorado_class property is not defined for dataset {self.data_name}")
        return f"ttd.{self.eldorado_class}.isInChain", "true"

    @property
    def protocol(self):
        return ("wasbs" if self.cloud_provider == CloudProviders.azure else "oss" if self.cloud_provider == CloudProviders.ali else "s3")

    @property
    def protocol_logging(self):
        return self.protocol


class DatasetNotFoundException(Exception):

    def __init__(self, dataset: Dataset, message: str):
        super().__init__(message)
        self.dataset = dataset


"""
    Standard Dataset Dependency checks
"""


def get_dependency_check_result_date(dag_name: str, task_id: str = "initialize_run_date") -> str:
    """
    Returns the xcom_pull string for the date result of check_dependency_datasets

    :param dag_name: name of dag
    :type dag_name: str
    :return: xcom_pull string
    :rtype: str
    """
    return f'{{{{ task_instance.xcom_pull(dag_id="{dag_name}", task_ids="{task_id}", key="data_date") }}}}'
