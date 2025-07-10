import copy
import logging
from abc import ABCMeta, abstractmethod
from datetime import date, timedelta, datetime
from typing import Optional, Dict, Any, Union, List

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.datasets.dataset import (
    Dataset,
    SUCCESS,
    T,
    DatasetNotFoundException,
)
from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration,
    ExistingDatasetPathConfiguration,
)
from ttd.monads.trye import Try, Failure, Success


class DateDataset(Dataset, metaclass=ABCMeta):
    date_partitioning_arg_format = "%Y-%m-%d"

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        date_format: str,
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        env_aware: bool = True,
        eldorado_class: Optional[str] = None,
        azure_bucket: Optional[str] = None,
        oss_bucket: Optional[str] = None,
        metadata_files: Optional[List[str]] = None,
        buckets_for_other_regions: Optional[Dict[str, str]] = None,
        env_path_configuration: EnvPathConfiguration = ExistingDatasetPathConfiguration(),
    ):
        super().__init__(
            bucket=bucket,
            path_prefix=path_prefix,
            data_name=data_name,
            version=version,
            success_file=success_file,
            env_aware=env_aware,
            eldorado_class=eldorado_class,
            azure_bucket=azure_bucket,
            oss_bucket=oss_bucket,
            metadata_files=metadata_files,
            buckets_for_other_regions=buckets_for_other_regions,
            env_path_configuration=env_path_configuration,
        )
        self.date_format = date_format
        self.lookback = None

    def get_partitioning_args(self, ds_date: Union[date, str], **kwargs) -> Dict[str, Any]:
        """
        Get partitioning arguments

        :param ds_date: Dataset date partition. Expected format: %Y-%m-%d
        :return: Partitioning arguments dictionary
        :rtype: Dict[str, Any]
        """
        return {"ds_date": ds_date}

    def parse_partitioning_args(self, ds_date: Union[date, str], **kwargs) -> Dict[str, Any]:
        logging.info(f"Partitioning args {ds_date}")
        if isinstance(ds_date, str):
            ds_date = datetime.strptime(ds_date, self.date_partitioning_arg_format)
        return {"ds_date": ds_date}

    @abstractmethod
    def _get_full_key(self, ds_date: Optional[date] = None, **kwargs) -> str:
        pass

    def _get_success_file(self, ds_date: date, **kwargs) -> Optional[str]:
        return (
            None if self.success_file_template is None else
            f"/{ds_date.strftime(self.success_file_template)}" if self.success_file_template != "" else ""
        )

    @property
    def success_file(self) -> Optional[str]:
        return self._get_success_file(ds_date=date.today())

    def with_lookback(self: T, lookback: int, **kwargs) -> T:
        """
        Creates copy of current dataset hardcoding `lookback` property to specified value.

        Commonly used with DatasetCheckOperator or DependencyOperator to provide lookback value for data check if this value different
        from general lookback.

        Example:

        check_incoming_data_exists = DatasetCheckOperator(dag, [
            Datasources.gracenote.external.device_hashed_ip.with_lookback(hashedIp_lookback),
            Datasources.gracenote.external.device_geo
        ])

        :param lookback:
        :param kwargs:
        :return:
        """
        if self.lookback == lookback:
            return self
        c = copy.copy(self)
        c.lookback = lookback
        return c

    def check_data_exist(self, cloud_storage: CloudStorage, ds_date: date, lookback: int = 0, **kwargs) -> bool:
        """
        Check if data for a dataset exists

        :param cloud_storage: Cloud hook to query CloudStorage (AWS S3 or Azure Blob Storage)
        :type cloud_storage: CloudStorage
        :param ds_date: Date of dataset to check
        :param lookback: Days of data to check for existence, if lookback is 0 then only specified day is checked.
                Will be overridden by `self.lookback` if specified through `with_lookback(int)`.
        :return: If data exists.
        :rtype: bool
        """
        logging.info(f'Check existence of "{self.data_name}" dataset on "{ds_date}" at "{self.get_dataset_path()}"')

        lookback = self.lookback if self.lookback is not None else lookback

        # if lookback is 0 then we should check at least current day
        for daysBack in range(lookback + 1):
            if not super(DateDataset, self).check_data_exist(
                    cloud_storage=cloud_storage,
                    ds_date=(ds_date - timedelta(days=daysBack)),
                    **kwargs,
            ):
                return False  # raise AirflowNotFoundException

        return True

    def check_recent_data_exist(
        self,
        cloud_storage: CloudStorage,
        ds_date: date,
        max_lookback: int = 0,
        **kwargs,
    ) -> Try[date]:
        """
        Check if data exist back in history,
        max_lookback limits how back into history to look when searching for data.
        If data not found within the limit `Failure` will be returned, otherwise search will be stopped upon
        finding existing date and `Success` with date will be returned.

        :param cloud_storage: Cloud hook to query CloudStorage (AWS S3 or Azure Blob Storage)
        :param ds_date:
        :param max_lookback: Maximum number of days to look into history when searching for data existence of this dataset.
                0 means checking specified `ds_date` only. Default: 0.
        :param kwargs:
        :return: `Success` holding the date if data found within max_lookback limit, otherwise `Failure[DatasetNotFoundException]`.
        """

        def check_recent_date(days_back: int) -> Try[date]:
            check_date = ds_date - timedelta(days=days_back)
            check_result = super(DateDataset, self).check_data_exist(cloud_storage=cloud_storage, ds_date=check_date, **kwargs)

            if not check_result:
                if days_back < max_lookback:
                    return check_recent_date(days_back + 1)
                else:
                    err_mgs = f'No Success file is found for "{self.data_name}" data up to "{check_date.strftime("%Y-%m-%d")}" back in history'
                    logging.error(err_mgs)
                    return Failure(DatasetNotFoundException(self, err_mgs))
            logging.info(f'"{self.data_name}" found.')
            return Success(check_date)

        logging.info(f'Checking "{self.data_name}" dataset starting on "{ds_date.strftime("%Y-%m-%d")}"')
        return check_recent_date(0)
