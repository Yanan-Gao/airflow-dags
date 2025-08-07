import copy
import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Union

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.datasets.dataset import (SUCCESS, DatasetNotFoundException, T, default_date_part_format, default_time_part_format)
from ttd.datasets.date_dataset import DateDataset
from ttd.datasets.env_path_configuration import (EnvPathConfiguration, ExistingDatasetPathConfiguration)
from ttd.monads.trye import Failure, Success, Try


class TimeDataset(DateDataset):
    """
    Use for datasets that contain the full time, not just the hour

    :param bucket: CloudStorage Bucket
    :type bucket: str
    :param path_prefix: CloudStorage Path Prefix
    :type path_prefix: str
    :param data_name: CloudStorage Data Name
    :type data_name: str
    :param path_postfix: CloudStorage Path Postfix that comes after the time
    :type path_postfix: str
    :param version: Version of the Data, appended to CloudStorage path including a 'v='
    :type version: Optional[int]
    :param date_format: Format of the date component of the path to dataset. Default: "date=%Y%m%d"
    :type date_format: str
    :param time_format: Format of the time part of the path to dataset. Default: "time=%H%M%S"
    :param success_file: Override the default success file for the dataset. If None, then presence of folder (prefix) is checked,
        otherwise presence of success file (CloudStorage key) as specified in this param is checked.
    :type success_file: str
    :param env_aware: Specify if this dataset is environment aware, in other words does CloudStorage key contain `env` part to it such as
        "prod" or "test"
    """

    date_partitioning_arg_format = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        path_postfix: str = "",
        version: Optional[int] = 1,
        date_format: str = default_date_part_format,
        time_format: str = default_time_part_format,
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
            azure_bucket=azure_bucket,
            path_prefix=path_prefix,
            data_name=data_name,
            version=version,
            date_format=date_format,
            success_file=success_file,
            env_aware=env_aware,
            eldorado_class=eldorado_class,
            oss_bucket=oss_bucket,
            metadata_files=metadata_files,
            buckets_for_other_regions=buckets_for_other_regions,
            env_path_configuration=env_path_configuration,
        )
        self.path_postfix = path_postfix
        self.time_format = time_format

    def get_dataset_path(self, **kwargs) -> str:
        """
        Get Full CloudStorage path, no postfixes.

        :return: CloudStorage Path
        :rtype: str
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" and self.data_name is not None else ""

        return f"{self.get_root_path()}{env_str}{data_name_str}"

    def get_partitioning_args(self, ds_date: Union[datetime, str], **kwargs) -> Dict[str, Any]:
        """
        Get partitioning arguments

        :param ds_date: Dataset date partition. Expected format: %Y-%m-%d %H:%M:%S
        :return: Partitioning arguments dictionary
        :rtype: Dict[str, Any]
        """
        return {"ds_date": ds_date}

    def parse_partitioning_args(self, ds_date: Union[datetime, str], **kwargs) -> Dict[str, Any]:
        if isinstance(ds_date, str):
            ds_date = datetime.strptime(ds_date, self.date_partitioning_arg_format)
        return {"ds_date": ds_date}

    def _get_full_key(self, ds_date: Optional[datetime] = None, **kwargs) -> str:
        """
        Get CloudStorage key path, including env, version, date_str if version and ds_date are provided
        Does not include CloudStorage Bucket.
        :param ds_date: Date partition of dataset
        :type ds_date: date
        :return: CloudStorage Dataset Key
        :rtype: str
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""

        check_datetime = ds_date if ds_date is not None else datetime.now()
        date_str = f"/{check_datetime.strftime(self.date_format)}"
        time_str = f"/{check_datetime.strftime(self.time_format)}"
        path_postfix = f"/{self.path_postfix}" if self.path_postfix != "" else ""
        return (f"{self.path_prefix}{env_str}{data_name_str}" + version_str + date_str + time_str + path_postfix)

    def get_full_key_without_time(self, ds_date: Optional[date] = None, **kwargs) -> str:
        """
        Get CloudStorage key path with the time field customizable through regex patterns, including env, version, date_str if version and ds_date are provided
        Does not include CloudStorage Bucket.
        :param ds_date: Date partition of dataset
        :type ds_date: date
        :return: CloudStorage Dataset Key
        :rtype: str
        """

        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""

        ds_date_current = ds_date if ds_date is not None else datetime.now()
        date_str = f"/{ds_date_current.strftime(self.date_format)}"
        return f"{self.path_prefix}{env_str}{data_name_str}" + version_str + date_str

    def get_read_path(self, **kwargs) -> str:
        if self.check_type == "day":
            return self.get_full_key_without_time(**kwargs)
        return self.get_full_path(**kwargs)

    def with_check_type(self: T, check_type: str) -> T:
        """
        Creates copy of current dataset hardcoding `check_type` property to specified value.

        Commonly used with DatasetCheckOperator or DependencyOperator to provide check type for data existence check if this value
        different from default type.

        Example:

        check_incoming_data_exists = DatasetCheckOperator(dag, [
            Datasources.gracenote.external.device_hashed_ip.with_check_type('time'),
            Datasources.gracenote.external.device_geo
        ])

        :param check_type:
        :return:
        """
        if self.check_type == check_type:
            return self
        c = copy.copy(self)
        c.check_type = check_type
        return c

    def check_any_time_data_exist(
        self,
        cloud_storage: CloudStorage,
        ds_date: Optional[date] = None,
        lookback: int = 0,
    ) -> bool:
        """
        Check if any time data for a dataset exists

        :param cloud_storage: Cloud hook to query CloudStorage (AWS S3 or Azure Blob Storage)
        :type cloud_storage: CloudStorage
        :param lookback: Days of data to check for existence, if lookback is 0 then only specified day is checked.
        :type lookback: int
        :param ds_date: Days
        :type ds_date: Optional[date]
        :return: If data exists.
        :rtype: bool
        """

        return self.check_recent_data_exist(
            cloud_storage=cloud_storage,
            ds_date=ds_date if ds_date is not None else date.today(),
            max_lookback=lookback,
        ).is_success

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
        :param ds_date: Days
        :param max_lookback: Maximum number of days to look into history when searching for data existence of this dataset.
                0 means checking specified `ds_date` only. Default: 0.
        :param kwargs:
        :return: `Success` holding the date if data found within max_lookback limit, otherwise `Failure[DatasetNotFoundException]`.
        """

        logging.info(f'Finding most recent time partition of "{self.get_dataset_path()}" dataset')
        logging.info(f"Start date: {ds_date}")

        lookback = self.lookback if self.lookback is not None else max_lookback

        ds_date_current = ds_date

        # if max_lookback is 0 then we should check at least current day
        for daysBack in range(lookback + 1):
            for hour in range(23, -1, -1):
                for minute in range(59, -1, -1):
                    for second in range(59, -1, -1):
                        if super().check_data_exist(
                                cloud_storage=cloud_storage,
                                ds_date=datetime.combine(ds_date_current - timedelta(days=daysBack), time(hour=hour, minute=minute,
                                                                                                          second=second)),
                        ):
                            logging.info(
                                f'Found most recent dataset at date: {ds_date_current.strftime("%Y-%m-%d")} time: {hour}:{minute}:{second}'
                            )
                            return Success(
                                datetime.combine(ds_date_current - timedelta(days=daysBack), time(hour=hour, minute=minute, second=second))
                            )

        err_mgs = (f'Dataset "{self.data_name}" is not found for any time in the lookback')
        return Failure(DatasetNotFoundException(self, err_mgs))

    def check_data_exist(
        self,
        cloud_storage: CloudStorage,
        ds_date: Optional[datetime] = None,
        lookback: int = 0,
        **kwargs,
    ) -> bool:
        """
        Check if data for a dataset exists

        :param cloud_storage: Cloud hook to query CloudStorage (AWS S3 or Azure Blob Storage)
        :type cloud_storage: CloudStorage
        :param lookback: Days of data to check for existence, if lookback is 0 then only specified day is checked.
        :type lookback: int
        :param ds_date: Days
        :return: If data exists.
        :rtype: bool
        """
        logging.info(f'Check data existence of "{self.get_dataset_path()}" dataset')
        logging.info(f"Dataset Date: {ds_date}")

        lookback = self.lookback if self.lookback is not None else lookback

        ds_date_current = ds_date if ds_date is not None else date.today()

        # if lookback is 0 then we should check at least current day
        for daysBack in range(lookback + 1):
            if not super().check_data_exist(
                    cloud_storage=cloud_storage,
                    ds_date=datetime.combine(
                        ds_date_current.date() - timedelta(days=daysBack),
                        time(hour=ds_date_current.hour, minute=ds_date_current.minute, second=ds_date_current.second),
                    ),
                    **kwargs,
            ):
                return False

        return True
