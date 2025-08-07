from datetime import date, datetime
from typing import Optional, Dict, Any, Union, List

from airflow.utils import timezone

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.datasets.dataset import (
    SUCCESS,
    Dataset,
)
from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration,
    ExistingDatasetPathConfiguration,
)
from ttd.monads.trye import Try


class TimestampGeneratedDataset(Dataset):
    date_partitioning_arg_format = "%Y-%m-%d"
    """
    Implementation for data set with the POSIX timestamp of the dag run time in the data set path

    :param bucket: CloudStorage Bucket
    :type bucket: str
    :param path_prefix: CloudStorage Path Prefix
    :type path_prefix: str
    :param data_name: CloudStorage Data Name (Path Postfix)
    :type data_name: str
    :param version: Version of the Data, appended to CloudStorage path including a 'v='
    :type version: Optional[int]
    :param date_format: Format of the date component of the path to dataset. Default: "date=%Y%m%d"
    :type date_format: str
    :param success_file: Override the default success file for the dataset. If None, then presence of folder (prefix) is checked,
        otherwise presence of success file (CloudStorage key) as specified in this param is checked.
    :type success_file: str
    :param env_aware: Specify if this dataset is environment aware, in other words does CloudStorage key contain `env` part to it such as
        "prod" or "test"
    """

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
        metadata_files: Optional[List[str]] = None,
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
            metadata_files=metadata_files,
            env_path_configuration=env_path_configuration,
        )
        self.date_format = date_format

    def get_partitioning_args(self, timestamp: str, ds_date: Union[date, str], **kwargs) -> Dict[str, Any]:
        """
        Get partitioning arguments

        :param timestamp: POSIX timestamp of the dag run time
        :type timestamp: str
        :param ds_date: Dataset date partition. Expected format: %Y-%m-%d
        :type ds_date: date
        :return: Partitioning arguments dictionary
        :rtype: Dict[str, Any]
        """
        return {"timestamp": timestamp, "ds_date": ds_date}

    def parse_partitioning_args(self, timestamp: str, ds_date: Union[date, str], **kwargs) -> Dict[str, Any]:
        if isinstance(ds_date, str):
            ds_date = datetime.strptime(ds_date, self.date_partitioning_arg_format)
        return {"timestamp": timestamp, "ds_date": ds_date}

    def _get_full_key(self, timestamp: Optional[str] = None, ds_date: Optional[date] = None, **kwargs) -> str:
        """
        Get CloudStorage key path, including env, version if version is provided, timestamp, date_str if timestamp and
        ds_date are provided otherwise will use the current time
        Does not include CloudStorage Bucket

        :param timestamp: POSIX timestamp of the dag run time
        :type timestamp: str
        :param ds_date: Date partition of dataset
        :type ds_date: date
        :return: CloudStorage Dataset Key
        :rtype: str
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""
        timestamp_str = (f"/{timestamp}" if timestamp is not None else f"/{int(timezone.utcnow().timestamp())}")
        ds_date_current = ds_date if ds_date is not None else datetime.now()
        date_str = (f"/{ds_date_current.strftime(self.date_format)}" if ds_date_current is not None else "")

        return f"{self.path_prefix}{env_str}{data_name_str}{version_str}{timestamp_str}{date_str}"

    def get_dataset_path(self, **kwargs) -> str:
        """
        Get Full CloudStorage path, only environment postfix.

        :return: CloudStorage Path
        :rtype: str
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""

        return f"{self.get_root_path()}{env_str}{data_name_str}"

    def check_recent_data_exist(
        self,
        cloud_storage: CloudStorage,
        ds_date: date,
        max_lookback: int = 0,
        **kwargs,
    ) -> Try[date]:
        raise NotImplementedError()
