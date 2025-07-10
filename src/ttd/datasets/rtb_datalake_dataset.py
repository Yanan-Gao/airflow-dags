from datetime import datetime, timedelta
from typing import Optional, List

from ttd.datasets.dataset import (
    default_date_part_format,
    default_hour_part_format,
    SUCCESS,
)
from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration,
    ExistingDatasetPathConfiguration,
)
from ttd.datasets.hour_dataset import HourGeneratedDataset


class RtbDatalakeDataset(HourGeneratedDataset):
    """
    RTB Datalake Dataset

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
    :param hour_format: Format of the hour part of the path, should be specified in formatting string syntax
            https://docs.python.org/3/library/string.html#formatstrings
    :param success_file: Override the default success file for the dataset. If None, then presence of folder (prefix) is checked,
        otherwise presence of success file (CloudStorage key) as specified in this param is checked.
    :type success_file: str
    :param env_aware: Specify if this dataset is environment aware, in other words does CloudStorage key contain `env`
                    part to it such as "prod" or "test"
    """

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        version: Optional[int] = 1,
        date_format: str = default_date_part_format,
        hour_format: str = default_hour_part_format,
        success_file: Optional[str] = SUCCESS,
        env_aware: bool = True,
        eldorado_class: Optional[str] = None,
        azure_bucket: Optional[str] = None,
        oss_bucket: Optional[str] = None,
        metadata_files: Optional[List[str]] = None,
        env_path_configuration: EnvPathConfiguration = ExistingDatasetPathConfiguration(),
    ):
        super().__init__(
            bucket=bucket,
            azure_bucket=azure_bucket,
            path_prefix=path_prefix,
            data_name=data_name,
            version=version,
            date_format=date_format,
            hour_format=hour_format,
            success_file=success_file,
            env_aware=env_aware,
            eldorado_class=eldorado_class,
            oss_bucket=oss_bucket,
            metadata_files=metadata_files,
            env_path_configuration=env_path_configuration,
        )

    def _get_env_path_part(self, **kwargs) -> str:
        env_str = self._get_ds_env()
        return ("" if not self.env_aware or env_str == "prod" else super()._get_env_path_part())

    def _get_success_file_path(self, **kwargs) -> str:
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""

        return f"{self.path_prefix}{env_str}{data_name_str}{version_str}"

    def _get_check_paths(self, ds_date: datetime, **kwargs) -> List[str]:
        return super()._get_check_paths(ds_date=ds_date) + super()._get_check_paths(ds_date=ds_date - timedelta(hours=1))
