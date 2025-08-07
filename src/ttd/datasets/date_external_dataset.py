from datetime import date
from typing import Optional, List

from ttd.datasets.dataset import (
    default_short_date_part_format,
    SUCCESS,
)
from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration,
    ExistingDatasetPathConfiguration,
)
from ttd.datasets.date_dataset import DateDataset


class DateExternalDataset(DateDataset):

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        version: int = None,
        date_format: str = default_short_date_part_format,
        success_file: Optional[str] = SUCCESS,
        eldorado_class: Optional[str] = None,
        azure_bucket: Optional[str] = None,
        metadata_files: Optional[List[str]] = None,
        env_path_configuration: EnvPathConfiguration = ExistingDatasetPathConfiguration(),
    ):
        """
        Used for External Non-TTD Datasets or Legacy TTD Datasets that do not conform to DateGeneratedDataset

        :param bucket: CloudStorage Bucket
        :type bucket: str
        :param path_prefix: CloudStorage Path Prefix
        :type path_prefix: str
        :param data_name: CloudStorage Data Name (Path Postfix)
        :type data_name: str
        :param version: Version of the Data, appended to CloudStorage path including a 'v='
        :type version: int
        :param date_format: date format for date partitions, appended to CloudStorage path including a 'date='
        :type date_format: str
        :param success_file: Override the default success file for the dataset. If None, then presence of folder (prefix) is checked,
            otherwise presence of success file (CloudStorage key) as specified in this param is checked.
        :type success_file: str
        """
        super().__init__(
            bucket=bucket,
            path_prefix=path_prefix,
            data_name=data_name,
            version=version,
            date_format=date_format,
            success_file=success_file,
            eldorado_class=eldorado_class,
            azure_bucket=azure_bucket,
            metadata_files=metadata_files,
            env_path_configuration=env_path_configuration,
        )

    def _get_full_key(self, ds_date: Optional[date] = None, **kwargs) -> str:
        """
        Get CloudStorage key path, including version and date_str if version and ds_date are provided
        Does not include CloudStorage Bucket.

        :param ds_date: Date partition of dataset
        :type ds_date: date
        :return: CloudStorage Dataset Key
        :rtype: str
        """

        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        date_str = (f"/{ds_date.strftime(self.date_format)}" if ds_date is not None else "")
        version_str = f"/v={self.version}" if self.version is not None else ""

        return f"{self.path_prefix}{data_name_str}" + version_str + date_str

    def get_dataset_path(self, **kwargs) -> str:
        """
        Get Full CloudStorage path, no postfixes.

        :return: CloudStorage Path
        :rtype: str
        """
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""

        return f"{self.get_root_path()}{data_name_str}"
