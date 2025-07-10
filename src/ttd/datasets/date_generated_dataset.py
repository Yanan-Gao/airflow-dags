from datetime import date, datetime
from typing import Optional, List, Dict

from ttd.datasets.dataset import (
    default_date_part_format,
    SUCCESS,
)
from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration,
    ExistingDatasetPathConfiguration,
)
from ttd.datasets.date_dataset import DateDataset


class DateGeneratedDataset(DateDataset):

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        version: Optional[int] = 1,
        date_format: str = default_date_part_format,
        success_file: Optional[str] = SUCCESS,
        env_aware: bool = True,
        eldorado_class: Optional[str] = None,
        azure_bucket: Optional[str] = None,
        oss_bucket: Optional[str] = None,
        metadata_files: Optional[List[str]] = None,
        buckets_for_other_regions: Optional[Dict[str, str]] = None,
        env_path_configuration: EnvPathConfiguration = ExistingDatasetPathConfiguration(),
    ):
        """
        Standard Dataset format for TTD Data
        Use for new Generated Datasets

        :param bucket: CloudStorage Bucket
        :type bucket: str
        :param path_prefix: CloudStorage Path Prefix
        :type path_prefix: str
        :param data_name: CloudStorage Data Name (Path Postfix)
        :type data_name: str
        :param version: Version of the Data, appended to CloudStorage path including a 'v='
        :type version: Optional[int]
        :param date_format: Format of the date component of the path to dataset in the form acceptable by datetime.strftime()
        :type date_format: str
        :param success_file: Override the default success file for the dataset. If None, then presence of folder (prefix) is checked,
            otherwise presence of success file (CloudStorage key) as specified in this param is checked.
        :type success_file: str
        :param env_aware: Specify if this dataset is environment aware, in other words does CloudStorage key contain `env` part to it such as
            "prod" or "test"
        :param oss_bucket: OSS Bucket, if empty, will use bucket
        :type oss_bucket: str
        """
        super().__init__(
            bucket=bucket,
            path_prefix=path_prefix,
            data_name=data_name,
            version=version,
            date_format=date_format,
            success_file=success_file,
            env_aware=env_aware,
            eldorado_class=eldorado_class,
            azure_bucket=azure_bucket,
            oss_bucket=oss_bucket if oss_bucket else bucket,
            metadata_files=metadata_files,
            buckets_for_other_regions=buckets_for_other_regions,
            env_path_configuration=env_path_configuration,
        )

    def _get_full_key(self, ds_date: Optional[date] = None, **kwargs) -> str:
        """
        Get CloudStorage key path, including env, version, date_str if version and ds_date are provided
        Does not include CloudStorage Bucket.

        :param ds_date: Date partition of dataset
        :type ds_date: date
        :return: CloudStorage Dataset Key
        :rtype: str
        """
        ds_date_current = ds_date if ds_date is not None else datetime.now()

        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        date_str = (f"/{ds_date_current.strftime(self.date_format)}" if ds_date_current is not None else "")
        version_str = f"/v={self.version}" if self.version is not None else ""

        return f"{self.path_prefix}{env_str}{data_name_str}" + version_str + date_str

    def get_dataset_path(self, **kwargs) -> str:
        """
        Get Full CloudStorage path, only environment postfix.
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""

        return f"{self.get_root_path()}{env_str}{data_name_str}"
