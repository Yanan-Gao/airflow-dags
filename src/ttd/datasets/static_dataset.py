from datetime import date
from typing import Optional, List, Dict, Any

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.datasets.dataset import Dataset, SUCCESS, DatasetNotFoundException
from ttd.datasets.env_path_configuration import EnvPathConfiguration, ExistingDatasetPathConfiguration
from ttd.monads.trye import Try, Success, Failure


class StaticDataset(Dataset):
    """
    Use for datasets that have a single instance and aren't generated daily/hourly

    :param bucket: CloudStorage Bucket
    :type bucket: str
    :param path_prefix: CloudStorage Path Prefix
    :type path_prefix: str
    :param data_name: CloudStorage Data Name
    :type data_name: str
    :param version: Version of the Data, appended to CloudStorage path including a 'v='
    :type version: Optional[int]
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

    def get_partitioning_args(self, **kwargs) -> Dict[str, Any]:
        return {}

    def parse_partitioning_args(self, **kwargs) -> Dict[str, Any]:
        return {}

    def _get_full_key(self, **kwargs) -> str:
        """
        Get CloudStorage key path, including env and version if provided
        Does not include CloudStorage Bucket.
        :type ds_date: date
        :return: CloudStorage Dataset Key
        :rtype: str
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""

        return (f"{self.path_prefix}{env_str}{data_name_str}" + version_str)

    def get_dataset_path(self, **kwargs) -> str:
        """
        Get Full CloudStorage path, no postfixes.

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
        if super().check_data_exist(cloud_storage=cloud_storage):
            return Success(ds_date)

        err_mgs = f'No Success file is found for "{self.data_name}" data up to "{ds_date.strftime("%Y-%m-%d")}" back in history'
        return Failure(DatasetNotFoundException(self, err_mgs))
