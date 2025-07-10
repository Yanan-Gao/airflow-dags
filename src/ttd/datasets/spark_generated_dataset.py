from typing import Optional

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


class SparkHourGeneratedDataset(HourGeneratedDataset):
    """
    Implementation of Hour partitioned dataset for spark datasets that require differences to the base classes.
    E.g. spark writes the success file to the version folder, not the date/hour folder
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
            env_path_configuration=env_path_configuration,
        )

    def _get_success_file_path(self, **kwargs) -> str:
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""

        return f"{self.path_prefix}{env_str}{data_name_str}{version_str}"
