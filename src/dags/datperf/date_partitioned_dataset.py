import os
from datetime import date
from typing import Optional, List, Dict

from ttd.datasets.dataset import default_date_part_format, SUCCESS
from ttd.datasets.date_generated_dataset import DateGeneratedDataset

from ttd.datasets.env_path_configuration import EnvPathConfiguration, ExistingDatasetPathConfiguration


class DatePartitionedDataset(DateGeneratedDataset):

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        partitions=List[str],
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
        super().__init__(
            bucket, path_prefix, data_name, version, date_format, success_file, env_aware, eldorado_class, azure_bucket, oss_bucket,
            metadata_files, buckets_for_other_regions, env_path_configuration
        )
        self.partitions = partitions

    def get_dataset_path(self, **kwargs) -> str:
        return super().get_dataset_path(**kwargs)

    def _get_full_key(self, ds_date: Optional[date] = None, **kwargs) -> str:
        return super()._get_full_key(ds_date, **kwargs) + "/" + self._get_suffix()

    def _get_suffix(self) -> str:
        return str(os.path.join(*self.partitions))
