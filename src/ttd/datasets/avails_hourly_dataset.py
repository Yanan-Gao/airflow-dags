from datetime import datetime
from typing import Optional, Dict, List

from ttd.datasets.dataset import (
    SUCCESS,
    T,
    default_hour_part_format,
)

from ttd.datasets.env_path_configuration import (
    EnvPathConfiguration,
    ExistingDatasetPathConfiguration,
)
from ttd.datasets.hour_dataset import HourGeneratedDataset


class AvailsHourlyDataset(HourGeneratedDataset):

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        region_dependency_map: Dict[str, List[str]],
        version: Optional[int] = 1,
        date_format: str = "date=%Y-%m-%d",
        hour_format: str = default_hour_part_format,
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
            hour_format=hour_format,
            success_file=success_file,
            env_aware=env_aware,
            eldorado_class=eldorado_class,
            oss_bucket=oss_bucket,
            metadata_files=metadata_files,
            buckets_for_other_regions=buckets_for_other_regions,
            env_path_configuration=env_path_configuration,
        )

        self.region_dependency_map = region_dependency_map
        self.current_region = None

    def _get_full_key(self, ds_date: Optional[datetime] = None, region: Optional[str] = None, **kwargs) -> str:
        key_without_region = super()._get_full_key(ds_date=ds_date)
        region_str = f"/originatingRegion={region}" if region is not None else ""

        return f"{key_without_region}{region_str}"

    def _get_check_paths(self, ds_date: Optional[datetime], **kwargs) -> List[str]:
        if self.current_region is None:
            return super()._get_check_paths(ds_date=ds_date)

        list_of_paths = [
            super(AvailsHourlyDataset, self)._get_check_paths(
                ds_date=ds_date,
                region=self._format_region_str(dependent_region),
                **kwargs,
            ) for dependent_region in self.region_dependency_map[self.current_region]
        ]
        return sum(list_of_paths, [])

    def with_region(self: T, region: str) -> T:
        ds_copy = super(AvailsHourlyDataset, self).with_region(region)

        if region not in self.region_dependency_map:
            raise ValueError(
                f"The 'region' argument value should be present in the region dependency map. "
                f"Region key {region} wasn't found"
            )
        ds_copy.current_region = region

        return ds_copy

    @staticmethod
    def _format_region_str(region: str) -> str:
        if region == "Ali_China_East_2":
            return region
        return region.upper().replace("-", "_")
