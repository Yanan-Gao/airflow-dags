import functools
from abc import ABCMeta
from datetime import date
from typing import Optional, Dict, Any, List, Sequence

from datasources.datasource import Datasource
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.datasets.dataset import Dataset, SUCCESS
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import EnvPathConfiguration, ExistingDatasetPathConfiguration
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.monads.trye import Try


@functools.total_ordering
class PartitioningArg:
    template_fields: Sequence[str] = ("key", "value")

    def __init__(self, key: str, value: str, weight: int = 1_000_000_000):
        self.key = key
        self.value = value
        self.weight = weight

    @staticmethod
    def _is_valid_operand(other):
        return isinstance(other, PartitioningArg)

    def __eq__(self, other):
        if not self._is_valid_operand(other):
            return NotImplemented
        return other.weight == self.weight and other.key == self.key and other.value == self.value

    def __lt__(self, other):
        if not self._is_valid_operand(other):
            return NotImplemented
        return self.weight < other.weight


class CustomPartitionDataset(Dataset, metaclass=ABCMeta):
    """
    WARNING!

    This class was created just to comply with an urgent release.
    It may not be a good idea to inherit nor use this class.
    This may require more thought to be safe for other teams to use it
    """

    def __init__(
        self,
        bucket: str,
        path_prefix: str,
        data_name: str,
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        env_aware: bool = True,
        azure_bucket: Optional[str] = None,
        oss_bucket: Optional[str] = None,
        eldorado_class: Optional[str] = None,
        env_path_configuration: EnvPathConfiguration = ExistingDatasetPathConfiguration()
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
            env_path_configuration=env_path_configuration
        )

    def get_partitioning_args(self, **kwargs) -> Dict[str, Any]:
        return kwargs

    def parse_partitioning_args(self, **kwargs) -> Dict[str, Any]:
        return kwargs

    def check_recent_data_exist(
        self,
        cloud_storage: CloudStorage,
        ds_date: date,
        max_lookback: int = 0,
        **kwargs,
    ) -> Try[date]:
        return kwargs

    def _get_full_key(self, **kwargs) -> str:
        """
        Get the full key of the object with custom partitions like:
         prefix/data_name/partition_key_0=partition_value_0/.../partition_key_n=partition_value_n
        in non-increasing order of partition.weight (assuming all partition args are an instance of
        PartitionArg class)

        NOTE: This method nor this class, parse any kind of date, it is assuming every param is parsed
        by the time it reaches this method
        """
        args_list: List[PartitioningArg] = []
        for k, v in kwargs.items():
            assert (isinstance(v, PartitioningArg))
            args_list.append(v)

        args_list.sort()
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""
        version_str = f"/v={self.version}" if self.version is not None else ""
        full_key: str = f"{self.path_prefix}{env_str}{data_name_str}" + version_str

        for arg in args_list:
            full_key = full_key + f"/{arg.key}={arg.value}"

        return full_key

    def get_dataset_path(self, **kwargs) -> str:
        """
        Get Full CloudStorage path, only environment postfix.
        """
        env_str = self._get_env_path_part()
        data_name_str = f"/{self.data_name}" if self.data_name != "" else ""

        return f"{self.get_root_path()}{env_str}{data_name_str}"


class RamDatasources(Datasource):
    dailyavails_tdid_hmh_agg: DateGeneratedDataset = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="adhoc/dailyavails_tdid_hmh_agg",
        version=None,
        env_aware=True,
        success_file=None
    )

    dailyavails_vectors_sample_filtered = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="adhoc/dailyavails_vectors_sample_filtered",
        env_aware=True,
        version=None,
        success_file=None
    )

    ram_daily_containment_records = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="ttd/ram_daily_containment_records",
        env_aware=True,
        version=None,
        success_file=None
    )

    ram_daily_vectors = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="ttd/ram_daily_vectors",
        env_aware=True,
        version=None,
        success_file=None
    )

    ram_daily_vectors_test = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="test/ttd/ram_daily_vectors",
        env_aware=False,
        version=None,
        success_file=None
    )

    ram_daily_containment_records_test = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="test/ttd/ram_daily_containment_records",
        env_aware=False,
        version=None,
        success_file=None
    )

    dailyavails_vectors_sample_filtered_test = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="test/adhoc/dailyavails_vectors_sample_filtered",
        env_aware=False,
        version=None,
        success_file=None
    )

    dailyavails_tdid_hmh_agg_test: DateGeneratedDataset = CustomPartitionDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="test/adhoc/dailyavails_tdid_hmh_agg",
        version=None,
        env_aware=False,
        success_file=None
    )

    randomly_sampled_ids: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="ttd/sampled_avails_cs_sync_ids",
        version=None,
        hour_format="hour={hour}",
        env_aware=True,
        success_file=None
    )
    randomly_sampled_ids.date_partitioning_arg_format = '%Y-%m-%dT%H:%M:%S'

    randomly_sampled_ids_test: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@eastusttdlogs",
        path_prefix="datapipeline/ram",
        data_name="test/ttd/sampled_avails_cs_sync_ids",
        version=None,
        hour_format="hour={hour}",
        env_aware=False,
        success_file=None
    )
    randomly_sampled_ids_test.date_partitioning_arg_format = '%Y-%m-%dT%H:%M:%S'
