from datetime import timedelta

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from datasources.sources.ram_datasources import PartitioningArg, RamDatasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask

_NAME = "run_task_append_targeting_data_ram_daily_vectors_from_azure_to_s3"
_PARTITIONING_ARGS = {
    "date_key": PartitioningArg(key="date", value=f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}', weight=0),
    "source": PartitioningArg(key="source", value="avails", weight=1),
    "vector_name": PartitioningArg(key="vectorName", value="TargetingDataId", weight=2)
}
_dataset = get_test_or_default_value(RamDatasources.ram_daily_vectors_test, RamDatasources.ram_daily_vectors)


class AppendTargetingDataRamDailyVectors(DatasetTransferTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            dataset=_dataset,
            src_cloud_provider=CloudProviders.azure,
            dst_cloud_provider=CloudProviders.aws,
            partitioning_args=_PARTITIONING_ARGS,
            num_partitions=3,
            max_threads=12,
            transfer_timeout=timedelta(hours=4),
            prepare_finalise_timeout=timedelta(minutes=30)
        )
