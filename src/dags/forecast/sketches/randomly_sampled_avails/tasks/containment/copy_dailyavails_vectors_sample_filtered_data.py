from datetime import timedelta

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from datasources.sources.ram_datasources import PartitioningArg, RamDatasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask

_NAME = 'copy_dailyavails_vectors_sample_filtered_data_from_s3_to_azure'
_PARTITIONING_ARGS = {
    "date_key": PartitioningArg(key="date", value=f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}', weight=0),
    "known_user": PartitioningArg(key="isKnownUser", value="true", weight=1)
}
_dataset = get_test_or_default_value(
    RamDatasources.dailyavails_vectors_sample_filtered_test, RamDatasources.dailyavails_vectors_sample_filtered
)


class CopyDailyAvailsVectorsSampleFilteredData(DatasetTransferTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            dataset=_dataset,
            src_cloud_provider=CloudProviders.aws,
            dst_cloud_provider=CloudProviders.azure,
            partitioning_args=_PARTITIONING_ARGS,
            max_threads=12,
            num_partitions=9,
            transfer_timeout=timedelta(hours=7),
            prepare_finalise_timeout=timedelta(minutes=30)
        )
