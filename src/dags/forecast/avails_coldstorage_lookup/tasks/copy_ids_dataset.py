from datetime import timedelta

from dags.forecast.avails_coldstorage_lookup.constants import DAG_NAME
from dags.forecast.avails_coldstorage_lookup.utils import xcom_pull_from_template
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from datasources.sources.ram_datasources import RamDatasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.datasets.hour_dataset import HourGeneratedDataset

_NAME = 'copy_ids_from_randomly_sampled_avails_to_azure'
_dataset: HourGeneratedDataset = get_test_or_default_value(RamDatasources.randomly_sampled_ids_test, RamDatasources.randomly_sampled_ids)


class CopyIdsDatasetTask(DatasetTransferTask):

    def __init__(self, iteration_number):
        super().__init__(
            name=f"{_NAME}_{iteration_number}",
            dataset=_dataset,
            src_cloud_provider=CloudProviders.aws,
            dst_cloud_provider=CloudProviders.azure,
            partitioning_args=_dataset.get_partitioning_args(
                ds_date=xcom_pull_from_template(DAG_NAME, "initialize_run_hour", iteration_number)
            ),
            max_threads=12,
            num_partitions=3,
            transfer_timeout=timedelta(hours=2),
            prepare_finalise_timeout=timedelta(minutes=30)
        )
