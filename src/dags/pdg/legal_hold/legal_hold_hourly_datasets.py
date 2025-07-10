from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.datasets.dataset import Dataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.rtb_datalake_dataset import RtbDatalakeDataset


class LegalHoldSourceDestinationDatasetHourly:

    def __init__(
        self,
        dataset_name: str,
        source_dataset: Dataset,
        dest_dataset: Dataset,
        cloud_provider: CloudProvider,
        num_partitions: int = 1,
        max_threads: int = 20
    ):
        self.dataset_name = dataset_name
        self.source_dataset = source_dataset
        self.dest_dataset = dest_dataset
        self.cloud_provider = cloud_provider
        self.num_partitions = num_partitions  # for large datasets to use multiple transfer tasks
        self.max_threads = max_threads


source_datalake_bucket = "ttd-datapipe-data"
source_datalake_azure_bucket = "ttd-datapipe-data@eastusttdlogs"
source_logs_bucket = "thetradedesk-useast-logs-2"
source_logs_azure_bucket = "thetradedesk-useast-logs-2@eastusttdlogs"
dest_bucket = "ttd-datapipe-file-hold"
dest_azure_bucket = "ttd-datapipe-file-hold@ttdlegalhold"

dest_rtb_bidfeedback: RtbDatalakeDataset = RtbDatalakeDataset(
    bucket=dest_bucket,
    azure_bucket=dest_azure_bucket,
    path_prefix="parquet",
    data_name="rtb_bidfeedback_cleanfile",
    version=5,
    success_file="_SUCCESS-sx-%Y%m%d-%H",
    env_aware=False,
    eldorado_class="BidFeedbackDataSetV5",
).with_check_type(check_type="hour")

dest_rtb_bidrequest: RtbDatalakeDataset = RtbDatalakeDataset(
    bucket=dest_bucket,
    azure_bucket=dest_azure_bucket,
    path_prefix="parquet",
    data_name="rtb_bidrequest_cleanfile",
    version=5,
    success_file="_SUCCESS-sx-%Y%m%d-%H",
    env_aware=False,
    eldorado_class="BidRequestDataSetV5",
).with_check_type(check_type="hour")

dest_rtb_conversiontracker: RtbDatalakeDataset = RtbDatalakeDataset(
    bucket=dest_bucket,
    path_prefix="parquet",
    data_name="rtb_conversiontracker_cleanfile",
    version=5,
    success_file="_SUCCESS-sx-%Y%m%d-%H",
    env_aware=False,
    eldorado_class="ConversionTrackerDataSetV5",
).with_check_type(check_type="hour")

dest_rtb_videoevent: RtbDatalakeDataset = RtbDatalakeDataset(
    bucket=dest_bucket,
    azure_bucket=dest_azure_bucket,
    path_prefix="parquet",
    data_name="rtb_videoevent_cleanfile",
    version=5,
    success_file="_SUCCESS-sx-%Y%m%d-%H",
    env_aware=False,
    eldorado_class="VideoEventDataSetV5",
).with_check_type(check_type="hour")

source_lostbidrequest: HourGeneratedDataset = HourGeneratedDataset(
    bucket=source_logs_bucket,
    azure_bucket=source_logs_azure_bucket,
    path_prefix="lostbidrequest",
    env_aware=False,
    data_name="cleansed",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

dest_lostbidrequest: HourGeneratedDataset = HourGeneratedDataset(
    bucket=dest_bucket,
    azure_bucket=dest_azure_bucket,
    path_prefix="lostbidrequest",
    env_aware=False,
    data_name="cleansed",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

source_dataimportpreaggprotolog: HourGeneratedDataset = HourGeneratedDataset(
    bucket=source_logs_bucket,
    azure_bucket=source_logs_azure_bucket,
    path_prefix="dataimportpreaggprotolog",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

dest_dataimportpreaggprotolog: HourGeneratedDataset = HourGeneratedDataset(
    bucket=dest_bucket,
    azure_bucket=dest_azure_bucket,
    path_prefix="dataimportpreaggprotolog",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

source_dataimportprotolog: HourGeneratedDataset = HourGeneratedDataset(
    bucket=source_logs_bucket,
    path_prefix="dataimportprotolog",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

dest_dataimportprotolog: HourGeneratedDataset = HourGeneratedDataset(
    bucket=dest_bucket,
    path_prefix="dataimportprotolog",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

source_advertiserdataimportprotolog: HourGeneratedDataset = HourGeneratedDataset(
    bucket=source_logs_bucket,
    azure_bucket=source_logs_azure_bucket,
    path_prefix="advertiserdataimportprotolog",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

dest_advertiserdataimportprotolog: HourGeneratedDataset = HourGeneratedDataset(
    bucket=dest_bucket,
    azure_bucket=dest_azure_bucket,
    path_prefix="advertiserdataimportprotolog",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
).with_check_type(check_type="hour")

legal_hold_hourly_datasets = [
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_bidfeedback_aws",
        source_dataset=Datasources.rtb_datalake.rtb_bidfeedback_v5,
        dest_dataset=dest_rtb_bidfeedback,
        cloud_provider=CloudProviders.aws,
        num_partitions=3,
        max_threads=5
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_bidrequest_aws",
        source_dataset=Datasources.rtb_datalake.rtb_bidrequest_v5,
        dest_dataset=dest_rtb_bidrequest,
        cloud_provider=CloudProviders.aws,
        num_partitions=5,
        max_threads=5
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_conversiontracker_aws",
        source_dataset=Datasources.rtb_datalake.rtb_conversiontracker_v5,
        dest_dataset=dest_rtb_conversiontracker,
        cloud_provider=CloudProviders.aws
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_videoevent_aws",
        source_dataset=Datasources.rtb_datalake.rtb_videoevent_v5,
        dest_dataset=dest_rtb_videoevent,
        cloud_provider=CloudProviders.aws,
        num_partitions=5,
        max_threads=5
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="lostbidrequest_aws",
        source_dataset=source_lostbidrequest,
        dest_dataset=dest_lostbidrequest,
        cloud_provider=CloudProviders.aws
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="dataimportpreaggprotolog_aws",
        source_dataset=source_dataimportpreaggprotolog,
        dest_dataset=dest_dataimportpreaggprotolog,
        cloud_provider=CloudProviders.aws,
        num_partitions=15
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="dataimportprotolog_aws",
        source_dataset=source_dataimportprotolog,
        dest_dataset=dest_dataimportprotolog,
        cloud_provider=CloudProviders.aws
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="advertiserdataimportprotolog_aws",
        source_dataset=source_advertiserdataimportprotolog,
        dest_dataset=dest_advertiserdataimportprotolog,
        cloud_provider=CloudProviders.aws
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_bidfeedback_azure",
        source_dataset=Datasources.rtb_datalake.rtb_bidfeedback_v5,
        dest_dataset=dest_rtb_bidfeedback,
        cloud_provider=CloudProviders.azure
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_bidrequest_azure",
        source_dataset=Datasources.rtb_datalake.rtb_bidrequest_v5,
        dest_dataset=dest_rtb_bidrequest,
        cloud_provider=CloudProviders.azure
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="rtb_videoevent_azure",
        source_dataset=Datasources.rtb_datalake.rtb_videoevent_v5,
        dest_dataset=dest_rtb_videoevent,
        cloud_provider=CloudProviders.azure
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="lostbidrequest_azure",
        source_dataset=source_lostbidrequest,
        dest_dataset=dest_lostbidrequest,
        cloud_provider=CloudProviders.azure,
        num_partitions=15,
        max_threads=100
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="dataimportpreaggprotolog_azure",
        source_dataset=source_dataimportpreaggprotolog,
        dest_dataset=dest_dataimportpreaggprotolog,
        cloud_provider=CloudProviders.azure,
        num_partitions=15,
        max_threads=100
    ),
    LegalHoldSourceDestinationDatasetHourly(
        dataset_name="advertiserdataimportprotolog_azure",
        source_dataset=source_advertiserdataimportprotolog,
        dest_dataset=dest_advertiserdataimportprotolog,
        cloud_provider=CloudProviders.azure
    ),
]
