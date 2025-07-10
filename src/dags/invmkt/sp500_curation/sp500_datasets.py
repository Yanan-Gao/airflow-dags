from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import ExistingDatasetPathConfiguration

rtb_platform_metrics: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-vertica-parquet-export",
    path_prefix="ExportRTBPlatformReportGlobal100Metrics/VerticaAws",
    env_aware=False,
    version=None,
    data_name="",
    date_format="date=%Y%m%d",
    success_file="_SUCCESS",
    env_path_configuration=ExistingDatasetPathConfiguration()
)

sincera_site_metrics: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-data-import",
    path_prefix="marketplace-quality/sincera/site-metrics-v4",
    env_aware=False,
    version=None,
    data_name="",
    date_format="date=%Y%m%d",
    success_file="_SUCCESS",
    env_path_configuration=ExistingDatasetPathConfiguration()
)
