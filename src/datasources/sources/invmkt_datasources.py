from datasources.datasource import Datasource
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration, ExistingDatasetPathConfiguration


class InvMktDatasources(Datasource):
    bucket = "ttd-avails-metadata"

    property_seller_metadata_daily: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket,
        path_prefix="property-seller-metadata",
        env_aware=True,
        version=1,
        data_name="",
        date_format="date=%Y%m%d",
        success_file="_SUCCESS",
        env_path_configuration=MigratedDatasetPathConfiguration()
    )

    property_deal_metadata_daily: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket,
        path_prefix="property-deal-metadata",
        env_aware=True,
        version=1,
        data_name="",
        date_format="date=%Y%m%d",
        success_file="_SUCCESS",
        env_path_configuration=MigratedDatasetPathConfiguration()
    )

    marketplace_metadata_daily: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket,
        path_prefix="marketplace-metadata",
        env_aware=True,
        version=1,
        data_name="",
        date_format="date=%Y%m%d",
        success_file="_SUCCESS",
        env_path_configuration=MigratedDatasetPathConfiguration()
    )

    ttd_deals_daily: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-vertica-parquet-export",
        path_prefix="ExportTTDDeals/Default",
        env_aware=False,
        version=None,
        data_name="",
        date_format="date=%Y%m%d",
        success_file="_SUCCESS",
        env_path_configuration=ExistingDatasetPathConfiguration()
    )

    mist_avails_agg_daily: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket,
        path_prefix="mist-avails-agg",
        env_aware=True,
        version=1,
        data_name="",
        date_format="date=%Y%m%d",
        success_file="_SUCCESS",
        env_path_configuration=MigratedDatasetPathConfiguration()
    )
