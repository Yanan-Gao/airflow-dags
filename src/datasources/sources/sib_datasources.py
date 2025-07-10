from datasources.datasource import Datasource
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.spark_generated_dataset import SparkHourGeneratedDataset


class SibDatasources(Datasource):
    sibv2_log: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-logs-2",
        path_prefix="seeninbiddingdataexportv2",
        data_name="collected",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        version=None,
        env_aware=False,
        success_file=None,
    )

    devices_active_counts_data_collection_non_restricted_azure: HourGeneratedDataset = (
        HourGeneratedDataset(
            bucket="ttd-datamarketplace",
            azure_bucket="ttd-datamarketplace@eastusttdlogs",
            path_prefix="counts",
            data_name="devicesactivecountsdatacollectionnonrestricted",
            version=1,
            hour_format="hour={hour}",
            success_file=None,
        )
    )

    devices_active_counts_data_collection: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="devicesactivecountsdatacollection",
        version=2,
        hour_format="hour={hour}",
        success_file=None,
    )

    devices_active_counts_data_collection_alicloud: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        oss_bucket="ttd-counts",
        path_prefix="counts",
        data_name="devicesactivecountsdatacollectionalicloud",
        version=2,
        hour_format="hour={hour}",
        success_file=None
    )

    persons_households_data_export: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="targetingdatacountspersonshouseholdsdataexport",
        version=1,
        hour_format="hour={hour}",
        success_file=None,
    )

    sibv2_device_seven_day_rollup: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        data_name="seeninbiddingdevicesevendayrollup",
        version=2,
        success_file=None,
    )

    sibv2_device_data_uniques: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        data_name="seeninbiddingdevicedatauniques",
        version=2,
        success_file=None,
    )

    sibv2_group_data_overlaps: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        data_name="seeninbiddingdevicedataoverlaps",
        version=2,
        success_file=None,
    )

    @staticmethod
    def sibv2_daily(xd_vendor_id):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="seeninbidding",
            date_format=f"xdvendorid={xd_vendor_id}/date=%Y%m%d",
            version=2,
            success_file=None,
        )

    @staticmethod
    def sibv2_group_seven_day_rollup(xd_vendor_id):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="seeninbiddingpersongroupsevendayrollup",
            date_format=f"xdvendorid={xd_vendor_id}/date=%Y%m%d",
            version=2,
            success_file=None,
        )

    @staticmethod
    def sibv2_group_data_uniques(xd_vendor_id):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="seeninbiddingpersongroupdatauniques",
            date_format=f"xdvendorid={xd_vendor_id}/date=%Y%m%d",
            version=2,
            success_file=None,
        )

    @staticmethod
    def sibv2_device_xd_data_uniques(xd_vendor_id="10"):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="seeninbiddingdeviceexpansiondatauniques",
            date_format=f"xdvendorid={xd_vendor_id}/date=%Y%m%d",
            version=2,
            success_file=None
        )

    sibv2_device_seven_day_rollup_index_bitmap: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        data_name="seeninbiddingdevicesevendayrollupindexbitmap",
        version=2,
        success_file=None,
    )
