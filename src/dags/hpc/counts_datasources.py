from dags.hpc.utils import CrossDeviceLevel
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.spark_generated_dataset import SparkHourGeneratedDataset
from datasources.datasource import Datasource
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.timestamp_generated_dataset import TimestampGeneratedDataset


class CountsDataName:
    ACTIVE_TDID_DAID = "tdiddaid"
    ACTIVE_UID_IDL = "uididl"
    ACTIVE_IPADDRESS = "ipaddress"
    GEOS_DIMENSIONS_AGGREGATION = "geosdimensionsaggregation"
    COUNTS_AVAILS_AGGREGATION = "countsavailsaggregation"
    BID_REQUEST_GEO_DIMENSIONS_AGGREGATION = "bidrequestsgeosdimensionsaggregation"
    USER_ID_ORDER_DATA_CHINA = "useridorderdata/china"
    TARGETING_DATA_CHINA = "dimensiontargetingdata/device/china/targetingdata"
    META_DATA_CHINA = "dimensiontargetingdata/device/china/metadata"


class CountsDatasources(Datasource):
    sib_daily_sketches_targeting_data_ids_staging_hmh_aws: TimestampGeneratedDataset = (
        TimestampGeneratedDataset(
            bucket="thetradedesk-useast-qubole",
            azure_bucket="ttd-datamarketplace@eastusttdlogs",
            path_prefix="warehouse/thetradedesk.db",
            data_name="sib-daily-sketches-targeting-data-ids-staging-hmh",
            date_format="%Y/%m/%d",
            env_aware=False,
        )
    )

    sib_daily_sketches_targeting_data_ids_aws: TimestampGeneratedDataset = (
        TimestampGeneratedDataset(
            bucket="thetradedesk-useast-qubole",
            azure_bucket="ttd-datamarketplace@eastusttdlogs",
            path_prefix="warehouse/thetradedesk.db",
            data_name="sib-daily-sketches-targeting-data-ids",
            date_format="%Y/%m/%d",
            env_aware=False,
        )
    )

    sib_daily_sketches_targeting_data_ids_staging_hmh_azure: TimestampGeneratedDataset = TimestampGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="sib-daily-sketches-targeting-data-ids-staging-hmh",
        date_format="%Y/%m/%d",
    )

    sib_daily_sketches_targeting_data_ids_azure: TimestampGeneratedDataset = (
        TimestampGeneratedDataset(
            bucket="thetradedesk-useast-qubole",
            azure_bucket="ttd-datamarketplace@eastusttdlogs",
            path_prefix="counts",
            data_name="sib-daily-sketches-targeting-data-ids",
            date_format="%Y/%m/%d",
        )
    )

    targeting_data_received_counts: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace", path_prefix="counts", data_name="targetingdatareceivedcounts", version=5, hour_format="hour={hour}"
    )

    targeting_data_received_counts_azure: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsazure",
        version=5,
        hour_format="hour={hour}"
    )

    targeting_data_deleted_received_counts_azure: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsdeletedazure",
        version=2,
        hour_format="hour={hour}"
    )

    targeting_data_received_counts_alicloud: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        oss_bucket="ttd-counts",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsalicloud",
        version=5,
        hour_format="hour={hour}"
    )

    targeting_data_deleted_received_counts_alicloud: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        oss_bucket="ttd-counts",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsdeletedalicloud",
        version=2,
        hour_format="hour={hour}"
    )

    targeting_data_received_counts_by_uiid_type: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsbyuiidtype",
        version=3,
        hour_format="hour={hour}"
    )

    targeting_data_received_counts_by_uiid_type_azure: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsbyuiidtypeazure",
        version=3,
        hour_format="hour={hour}"
    )

    targeting_data_received_counts_by_uiid_type_alicloud: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        oss_bucket="ttd-counts",
        path_prefix="counts",
        data_name="targetingdatareceivedcountsbyuiidtypealicloud",
        version=3,
        hour_format="hour={hour}"
    )

    targeting_data_ip_received_counts_non_restricted: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="targetingdataipreceivedcounts/aws",
        version=3,
        hour_format="hour={hour}"
    )

    targeting_data_ip_segments_non_restricted: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="ipsegments/aws",
        version=1,
        hour_format="hour={hour}"
    )

    ip_dimension_targeting_data: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="dimensiontargetingdata/device/ipaws/targetingdata",
        version=1,
        hour_format="hour={hour}"
    )

    hawkid_data_export: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-datamarketplace",
        path_prefix="counts",
        data_name="activehawkid",
        version=1,
        hour_format="hour={hour}",
        success_file=None,
    )

    hotcache_counts: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        oss_bucket="ttd-counts",
        path_prefix="counts",
        data_name="hotcachecounts",
        version=1,
        date_format="date=%Y%m%d",
    )

    avails_7_day_alicloud: HourGeneratedDataset = HourGeneratedDataset(
        bucket="n/a",
        oss_bucket="ttd-counts",
        path_prefix="counts",
        data_name="avails7day",
        hour_format="hour={hour}",
        version=2,
        env_aware=True,
        success_file=None,
    )

    active_targeting_data_ids_aws: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        path_prefix="counts",
        data_name="activetargetingdataids/aws",
        version=1,
        hour_format="hour={hour}",
        success_file=None
    )

    active_targeting_data_ids_azure: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="activetargetingdataids/azure",
        version=1,
        hour_format="hour={hour}",
        success_file=None
    )

    persons_households_booster: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="personshouseholdsboosterdataset",
        version=1,
        hour_format="hour={hour}",
        success_file=None,
    )

    households_expansion: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        azure_bucket="ttd-datamarketplace@eastusttdlogs",
        path_prefix="counts",
        data_name="dimensiontargetingdata/householdexpanded/main/targetingdata",
        version=1,
        hour_format="hour={hour}",
        success_file=None,
    )

    segment_counts_export: HourGeneratedDataset = SparkHourGeneratedDataset(
        bucket="ttd-datamarketplace",
        path_prefix="counts",
        data_name="generatedactivecounts/segments/aws",
        version=2,
        hour_format="hour={hour:0>2d}",
        success_file=None,
    )

    @staticmethod
    def get_id_to_global_id_map_dataset(cross_device_level: CrossDeviceLevel) -> SparkHourGeneratedDataset:
        return SparkHourGeneratedDataset(
            bucket="ttd-datamarketplace",
            azure_bucket="ttd-datamarketplace@eastusttdlogs",
            path_prefix="counts",
            data_name=f"idtoglobalidmap/{str(cross_device_level)}",
            date_format="date=%Y%m%d",
            hour_format="hour={hour}",
            success_file=None,
            version=1,
            env_aware=True,
        )

    @staticmethod
    def get_targeting_data_id_bitmaps_dataset(cross_device_level: CrossDeviceLevel) -> SparkHourGeneratedDataset:
        return SparkHourGeneratedDataset(
            bucket="ttd-datamarketplace",
            azure_bucket="ttd-datamarketplace@eastusttdlogs",
            path_prefix="counts",
            data_name=f"targetingdataidbitmaps/{str(cross_device_level)}",
            date_format="date=%Y%m%d",
            hour_format="hour={hour}",
            success_file=None,
            version=1,
            env_aware=True,
        )

    @staticmethod
    def get_counts_dataset(counts_data_name: str, version: int = 1):
        return HourGeneratedDataset(
            bucket="ttd-datamarketplace",
            azure_bucket="ttd-counts@ttdexportdata",
            oss_bucket="ttd-counts",
            path_prefix="counts",
            data_name=counts_data_name,
            date_format="date=%Y%m%d",
            hour_format="hour={hour}",
            success_file=None,
            version=version,
            env_aware=True,
        )

    @staticmethod
    def get_audience_counts_records_dataset(is_historical: bool = False) -> HourGeneratedDataset:
        return HourGeneratedDataset(
            bucket="ttd-datamarketplace",
            path_prefix="counts" if not is_historical else "counts/historical",
            data_name="audiencecountrecords",
            version=2,
            date_format="date=%Y%m%d",
            hour_format="hour={hour:0>2d}",
            success_file=None,
            env_aware=True,
        )
