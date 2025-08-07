from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class DoohDatasources:
    location_visits_CSV: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-logs-2",
        path_prefix="locationvisits/verticaload",
        data_name="ttd_locationvisitstargetingdata",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        version=None,
        success_file=None,
        env_aware=False,
    )

    location_visits_deduped: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="location-visits-hourly",
        hour_format="hour={hour}",
        version=1,
    )

    location_visits_deduped_v3: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="location-visits-hourly",
        hour_format="hour={hour:0>2d}",
        success_file=None,
        version=3,
    )

    location_visits_cross_device: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="location-visits-cross-device",
        version=2,
    )

    location_visits_aggregated: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="location-visits-targeting-data-aggregated",
        hour_format="hour={hour}",
        version=1,
    )

    cold_storage_lookup_output: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="cold-storage-lookup-output",
        hour_format="hour={hour}",
        version=1,
    )

    cold_storage_lookup_output_v2: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="cold-storage-lookup-output",
        hour_format="hour={hour:0>2d}",
        version=2,
    )

    cold_storage_lookup_output_xd: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh/xd",
        data_name="cold-storage-lookup-output",
        hour_format="hour={hour}",
        version=1,
    )

    audience_targeting_model: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="dooh-audience-targeting-model-results",
        hour_format="hour={hour}",
        version=1,
    )

    on_target_visit_counts: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="observed-on-target-visit-counts",
        hour_format="hour={hour}",
        version=2,
    )

    arp_baseline: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="dooh-arp-baseline",
        date_format="lookbackDays=5/date=%Y%m%d",
        hour_format="hour={hour:0>2d}",
        version=2,
    )

    arp_report: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="arp-report-without-baseline",
        date_format="lookbackDays=5/date=%Y%m%d",
        hour_format="hour={hour:0>2d}",
        version=5,
    )

    total_visit_counts: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="total-visits-counts",
        hour_format="hour={hour}",
        version=1,
    )

    otp_v2: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="otp-report",
        hour_format="hour={hour}",
        version=2,
    )

    otp_with_historical_data: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="otp-report",
        hour_format="hour={hour}",
        version=3,
    )

    used_audiences: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="used_audiences",
        hour_format="hour={hour}",
        version=2,
    )

    sa_audience_targeting_model: HourGeneratedDataset = HourGeneratedDataset(
        bucket="sa-3317-dooh-data",
        path_prefix="model-data",
        data_name="",
        hour_format="hour={hour}",
        version=1,
    )

    active_adgroups: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="dooh-active-adgroups",
        version=1,
    )

    extra_audiences: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="dooh-extra-audiences",
        version=1,
    )

    cold_storage_cross_device_lookup_output: DateGeneratedDataset = (
        DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline/dooh",
            data_name="cold-storage-cross-device-lookup-output",
            version=1,
        )
    )

    dooh_avail_logs_hour: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-logs-2",
        path_prefix="doohavails",
        data_name="collected",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        version=None,
        success_file=None,
        env_aware=False,
    )

    arp_tiers_model: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="dooh-arp-tiers-model-results",
        hour_format="hour={hour}",
        version=1,
    )

    campaign_visualisation_bid_feedback: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/dooh",
        data_name="dooh-campaign-visualisation",
        hour_format="hour={hour:0>2d}",
        success_file=None,
        version=5,
    )
