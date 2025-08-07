from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.date_external_dataset import DateExternalDataset


class SambaDatasources:
    daily_brand_aggs: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="linear/acr",
        data_name="dailyAggs",
        version=1,
    )

    def samba_enriched_v3(country):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"samba-enriched/country={country}",
            date_format="date=%Y%m%d",
            version=2,
            env_aware=True,
        )

    samba_enriched: DateExternalDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="linear/acr",
        data_name="samba-enriched",
        date_format="date=%Y%m%d",
        version=2,
        env_aware=True,
        success_file=None,  # this will change but as of right now we dont use the writer class.
    )

    samba_au_ip_public_id_mapping: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-partner-datafeed",
        path_prefix="sambatv",
        data_name="ttd-idmapping-au",
        date_format="year=%Y/month=%-m/day=%-d",
    )

    samba_au_impression_feed: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-partner-datafeed",
        path_prefix="sambatv-ttd",
        data_name="au-feeds",
        date_format="yyyy=%Y/mm=%m/dd=%d",
    )

    samba_au_tradedesk_id_ip_mapping: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-partner-datafeed",
        path_prefix="sambatv-ttd",
        data_name="au-match-file",
        date_format="year=%Y/month=%m/day=%d",
    )

    samba_au_tradedesk_id_ip_mapping_v2: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-partner-datafeed",
        path_prefix="sambatv-ttd",
        data_name="au-match-file-v2",
        date_format="yyyy=%Y/mm=%m/dd=%d"
    )

    samba_ca_impression_feed: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-partner-datafeed",
        path_prefix="sambatv-ttd",
        data_name="CA/ca-feeds",
        date_format="yyyy=%Y/mm=%m/dd=%d"
    )

    samba_ca_tradedesk_id_ip_mapping: DateExternalDataset = DateExternalDataset(
        bucket="thetradedesk-useast-partner-datafeed",
        path_prefix="sambatv-ttd",
        data_name="CA/ca-match-file",
        date_format="yyyy=%Y/mm=%m/dd=%d"
    )

    samba_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="linear/acr",
        data_name="dailyAggs/prod",
        date_format="date=%Y%m%d",
        version=1,
        env_aware=False,
        success_file=None,
    )

    samba_brand_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="linear/acr",
        data_name="brandAggs/prod",
        date_format="date=%Y%m%d",
        version=1,
        env_aware=False,
        success_file=None,
    )
