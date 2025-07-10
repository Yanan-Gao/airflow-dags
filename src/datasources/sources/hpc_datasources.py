from ttd.datasets.hour_dataset import HourGeneratedDataset


class HpcDatasources:
    _data_ingestion_bucket = "thetradedesk-useast-logs-2"

    full_users_data_export: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_data_ingestion_bucket,
        path_prefix="fulluserdataexport",
        data_name="collected",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        version=None,
        success_file=None,
        env_aware=False
    )
