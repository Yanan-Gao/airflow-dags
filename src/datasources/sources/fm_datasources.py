from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class ForwardMarketDataSources:
    fm_proto_logs: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-fwdmkt-avails",
        path_prefix="freewheel",
        data_name="",
        date_format="%Y/%m/%d",
        version=None,
        success_file=None,
        env_aware=False,
    )

    fm_proto_to_parquet_output: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-fwdmkt-avails",
        path_prefix="parquet",
        data_name="convertedFromProto",
        version=5,
    )

    fm_coldstorage_cache: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-fwdmkt-avails",
        path_prefix="coldstoragelookupcache",
        data_name="dummy",
    )
