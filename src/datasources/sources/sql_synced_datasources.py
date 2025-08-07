from ttd.datasets.hour_dataset import HourSqlSyncedDataset


class SQLSyncedDataSources:
    acr_provider_exposure_segments: HourSqlSyncedDataset = HourSqlSyncedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external/thetradedesk.db/provisioning",
        data_name="acrproviderbrandexposuresegments_v2",
        version=1,
        success_file=None,
        env_aware=False,
    )

    acr_provider_segments: HourSqlSyncedDataset = HourSqlSyncedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external/thetradedesk.db/provisioning",
        data_name="acrprovidersegment_v2",
        version=1,
        success_file=None,
        env_aware=False,
    )
