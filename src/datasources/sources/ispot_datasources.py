from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class ISpotDatasources:
    ispot_reach_report: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv", path_prefix="ispot", data_name="ReachReport", date_format="date=%Y%m%d", version=1, env_aware=True
    )

    linear_source_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ispot-ecdi-tradedesk",
        path_prefix="iuld/1",
        data_name="brands_all",
        date_format="%Y%m%d",
        version=None,
        env_aware=False,
        success_file=None,
    )

    linear_target_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="ispot/linear",
        data_name="brands_all",
        date_format="%Y%m%d",
        version=None,
        env_aware=False,
        success_file=None,
    )

    ispot_enriched_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="linear/acr",
        data_name="ispot-enriched",
        date_format="date=%Y%m%d",
        version=1,
        env_aware=True
    )
