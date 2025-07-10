from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class SegmentDatasources:

    def __init__(self, provider: str):
        self.provider = provider

    def get_segment_dataset(self, country: str, version: int):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"acrsegmentdata/provider={self.provider}/country={country}",
            version=version,
            date_format="date=%Y%m%d/generation=%Y%m%d",
        )

    def get_frequency_dataset(self, country: str, days: int, version: int):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"acrSegmentFrequency/provider={self.provider}/country={country}/days={days}",
            version=version,
            date_format="date=%Y%m%d/generation=%Y%m%d",
        )

    def get_tv_activity_dataset(self, country: str):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"normalized-tv-activity/provider={self.provider}/country={country}",
            version=1,
            date_format="date=%Y%m%d",
        )

    def get_daily_graph_agg(self, country: str, version: int):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"graph/daily-provider-graph-agg/provider={self.provider}/country={country}",
            version=version,
            date_format="date=%Y%m%d",
        )

    def get_acr_graph(self, country: str, version: int):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"graph/acr-hh-graph/provider={self.provider}/country={country}",
            version=version,
            date_format="date=%Y%m%d",
        )

    def get_acr_provider_grain_value_dataset(self, country: str):
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="linear/acr",
            data_name=f"acrProviderGrainValue/provider={self.provider}/country={country}",
            version=1,
            date_format="date=%Y%m%d",
        )
