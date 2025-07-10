from ttd.datasets.dataset import SUCCESS
from ttd.datasets.date_external_dataset import DateExternalDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration


class CtvDatasources:
    nielsen_bucket = "thetradedesk-useast-data-import"

    unified_open_graph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        azure_bucket="ttd-insights",
        path_prefix="graph",
        data_name="unified-open-graph",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=True,
        buckets_for_other_regions={
            "us-west-2": "thetradedesk-uswest-2-avails",
            "ap-northeast-1": "thetradedesk-jp1-avails",
            "ap-southeast-1": "thetradedesk-sg2-avails",
            "eu-west-1": "thetradedesk-ie1-avails",
            "eu-central-1": "thetradedesk-de2-avails"
        }
    )

    owdi_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-datprd-us-east-1",
        path_prefix="application/radar/data/export/general",
        data_name="Demographic",
        date_format="Date=%Y-%m-%d",
        success_file=None,
        version=None,
        env_aware=False
    )

    legacy_iav2_owdi_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-datprd-us-east-1",
        path_prefix="application/radar/data/export/general",
        data_name="LegacyIav2PersonDemographic",
        date_format="Date=%Y-%m-%d",
        success_file=None,
        version=None,
        env_aware=False
    )

    # TODO: Once open graph is well tested, need to migrate to open graph ids
    open_graph_iav2_owdi_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-datprd-us-east-1",
        path_prefix="application/radar/data/export/general",
        data_name="Iav2PersonDemographic",
        date_format="Date=%Y-%m-%d",
        success_file=None,
        version=None,
        env_aware=False
    )

    open_graph_adbrain_owdi_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-datprd-us-east-1",
        path_prefix="application/radar/data/export/general",
        data_name="OpenGraphPersonDemographic",
        date_format="Date=%Y-%m-%d",
        success_file=None,
        version=None,
        env_aware=False
    )

    # Deprecated. Please use `ttd.identity_graphs.identity_graphs.IdentityGraphs` instead.
    # This is leagcy graph, once open graph is well tested, need to use open graph defined below
    # https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?spaceKey=EN&title=OpenGraph+Release
    iav2_legacy_graph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="sxd-etl/universal",
        data_name="iav2graph_legacy",
        date_format="%Y-%m-%d",
        version=None,
        success_file=SUCCESS,
        env_aware=False
    )

    fwm_liveramp_element_tdid_mapping_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="TBD",
        path_prefix="TBD",
        data_name="DBD",
        date_format="date=%Y%m%d",
        version=None,
        env_aware=False,
    )

    external_nbcu_device_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="nbcuas-audience-graph",
        path_prefix="dev",
        data_name="device_data_generic",
        date_format="%Y/%m/%d/00",
        version=None,
        env_aware=False,
    )

    internal_nbcu_device_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="nbcu",
        data_name="device_data_generic",
        date_format="date=%Y%m%d",
        version=None,
        env_aware=False,
    )

    ctv_avails_CSV: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-logs-2",
        path_prefix="ctvdeviceavails",
        data_name="collected",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        version=None,
        success_file=None,
        env_aware=False,
    )

    brand_uniques: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="linear/acr",
        data_name="brandUniqueCountsAgg",
        version=1,
    )

    household_bloom_filter: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        data_name="householdAvailsBloomFilter",
        version=1,
    )

    app_site_map: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/mobileapplicationidnamemap",
        version=1,
        success_file=None,
        env_aware=False,
    )

    imdb: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="imdb/prod",
        data_name="",
        date_format="%Y%m%d",
        version=1,
        success_file=None,
        env_aware=False,
    )

    @staticmethod
    def get_upstream_forecast_fmap(version: str = "v2") -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket="ttd-ctv",
            path_prefix=f"upstream-forecast/data-preparation-{version}",
            data_name="frequencyDistributionsPartitioned/900",
            date_format="date=%Y%m%d",
            version=None,
            success_file=None,
            env_aware=True
        )

    upstream_forecast_fmap_v2: DateGeneratedDataset = get_upstream_forecast_fmap("v2")
    upstream_forecast_fmap_v3: DateGeneratedDataset = get_upstream_forecast_fmap("v3")

    upstream_forecast_demo_weights_v2: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="upstream-forecast",
        data_name="demos/weighted",
        date_format="date=%Y%m%d",
        version=None,
        success_file=None,
        env_aware=True
    )

    upstream_forecast_demo_weights_v3: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="upstream-forecast/data-preparation-v3",
        data_name="owdi-demos/weighted",
        date_format="date=%Y%m%d",
        version=None,
        success_file=None,
        env_aware=True
    )

    upstream_forecast_demo_weights_og: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="upstream-forecast/data-preparation-og",
        data_name="owdi-demos/weighted",
        date_format="date=%Y%m%d",
        version=None,
        success_file=None,
        env_aware=True
    )

    upstream_forecast_geo_enriched_graph_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="geo-enriched-graph",
        data_name="graph",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=True,
    )

    upstream_forecast_validation_pass_rate: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="upstream-forecast",
        data_name="validation-sanity/pass-rate",
        date_format="date=%Y%m%d",
        version=None,
        success_file=None,
        env_aware=True,
    )

    upstream_forecast_validation_pass_rate_v3: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="upstream-forecast/data-preparation-v3/validating",
        data_name="validation-sanity/pass-rate",
        date_format="date=%Y%m%d",
        version=None,
        success_file=None,
        env_aware=True,
    )

    upstream_forecast_validation_pass_rate_og: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="upstream-forecast/data-preparation-og/validating",
        data_name="validation-sanity/pass-rate",
        date_format="date=%Y%m%d",
        version=None,
        success_file=None,
        env_aware=True,
    )

    upstream_forecast_profile_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-hadoop",
        path_prefix="unified-data-models",
        data_name="user-profiles/avails",
        date_format="date=%Y-%m-%d",
        version=None,
        env_aware=True,
    )

    upstream_forecast_bloom_filter: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/prod",
        data_name="seeninbidding-hh-avails-bloom-filter/v=1",
        version=None,
        env_aware=True,
    )

    io_consolidation_avails: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="rShiny",
        data_name="IOConsolidation/v=3/availsprocessing",
        version=None,
        env_aware=True,
    )

    io_consolidation_30days_aggregated_avails: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-publisher-insights",
        path_prefix="IOConsolidation",
        data_name="v=6/rollup",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None
    )

    io_consolidation_avails_v2: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-publisher-insights",
        path_prefix="IOConsolidation",
        data_name="v=5/availsprocessing",
        env_path_configuration=MigratedDatasetPathConfiguration(),
        version=None
    )

    io_consolidation: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="rShiny",
        data_name="IOConsolidation/v=3/output",
        date_format="date=%Y%m%d",
        version=None,
    )

    io_consolidation_v2: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-publisher-insights",
        path_prefix="IOConsolidation",
        data_name="v=5/output",
        date_format="date=%Y%m%d",
        env_path_configuration=MigratedDatasetPathConfiguration(),
    )

    user_embeddings: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-user-embeddings",
        path_prefix="dataexport/type=101",
        data_name="",
        version=None,
        date_format="date=%Y%m%d/hour=00/batch=01",  # will always be hour 00 and batch 01
        env_aware=False
    )

    live_events_household_demo_labels_opengraph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv", path_prefix="live-events", data_name="household-demo-labels-opengraph", version=None, date_format="date=%Y-%m-%d"
    )

    live_events_person_demo_labels_opengraph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv", path_prefix="live-events", data_name="person-demo-labels-opengraph", version=None, date_format="date=%Y-%m-%d"
    )

    daily_avails_counts_v2: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="ctv-forecasting-tool",
        data_name="inputs/daily-avail-counts",
        date_format="date=%Y-%m-%d",
        version=2,
        env_aware=True
    )

    daily_avails_counts_v3: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="ctv-forecasting-tool",
        data_name="inputs/daily-avail-counts",
        date_format="date=%Y-%m-%d",
        version=3,
        env_aware=True
    )

    daily_agg_filtered_avails_v2: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="ctv-forecasting-tool",
        data_name="source-data/AggregatedAvailsWithDealCodeHash",
        date_format="date=%Y%m%d",
        version=2,
        env_aware=True,
    )

    daily_agg_filtered_avails_v3: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-ctv",
        path_prefix="ctv-forecasting-tool",
        data_name="source-data/AggregatedAvailsWithDealCodeHash",
        date_format="date=%Y%m%d",
        version=3,
        env_aware=True,
    )

    @staticmethod
    def bidfeedback_daily_subset_no_partition(env: str, version=1):
        return DateGeneratedDataset(
            bucket="ttd-ctv",
            path_prefix=env,
            data_name="OmnichannelView/bidfeedbackdailysubsetnopartition",
            version=version,
            date_format="date=%Y%m%d",
            success_file=SUCCESS if version == 1 else None,
            env_aware=False
        )

    @staticmethod
    def household_channels_embeddings(env: str):
        return DateGeneratedDataset(
            bucket="ttd-ctv",
            path_prefix=env,
            data_name="OmnichannelView/v=1/HouseholdChannelsEmbeddings",
            version=None,
            date_format="date=%Y%m%d",
            success_file=None,
            env_aware=False
        )

    @staticmethod
    def household_feature(country: str):
        return DateGeneratedDataset(
            bucket="ttd-ctv", path_prefix=f"household-feature-library/{country}", data_name="HouseholdFeatureLibraryV2", version=None
        )

    @staticmethod
    def ctv_avails(country: str):
        return HourGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name=f"ctvavails/{country}",
            hour_format="hour={hour}",
            version=1,
        )

    def nielsen_amrld(self, recordtype: str = ""):
        return DateExternalDataset(
            bucket=self.nielsen_bucket,
            path_prefix="nielsen/amrld/parquet/parsed",
            data_name=f"recordtype={recordtype}",
            date_format="filedate=%Y%m%d",
            version=None,
            success_file=None,
        )

    def nielsen_adintel(self, filetype: str = "", mediatype: str = ""):
        return DateExternalDataset(
            bucket=self.nielsen_bucket,
            path_prefix="nielsen/adintel/parquet/parsed",
            data_name=f"filetype={filetype}/mediatype={mediatype}",
            date_format="filedate=%Y%m%d",
            version=None,
            success_file=None,
        )

    def nielsen_adintel_v2(self, filetype: str = "", mediatype: str = ""):
        return DateExternalDataset(
            bucket=self.nielsen_bucket,
            path_prefix="nielsen/adintel-v2/parquet/parsed",
            data_name=f"filetype={filetype}/mediatype={mediatype}",
            date_format="filedate=%Y%m%d",
            version=None,
            success_file=None,
        )

    @staticmethod
    def whip_external(data_type: str) -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket="the-trade-desk",
            path_prefix=data_type,
            data_name="old",
            date_format="%Y%m%d",
            version=None,
            env_aware=False,
            success_file=None,
        )

    @staticmethod
    def whip_internal(data_type: str) -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="whip",
            data_name=f"v=1/data={data_type}",
            date_format="date=%Y%m%d",
            version=None,
            success_file=None,
            env_aware=True,
        )
