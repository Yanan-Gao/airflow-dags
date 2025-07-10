from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset


class IdentityDatasets:

    _bucket = "ttd-identity"
    _sources_prefix = "datapipeline/sources"

    attributed_event: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix + "/firstpartydata_v2",
        data_name="attributedevent",
        version=None,
        date_format="date=%Y-%m-%d",
        env_aware=False,
    )

    attributed_event_result: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix + "/firstpartydata_v2",
        data_name="attributedeventresult",
        version=None,
        date_format="date=%Y-%m-%d",
        env_aware=False,
    )

    conversion_tracker: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix + "/firstpartydata_v2",
        data_name="conversiontracker",
        version=None,
        date_format="date=%Y-%m-%d",
        env_aware=False,
    )

    avails_daily: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix,
        data_name="avails-idnt",
        version=None,
        date_format="%Y-%m-%d",
        hour_format="{hour:0>2d}",
        env_aware=False,
    )

    avails_daily_masked_ip: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix,
        data_name="avails-idnt-masked-ip",
        version=None,
        date_format="%Y-%m-%d",
        hour_format="{hour:0>2d}",
        env_aware=False,
    )

    avails_daily_hashed_id: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix,
        data_name="avails-idnt-hashed-id",
        version=None,
        date_format="%Y-%m-%d",
        hour_format="{hour:0>2d}",
        env_aware=False,
    )

    identity_avails_v2: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix,
        data_name="avails-idnt-v2",
        version=None,
        date_format="%Y-%m-%d",
        hour_format="{hour:0>2d}",
        env_aware=False,
    )

    identity_avails_v2_idless: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_bucket,
        path_prefix=_sources_prefix,
        data_name="avails-idnt-idless-v2",
        version=None,
        date_format="%Y-%m-%d",
        hour_format="{hour:0>2d}",
        env_aware=False,
    )

    # unsampled avails hourly agg, identity customized dataset
    avails_v3_hourly_agg: HourGeneratedDataset = HourGeneratedDataset(
        bucket=_bucket,
        path_prefix="datapipeline/prod/availspipeline",
        data_name="avails-pipeline-daily-agg/cleansed",
        version=3,
        date_format="date=%Y-%m-%d",
        hour_format="hour={hour:0>2d}",
        env_aware=False
    )

    # unsampled avails daily agg, identity customized dataset
    avails_v3_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket,
        path_prefix="data/prod/events",
        data_name="avails-pipeline-daily-agg/idfull",
        version=3,
        date_format="date=%Y-%m-%d",
        env_aware=False
    )

    # unsampled avails daily agg idless, identity customized dataset
    avails_v3_daily_agg_idless: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket,
        path_prefix="data/prod/events",
        data_name="avails-pipeline-daily-agg/idless",
        version=3,
        date_format="date=%Y-%m-%d",
        env_aware=False
    )

    bidfeedback_eu: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket, path_prefix=_sources_prefix, data_name="bidfeedback_eu_v2", version=None, date_format="%Y-%m-%d", env_aware=False
    )

    mobile_walla: DateGeneratedDataset = DateGeneratedDataset(
        bucket=_bucket, path_prefix=_sources_prefix, data_name="mobilewalla", version=None, date_format="%Y-%m-%d", env_aware=False
    )

    # source data of this agg is etl-avails-v2
    avails_daily_agg_v3: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="data",
        data_name="prod/events/avails-daily-agg",
        date_format="date=%Y-%m-%d",
        version=3,
        env_aware=False,
    )

    # source data of this agg is etl-avails-v2
    avails_idless_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="data",
        data_name="prod/events/avails-idless-daily-agg",
        date_format="date=%Y-%m-%d",
        version=3,
        env_aware=False,
    )

    original_ids_overall_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@ttdexportdata",
        path_prefix="data",
        data_name="events/bidfeedback/originalIds-overall-agg",
        date_format="date=%Y-%m-%d",
        version=None,
        env_aware=True
    )

    avails_id_overall_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@ttdexportdata",
        path_prefix="data",
        data_name="events/avails/avails-id-overall-agg",
        date_format="date=%Y-%m-%d",
        version=None,
        env_aware=True
    )

    uuid_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        path_prefix="datapipeline",
        data_name="prod/ttdgraph_uiiddailyagg",
        date_format="date=%Y-%m-%d",
        version=1,
        env_aware=False,
        success_file=None,
    )

    ip_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        path_prefix="datapipeline",
        data_name="prod/ttdgraph_ipdailyagg",
        date_format="date=%Y-%m-%d",
        version=1,
        env_aware=False,
        success_file=None,
    )

    uuid_ip_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        path_prefix="datapipeline",
        data_name="prod/ttdgraph_uiidipdailyagg",
        date_format="date=%Y-%m-%d",
        version=1,
        env_aware=False,
        success_file=None,
    )

    iav2_credit_summary_azure: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        azure_bucket="ttd-identity@ttdexportdata",
        path_prefix="data",
        data_name="non-pii/iav2_credit_summary/v=1/tenantId=2",
        date_format="date=%Y%m%d",
        version=None,
        env_aware=True,
    )

    class GraphCheckPoints:

        @staticmethod
        def _getCheckPoint(name: str):
            return DateGeneratedDataset(
                bucket="ttd-identity",
                path_prefix="data/prod/tmp/uiidIpMetaAgg/graph-checkpoint",
                data_name=name,
                date_format="%Y-%m-%d",
                version=None,
                env_aware=False,
                success_file=None,
            )

        uuidMeta = _getCheckPoint("uiidMeta")
        ipMeta = _getCheckPoint("ipMeta")
        uuidIpDate = _getCheckPoint("uiidIpDate")

    daily_bidrequest = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="deltaData",
        data_name="openGraph/daily/bidRequest",
        date_format="dayDate=%Y-%m-%d",
        version=None,
        success_file=None,
        env_aware=False,
    )

    daily_bidfeedback = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="deltaData",
        data_name="openGraph/daily/bidFeedback",
        date_format="dayDate=%Y-%m-%d",
        version=None,
        success_file=None,
        env_aware=False,
    )

    opengraph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="deltaData",
        data_name="openGraph/published/backwardsCompatibleFormat",
        date_format="dayDate=%Y-%m-%d",
        version=None,
        success_file=None,
        env_aware=False,
    )

    ttdgraph_v1: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="data/prod/graph",
        data_name="adbrain_legacy/person_relabeller",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=True,
    )

    @staticmethod
    def get_post_etl_graph(graph_name: str) -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            azure_bucket="ttd-identity@ttdexportdata",
            path_prefix="sxd-etl/universal",
            data_name=graph_name,
            date_format="%Y-%m-%d/success",
            version=None,
            env_aware=False,
        )

    cookieless_adbrain: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="data/prod/graph",
        data_name="cookielessttdgraph4iav2/person_relabeller",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=True,
    )

    singleton_graph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        path_prefix="graph",
        data_name="prod/singletongraph",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=False,
    )

    @staticmethod
    def get_pre_etl_iav2(iav2_name: str) -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="data/prod/graph",
            data_name=iav2_name,
            date_format="%Y-%m-%d",
            version=None,
            env_aware=True,
        )

    openpass_signin_events: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-openpass-events-data-useast",
        path_prefix="env=prod/events",
        data_name="authentication-attribution-events",
        date_format="date=%Y-%m-%d",
        hour_format="hour={hour:0>2d}",
        success_file=None,
        env_aware=False,
    )

    openpass_user_activity_events: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-openpass-events-data-useast",
        path_prefix="env=prod/events",
        data_name="foundation-program-user-activity-events",
        date_format="date=%Y-%m-%d",
        hour_format="hour={hour:0>2d}",
        success_file=None,
        env_aware=False,
    )

    user_segments_combined: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="data",
        data_name="prod/userSegments/combined/prod",
        date_format="dayDate=%Y-%m-%d",
        version=None,
        success_file=None,
        env_aware=False,
    )

    targetingdatauniquesv2: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/targetingdatauniquesv2",
        env_aware=False,
        version=1,
        success_file=None,
    )

    thirdpartydata: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/thirdpartydata",
        env_aware=False,
        version=1,
        success_file=None,
    )
