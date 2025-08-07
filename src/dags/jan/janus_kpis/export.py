from datetime import datetime, timedelta
from typing import Optional
from airflow import DAG
from dags.jan.janus_kpis.core import (
    JanusExportKPIDag,
    JanusExportAssignmentSourceDag,
    CURRENT_HOUR_DS_CHECK,
    JanusExportHealthCheckDag,
)
from datasources.datasources import Datasources
from ttd.tasks.op import OpTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

winrate_partner_advertiser_campaign_adgroup_hourly_export = JanusExportKPIDag(
    rollup_name="WinRateDataSet.PartnerAdvertiserCampaignAdGroup",
    metric_col="WinRate",
    metric_key_suffix="PartnerAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 6, 3, 23, 0),
    schedule_interval="0 * * * *",
    sensor_metric="winrate-hourly-agg",
).create_dag()


def get_bid_request_bid_feedback_agg_based_export_dag(metric_col: str, aggregation_name: str, start_date: Optional[datetime] = None) -> DAG:
    if start_date is None:
        start_date = datetime(2024, 6, 3, 23, 0)

    return JanusExportKPIDag(
        rollup_name=f"BidRequestBidFeedbackAggDataSet.{aggregation_name}",
        metric_col=metric_col,
        metric_key_suffix=aggregation_name,
        start_date=start_date,
        schedule_interval="0 * * * *",
        sensor_metric="bid-request-bid-feedback-hourly-agg",
    ).create_dag()


base_aggregation = "Base"
plutus_aggregation = "PredictiveClearingBase"
plutus_campaign_aggregation = "PredictiveClearingPartnerAdvertiserCampaign"

winrate_metric = "WinRate"
savings_rate_metric = "SavingsRate"
potential_surplus_metric = "PotentialSurplus"
realized_surplus_metric = "RealizedSurplus"
pc_margin_metric = "PCMargin"
value_per_dollar_by_bid_metric = "ValuePerDollar_ByBid"
value_per_dollar_by_spend_metric = "ValuePerDollar_BySpend"

winrate_base_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(metric_col=winrate_metric, aggregation_name=base_aggregation)
savings_rate_base_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=savings_rate_metric, aggregation_name=base_aggregation
)
potential_surplus_base_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=potential_surplus_metric, aggregation_name=base_aggregation
)
realized_surplus_base_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=realized_surplus_metric, aggregation_name=base_aggregation
)

winrate_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=winrate_metric, aggregation_name=plutus_aggregation
)
savings_rate_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=savings_rate_metric, aggregation_name=plutus_aggregation
)
potential_surplus_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=potential_surplus_metric, aggregation_name=plutus_aggregation
)
realized_surplus_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=realized_surplus_metric, aggregation_name=plutus_aggregation
)
pc_margin_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=pc_margin_metric, aggregation_name=plutus_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
value_per_dollar_by_bid_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=value_per_dollar_by_bid_metric, aggregation_name=plutus_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
value_per_dollar_by_spend_plutus_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=value_per_dollar_by_spend_metric, aggregation_name=plutus_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)

winrate_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=winrate_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
savings_rate_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=savings_rate_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
potential_surplus_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=potential_surplus_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
realized_surplus_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=realized_surplus_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
pc_margin_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=pc_margin_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
value_per_dollar_by_bid_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=value_per_dollar_by_bid_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)
value_per_dollar_by_spend_plutus_campaign_hourly_export = get_bid_request_bid_feedback_agg_based_export_dag(
    metric_col=value_per_dollar_by_spend_metric, aggregation_name=plutus_campaign_aggregation, start_date=datetime(2025, 7, 1, 23, 0)
)

health_check_bid_request_variant_traffichourly_export = JanusExportHealthCheckDag(
    export_name="BidRequestVariantTrafficDataSet",
    start_date=datetime(2025, 7, 1, 23, 0),
    schedule_interval="0 * * * *",
    health_check_sensor_type="bid-request-variant-traffic-hourly",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

health_check_bid_request_variant_noisetopnhourly_export = JanusExportHealthCheckDag(
    export_name="BidRequestVariantNoiseConflictExperimentsDataSet",
    start_date=datetime(2025, 7, 1, 23, 0),
    schedule_interval="0 * * * *",
    health_check_sensor_type="bid-request-variant-noise-top-n-hourly",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

conversion_rate_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="ConversionAggDataSet.AdvertiserCampaignAdGroup",
    metric_col="ConversionRate",
    metric_key_suffix="AdvertiserCampaignAdGroup",
    start_date=datetime(2024, 9, 29),
    schedule_interval=timedelta(days=1),
    sensor_metric="conversion-advertiser-campaign-adgroup-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

cpa_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="ConversionAggDataSet.AdvertiserCampaignAdGroup",
    metric_col="CPA",
    metric_key_suffix="AdvertiserCampaignAdGroup",
    start_date=datetime(2024, 9, 29),
    schedule_interval=timedelta(days=1),
    sensor_metric="conversion-advertiser-campaign-adgroup-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

conversion_rate_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="ConversionAggDataSet.DistributedAlgoAdvertiserCampaignAdGroup",
    metric_col="ConversionRate",
    metric_key_suffix="DistributedAlgoAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 11, 18),
    schedule_interval=timedelta(days=1),
    sensor_metric="conversion-distributed-algo-adv-cmp-ag-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

cpa_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="ConversionAggDataSet.DistributedAlgoAdvertiserCampaignAdGroup",
    metric_col="CPA",
    metric_key_suffix="DistributedAlgoAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 11, 18),
    schedule_interval=timedelta(days=1),
    sensor_metric="conversion-distributed-algo-adv-cmp-ag-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

roas_conversion_rate_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="RevenueAggDataSet.AdvertiserCampaignAdGroup",
    metric_col="ConversionRate",
    metric_key_suffix="AdvertiserCampaignAdGroup",
    start_date=datetime(2024, 12, 17),
    schedule_interval=timedelta(days=1),
    sensor_metric="revenue-advertiser-campaign-adgroup-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

roas_CPA_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="RevenueAggDataSet.AdvertiserCampaignAdGroup",
    metric_col="CPA",
    metric_key_suffix="AdvertiserCampaignAdGroup",
    start_date=datetime(2024, 12, 17),
    schedule_interval=timedelta(days=1),
    sensor_metric="revenue-advertiser-campaign-adgroup-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

roas_cvr_distributed_algo_conversion_rate_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="RevenueAggDataSet.DistributedAlgoAdvertiserCampaignAdGroup",
    metric_col="ConversionRate",
    metric_key_suffix="DistributedAlgoAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 12, 17),
    schedule_interval=timedelta(days=1),
    sensor_metric="revenue-distributed-algo-adv-cmp-ag-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

roas_CPA_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="RevenueAggDataSet.DistributedAlgoAdvertiserCampaignAdGroup",
    metric_col="CPA",
    metric_key_suffix="DistributedAlgoAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 12, 17),
    schedule_interval=timedelta(days=1),
    sensor_metric="revenue-distributed-algo-adv-cmp-ag-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

roas_distributed_algo_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="RevenueAggDataSet.DistributedAlgoAdvertiserCampaignAdGroup",
    metric_col="ROAS",
    metric_key_suffix="DistributedAlgoAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 12, 17),
    schedule_interval=timedelta(days=1),
    sensor_metric="revenue-distributed-algo-adv-cmp-ag-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

roas_revenue_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="RevenueAggDataSet.DistributedAlgoAdvertiserCampaignAdGroup",
    metric_col="Revenue",
    metric_key_suffix="DistributedAlgoAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 12, 17),
    schedule_interval=timedelta(days=1),
    sensor_metric="revenue-distributed-algo-adv-cmp-ag-daily-agg",
    sensor_timeout=60 * 60 * 13,  # in seconds
).create_dag()

ctr_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="ClickAggDataSet.DistributedAlgoPartnerAdvertiserCampaignAdGroup",
    metric_col="CTR",
    metric_key_suffix="DistributedAlgoPartnerAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 11, 22),
    schedule_interval="0 * * * *",
    sensor_metric="click-da-ptnr-adv-cmp-ag-hourly-agg",
    sensor_timeout=60 * 60 * 11,  # in seconds
).create_dag()

cpc_distributed_algo_advertiser_campaign_adgroup_daily_export = JanusExportKPIDag(
    rollup_name="ClickAggDataSet.DistributedAlgoPartnerAdvertiserCampaignAdGroup",
    metric_col="CPC",
    metric_key_suffix="DistributedAlgoPartnerAdvertiserCampaignAdGroup",
    start_date=datetime(2024, 11, 22),
    schedule_interval="0 * * * *",
    sensor_metric="click-da-ptnr-adv-cmp-ag-hourly-agg",
    sensor_timeout=60 * 60 * 11,  # in seconds
).create_dag()

janus_assignment_source_export = JanusExportAssignmentSourceDag(
    export_name="JanusAssignmentSourceExport",
    start_date=datetime(2025, 6, 19),
    schedule_interval="0 * * * *",  # runs every hour at minute 0
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_bidrequest_v5,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 4,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 8,  # in seconds
        )
    )
).create_dag()
