from datetime import datetime, timedelta
from airflow import DAG
from dags.jan.janus_kpis.core import JanusRollupKPIDag, CURRENT_HOUR, CURRENT_HOUR_DS_CHECK
from dags.jan.janus_kpis.datasets.intermediate import IntermediateJanusKPIDatasets
from ttd.tasks.op import OpTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

winrate_hourly_aggregator: DAG = JanusRollupKPIDag(
    metric="winrate-hourly-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.WinRateTransformer",
    config_options=[
        ("WinRateTransformer.FromJanus", "true"),
        ("WinRateTransformer.rollupTime", CURRENT_HOUR),
        ("WinRateTransformer.aggregationLevel", "PartnerAdvertiserCampaignAdGroup"),
    ],
    start_date=datetime(2024, 6, 3, 23, 0),
    schedule_interval="0 * * * *",
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[IntermediateJanusKPIDatasets.bid_request_bid_feedback.as_write()],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 2,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 9,  # in seconds
        )
    )
).create_dag()

plutus_base_bid_request_bid_feedback_hourly_aggregator: DAG = JanusRollupKPIDag(
    metric="bid-request-bid-feedback-hourly-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.BidRequestBidFeedbackTransformer",
    config_options=[
        ("BidRequestBidFeedbackTransformer.FromJanus", "true"),
        ("BidRequestBidFeedbackTransformer.rollupTime", CURRENT_HOUR),
        ("BidRequestBidFeedbackTransformer.aggregationLevel", "PredictiveClearingBase"),
    ],
    start_date=datetime(2024, 6, 3, 23, 0),
    schedule_interval="0 * * * *",
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[IntermediateJanusKPIDatasets.bid_request_bid_feedback.as_write()],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 1,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 9,  # in seconds
        )
    )
).create_dag()

plutus_partner_advertiser_campaign_bid_request_bid_feedback_hourly_aggregator: DAG = JanusRollupKPIDag(
    metric="plutus-partner-advertiser-campaign-bid-request-bid-feedback-hourly-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.BidRequestBidFeedbackTransformer",
    config_options=[
        ("BidRequestBidFeedbackTransformer.FromJanus", "true"),
        ("BidRequestBidFeedbackTransformer.rollupTime", CURRENT_HOUR),
        ("BidRequestBidFeedbackTransformer.aggregationLevel", "PredictiveClearingPartnerAdvertiserCampaign"),
    ],
    start_date=datetime(2025, 7, 1, 23, 0),
    schedule_interval="0 * * * *",
    max_active_runs=3
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[IntermediateJanusKPIDatasets.bid_request_bid_feedback.as_write()],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 2,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 9,  # in seconds
        )
    )
).create_dag()

conversion_advertiser_campaign_adgroup_daily_aggregator: DAG = JanusRollupKPIDag(
    metric="conversion-advertiser-campaign-adgroup-daily-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.ConversionTransformer",
    config_options=[
        ("ConversionTransformer.FromJanus", "true"),
        ("ConversionTransformer.rollupTime", CURRENT_HOUR),
        ("ConversionTransformer.aggregationLevel", "AdvertiserCampaignAdGroup"),
    ],
    start_date=datetime(2024, 9, 29, 0, 0),
    schedule_interval=timedelta(days=1),
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                IntermediateJanusKPIDatasets.attribution_event_aggregate,
                IntermediateJanusKPIDatasets.bid_request_bid_feedback.with_check_type("day"),
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 3,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 12,  # in seconds
        )
    )
).create_dag()

conversion_distributed_algo_advertiser_campaign_adgroup_daily_aggregator: DAG = JanusRollupKPIDag(
    metric="conversion-distributed-algo-adv-cmp-ag-daily-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.ConversionTransformer",
    config_options=[
        ("ConversionTransformer.FromJanus", "true"),
        ("ConversionTransformer.rollupTime", CURRENT_HOUR),
        ("ConversionTransformer.aggregationLevel", "DistributedAlgoAdvertiserCampaignAdGroup"),
    ],
    start_date=datetime(2024, 11, 18, 0, 0),
    schedule_interval=timedelta(days=1),
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                IntermediateJanusKPIDatasets.attribution_event_aggregate,
                IntermediateJanusKPIDatasets.bid_request_bid_feedback.with_check_type("day"),
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 3,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 12,  # in seconds
        )
    )
).create_dag()

revenue_advertiser_campaign_adgroup_daily_aggregator: DAG = JanusRollupKPIDag(
    metric="revenue-advertiser-campaign-adgroup-daily-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.RevenueTransformer",
    config_options=[
        ("RevenueTransformer.FromJanus", "true"),
        ("RevenueTransformer.rollupTime", CURRENT_HOUR),
        ("RevenueTransformer.aggregationLevel", "AdvertiserCampaignAdGroup"),
    ],
    start_date=datetime(2024, 12, 17, 0, 0),
    schedule_interval=timedelta(days=1),
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                IntermediateJanusKPIDatasets.attribution_event_aggregate,
                IntermediateJanusKPIDatasets.bid_request_bid_feedback.with_check_type("day"),
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 3,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 12,  # in seconds
        )
    )
).create_dag()

revenue_distributed_algo_advertiser_campaign_adgroup_daily_aggregator: DAG = JanusRollupKPIDag(
    metric="revenue-distributed-algo-adv-cmp-ag-daily-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.RevenueTransformer",
    config_options=[
        ("RevenueTransformer.FromJanus", "true"),
        ("RevenueTransformer.rollupTime", CURRENT_HOUR),
        ("RevenueTransformer.aggregationLevel", "DistributedAlgoAdvertiserCampaignAdGroup"),
    ],
    start_date=datetime(2024, 12, 17, 0, 0),
    schedule_interval=timedelta(days=1),
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                IntermediateJanusKPIDatasets.attribution_event_aggregate,
                IntermediateJanusKPIDatasets.bid_request_bid_feedback.with_check_type("day"),
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 3,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 12,  # in seconds
        )
    )
).create_dag()

click_distributed_algo_partner_advertiser_campaign_adgroup_hourly_aggregator: DAG = JanusRollupKPIDag(
    metric="click-da-ptnr-adv-cmp-ag-hourly-agg",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.rollup.ClickTransformer",
    config_options=[
        ("ClickTransformer.FromJanus", "true"),
        ("ClickTransformer.rollupTime", CURRENT_HOUR),
        ("ClickTransformer.aggregationLevel", "DistributedAlgoPartnerAdvertiserCampaignAdGroup"),
    ],
    start_date=datetime(2024, 11, 22, 0, 0),
    schedule_interval="0 * * * *",
    max_active_runs=3,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[IntermediateJanusKPIDatasets.click_aggregate, IntermediateJanusKPIDatasets.bid_request_bid_feedback],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 1,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 9,  # in seconds
        )
    )
).create_dag()
