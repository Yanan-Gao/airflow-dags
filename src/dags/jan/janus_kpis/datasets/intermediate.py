from dags.jan.janus_kpis.datasets.core import IntermediateJanusKPIDateDataset, IntermediateJanusKPIHourDataset


class IntermediateJanusKPIDatasets:
    attribution_event_aggregate = IntermediateJanusKPIDateDataset(dataset="AttributionDailyAggregateDataSet")
    bid_feedback_variant = IntermediateJanusKPIHourDataset(dataset="BidFeedbackVariantDataSet")
    bid_request_bid_feedback = IntermediateJanusKPIHourDataset(dataset="BidRequestBidFeedbackHourlyAggregateDataSet")
    bid_request_variant = IntermediateJanusKPIHourDataset(dataset="BidRequestVariantDataSet")
    click_aggregate = IntermediateJanusKPIHourDataset(dataset="ClickHourlyAggregateDataSet")
    click_variant = IntermediateJanusKPIHourDataset(dataset="ClickVariantDataSet")
