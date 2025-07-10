select /*+label(PerfDataElementHourlyConsistencyCheck_Airflow)*/ * from (
with raw as (
    select
        date_trunc('hour', LogEntryTime) as Date,
        0.0 as TTDCostInUSD,
        0.0 as TTDCostInAdvertiserCurrency,
        0.0 as TTDCostInPartnerCurrency,
        0.0 as PartnerCostInUSD,
        0.0 as AdvertiserCostInUSD,
        count(*) as DataBidCount,
        0 as UsedDataImpressionCount,
        0.0 as UsedDataCostInUSD,
        0 as NotUsedDataImpressionCount,
        0.0 as NotUsedDataCostInUSD,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0 as ClickCount,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.BidRequestDataElement
    where LogEntryTime >= '{startDate}' and LogEntryTime < '{endDate}'
    group by 1
    union all
    select
        date_trunc('hour', LogEntryTime) as Date,
        sum(TTDCostInUSD) as TTDCostInUSD,
        sum(TTDCostInUSD * AdvertiserCurrencyExchangeRate) as TTDCostInAdvertiserCurrency,
        sum(TTDCostInUSD * PartnerCurrencyExchangeRate) as TTDCostInPartnerCurrency,
        sum(PartnerCostInUSD) as PartnerCostInUSD,
        sum(AdvertiserCostInUSD) as AdvertiserCostInUSD,
        0 as DataBidCount,
        sum( case when Used = true then 1 else 0 end ) as UsedDataImpressionCount,
        sum( case when Used = true then DataCostInUSD else 0 end ) as UsedDataCostInUSD,
        sum( case when Used = true then 0 else 1 end ) as NotUsedDataImpressionCount,
        sum( case when Used = true then 0 else DataCostInUSD end ) as NotUsedDataCostInUSD,
        sum( WinningPriceCPMInUSD ) / 1000.0 as MediaCostInUSD,
        sum( FeeFeaturesCostInUSD ) as FeeFeaturesCostInUSD,
        0 as ClickCount,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.BidFeedbackDataElement
    where LogEntryTime >= '{startDate}' and LogEntryTime < '{endDate}'
    group by 1
    union all
    select
        date_trunc('hour', LogEntryTime) as Date,
        sum( TTDCostInUSD ) as TTDCostInUSD,
        sum( TTDCostInUSD * AdvertiserCurrencyExchangeRate ) as TTDCostInAdvertiserCurrency,
        sum( TTDCostInUSD * PartnerCurrencyExchangeRate ) as TTDCostInPartnerCurrency,
        sum( PartnerCostInUSD ) as PartnerCostInUSD,
        sum( AdvertiserCostInUSD ) asAdvertiserCostInUSD,
        0 as DataBidCount,
        0 as UsedDataImpressionCount,
        0.0 as UsedDataCostInUSD,
        0 as NotUsedDataImpressionCount,
        0.0 as NotUsedDataCostInUSD,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        count(*) as ClickCount,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.ClickTrackerDataElement
    where LogEntryTime >= '{startDate}' and LogEntryTime < '{endDate}'
    group by 1
    union all
    select
        date_trunc('hour', LogEntryTime) as Date,
        0.0 as TTDCostInUSD,
        0.0 as TTDCostInAdvertiserCurrency,
        0.0 as TTDCostInPartnerCurrency,
        0.0 as PartnerCostInUSD,
        0.0 as AdvertiserCostInUSD,
        0 as DataBidCount,
        0 as UsedDataImpressionCount,
        0.0 as UsedDataCostInUSD,
        0 as NotUsedDataImpressionCount,
        0.0 as NotUsedDataCostInUSD,
        0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0 as ClickCount,
        sum( VideoEventStart ) as VideoEventStartCount,
        sum( VideoEventComplete ) as VideoEventCompleteCount,
        sum( case when CreativeIsTrackable then 1 else 0 end ) as CreativeIsTrackableCount,
        sum( case when CreativeWasViewable then 1 else 0 end ) as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.VideoEventDataElement
    where LogEntryTime >= '{startDate}' and LogEntryTime < '{endDate}'
    group by 1
    union all
    select
        date_trunc('hour', ReportHourUtc) as Date,
        0.0 as TTDCostInUSD,
        0.0 as TTDCostInAdvertiserCurrency,
        0.0 as TTDCostInPartnerCurrency,
        0.0 as PartnerCostInUSD,
        0.0 as AdvertiserCostInUSD,
        0 as DataBidCount,
        0 as UsedDataImpressionCount,
        0.0 as UsedDataCostInUSD,
        0 as NotUsedDataImpressionCount,
        0.0 as NotUsedDataCostInUSD,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0 as ClickCount,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        sum(Touch1Count) as Touch1Count,
        sum(Touch2Count) as Touch2Count
    from ttd.AttributedEventDataElementPivot
    where ReportHourUtc >= '{startDate}' and ReportHourUtc < '{endDate}'
    and LateDataProviderId is null
    group by 1
), de as (
    select
        date_trunc('hour', ReportHourUTC) as Date,
        DataDomainId,
        sum(TTDCostInUSD)::numeric(37,15)  as TTDCostInUSD,
        sum(TTDCostInAdvertiserCurrency)::numeric(37,15)  as TTDCostInAdvertiserCurrency,
        sum(TTDCostInPartnerCurrency)::numeric(37,15)  as TTDCostInPartnerCurrency,
        sum(PartnerCostInUSD)::numeric(37,15)  as PartnerCostInUSD,
        sum(AdvertiserCostInUSD)::numeric(37,15)  as AdvertiserCostInUSD,
        sum(DataBidCount) as DataBidCount,
        sum(UsedDataImpressionCount) as UsedDataImpressionCount,
        sum(UsedDataCostInUSD)::numeric(37,15)  as UsedDataCostInUSD,
        sum(NotUsedDataImpressionCount) as NotUsedDataImpressionCount,
        sum(NotUsedDataCostInUSD)::numeric(37,15)  as NotUsedDataCostInUSD,
        sum(MediaCostInUSD)::numeric(37,15)  as MediaCostInUSD,
        sum(FeeFeaturesCostInUSD)::numeric(37,15)  as FeeFeaturesCostInUSD,
        sum(ClickCount) as ClickCount,
        sum(VideoEventStartCount) as VideoEventStartCount,
        sum(VideoEventCompleteCount) as VideoEventCompleteCount,
        sum( CreativeIsTrackableCount) as CreativeIsTrackableCount,
        sum( CreativeWasViewableCount ) as CreativeWasViewableCount,
        sum(Touch1Count) as Touch1Count,
        sum(Touch2Count) as Touch2Count
    from reports.RTBPlatformDataElementReport
    where ReportHourUTC >= '{startDate}' and ReportHourUTC < '{endDate}'
    and LateDataProviderId is null -- should not contain integral data, so safe operation to do
    group by 1,2
)
select
    Date,
    '{dataDomainName}' as DataDomainName,
    'raw' as Source,
    zeroifnull(sum(TTDCostInUSD)) as TTDCostInUSD,
    zeroifnull(sum(TTDCostInAdvertiserCurrency)) as TTDCostInAdvertiserCurrency,
    zeroifnull(sum(TTDCostInPartnerCurrency)) as TTDCostInPartnerCurrency,
    zeroifnull(sum(PartnerCostInUSD)) as PartnerCostInUSD,
    zeroifnull(sum(AdvertiserCostInUSD)) as AdvertiserCostInUSD,
    zeroifnull(sum(DataBidCount)) as DataBidCount,
    zeroifnull(sum(UsedDataImpressionCount)) as UsedDataImpressionCount,
    zeroifnull(sum(UsedDataCostInUSD)) as UsedDataCostInUSD,
    zeroifnull(sum(NotUsedDataImpressionCount)) as NotUsedDataImpressionCount,
    zeroifnull(sum(NotUsedDataCostInUSD)) as NotUsedDataCostInUSD,
    zeroifnull(sum(MediaCostInUSD)) as MediaCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInUSD)) as FeeFeaturesCostInUSD,
    zeroifnull(sum(ClickCount)) as ClickCount,
    zeroifnull(sum(VideoEventStartCount)) as VideoEventStartCount,
    zeroifnull(sum(VideoEventCompleteCount)) as VideoEventCompleteCount,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(CreativeWasViewableCount)) as CreativeWasViewableCount,
    zeroifnull(sum(Touch1Count)) as Touch1Count,
    zeroifnull(sum(Touch2Count)) as Touch2Count
from raw
group by 1,2
union all
select
    Date,
    DataDomainName,
    'perf_report' as Source,
    zeroifnull(sum(TTDCostInUSD))::numeric(37,15)  as TTDCostInUSD,
    zeroifnull(sum(TTDCostInAdvertiserCurrency))::numeric(37,15)  as TTDCostInAdvertiserCurrency,
    zeroifnull(sum(TTDCostInPartnerCurrency))::numeric(37,15)  as TTDCostInPartnerCurrency,
    zeroifnull(sum(PartnerCostInUSD))::numeric(37,15)  as PartnerCostInUSD,
    zeroifnull(sum(AdvertiserCostInUSD))::numeric(37,15)  as AdvertiserCostInUSD,
    zeroifnull(sum(DataBidCount)) as DataBidCount,
    zeroifnull(sum(UsedDataImpressionCount)) as UsedDataImpressionCount,
    zeroifnull(sum(UsedDataCostInUSD))::numeric(37,15)  as UsedDataCostInUSD,
    zeroifnull(sum(NotUsedDataImpressionCount)) as NotUsedDataImpressionCount,
    zeroifnull(sum(NotUsedDataCostInUSD))::numeric(37,15)  as NotUsedDataCostInUSD,
    zeroifnull(sum(MediaCostInUSD))::numeric(37,15)  as MediaCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInUSD))::numeric(37,15)  as FeeFeaturesCostInUSD,
    zeroifnull(sum(ClickCount)) as ClickCount,
    zeroifnull(sum(VideoEventStartCount)) as VideoEventStartCount,
    zeroifnull(sum(VideoEventCompleteCount)) as VideoEventCompleteCount,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(CreativeWasViewableCount)) as CreativeWasViewableCount,
    zeroifnull(sum(Touch1Count)) as Touch1Count,
    zeroifnull(sum(Touch2Count)) as Touch2Count
from de
inner join provisioning2.DataDomain using (DataDomainId)
group by 1,2 ) labeled