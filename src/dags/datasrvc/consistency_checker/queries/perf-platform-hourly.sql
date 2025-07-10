select /*+label(PerfAndPlatformHourlyConsistencyCheck_Airflow)*/ * from (
with raw as (
    select
        date_trunc('hour', LogEntryTime) as Date,
        count(*) as BidCount,
        0 as ImpressionCount,
        0 as ClickCount,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0.0 as DataCostInUSD,
        0.0 as TTDCostInUSD,
        0.0 as TTDCostInAdvertiserCurrency,
        0.0 as TTDCostInPartnerCurrency,
        0.0 as PartnerCostInUSD,
        0.0 as AdvertiserCostInUSD,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.BidRequest
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1
    union all
    select
        date_trunc('hour', LogEntryTime) as Date,
        0 as BidCount,
        count(*) as ImpressionCount,
        0 as ClickCount,
        zeroifnull(sum(WinningPriceCPMInBucks)) / 1000.0 as MediaCostInUSD,
        zeroifnull(sum(FeeFeaturesCost)) as FeeFeaturesCostInUSD,
        zeroifnull(sum(DataUsageTotalCost)) as DataCostInUSD,
        zeroifnull(sum(TTDCostInUSD)) as TTDCostInUSD,
        zeroifnull(sum(TTDCostInUSD * AdvertiserCurrencyExchangeRate)) as TTDCostInAdvertiserCurrency,
        zeroifnull(sum(TTDCostInUSD * PartnerCurrencyExchangeRate)) as TTDCostInPartnerCurrency,
        zeroifnull(sum(PartnerCostInUSD)) as PartnerCostInUSD,
        zeroifnull(sum(AdvertiserCostInUSD)) as AdvertiserCostInUSD,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.BidFeedback
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1
    union all
    select
        date_trunc('hour', LogEntryTime) as Date,
        0 as BidCount,
        0 as ImpressionCount,
        count(*) as ClickCount,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0.0 as DataCostInUSD,
        zeroifnull(sum(TTDCostInUSD)) as TTDCostInUSD,
        zeroifnull(sum(TTDCostInUSD * AdvertiserCurrencyExchangeRate)) as TTDCostInAdvertiserCurrency,
        zeroifnull(sum(TTDCostInUSD * PartnerCurrencyExchangeRate)) as TTDCostInPartnerCurrency,
        zeroifnull(sum(PartnerCostInUSD)) as PartnerCostInUSD,
        zeroifnull(sum(AdvertiserCostInUSD)) as AdvertiserCostInUSD,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.ClickTracker
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    and PlacementId is null   -- exclude fixed price clicks
    and AdGroupId is not null -- exclude other non RTB (e.g.search, test) clicks
   group by 1
   union all
   select
        date_trunc('hour', LogEntryTime) as Date,
        0 as BidCount,
        0 as ImpressionCount,
        0 as ClickCount,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0.0 as DataCostInUSD,
        0.0 as TTDCostInUSD,
        0.0 as TTDCostInAdvertiserCurrency,
        0.0 as TTDCostInPartnerCurrency,
        0.0 as PartnerCostInUSD,
        0.0 as AdvertiserCostInUSD,
        sum(VideoEventStart) as VideoEventStartCount,
        sum(VideoEventComplete) as VideoEventCompleteCount,
        sum(case when CreativeIsTrackable then 1 else 0 end) as CreativeIsTrackableCount,
        sum(case when CreativeWasViewable then 1 else 0 end) as CreativeWasViewableCount,
        0 as Touch1Count,
        0 as Touch2Count
    from ttd.VideoEvent
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}' and IncludeInReports = true
    group by 1
    union all
    select
        date_trunc('hour', ReportHourUtc) as Date,
        0 as BidCount,
        0 as ImpressionCount,
        0 as ClickCount,
        0.0 as MediaCostInUSD,
        0.0 as FeeFeaturesCostInUSD,
        0.0 as DataCostInUSD,
        0.0 as TTDCostInUSD,
        0.0 as TTDCostInAdvertiserCurrency,
        0.0 as TTDCostInPartnerCurrency,
        0.0 as PartnerCostInUSD,
        0.0 as AdvertiserCostInUSD,
        0 as VideoEventStartCount,
        0 as VideoEventCompleteCount,
        0 as CreativeIsTrackableCount,
        0 as CreativeWasViewableCount,
        sum(Touch1Count) as Touch1Count,
        sum(Touch2Count) as Touch2Count
    from ttd.AttributedEventPivot -- AttributionInput
    where ReportHourUtc >= '{startDate}' and ReportHourUtc < '{endDate}'
    and LateDataProviderId is null -- will not contain values for Integral, so it is a safe operation
    group by 1
)
select
    Date,
    '{dataDomainName}' as DataDomainName,
    'raw' as Source,
    zeroifnull(sum(BidCount)) as BidCount,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount,
    zeroifnull(sum(ClickCount)) as ClickCount,
    zeroifnull(sum(MediaCostInUSD)) as MediaCostInUSD,
    zeroifnull(sum(DataCostInUSD)) as DataCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInUSD)) as FeeFeaturesCostInUSD,
    zeroifnull(sum(TTDCostInUSD)) as TTDCostInUSD,
    zeroifnull(sum(TTDCostInAdvertiserCurrency)) as TTDCostInAdvertiserCurrency,
    zeroifnull(sum(TTDCostInPartnerCurrency)) as TTDCostInPartnerCurrency,
    zeroifnull(sum(PartnerCostInUSD)) as PartnerCostInUSD,
    zeroifnull(sum(AdvertiserCostInUSD)) as AdvertiserCostInUSD,
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
    date_trunc('hour', ReportHourUTC) as Date,
    DataDomainName,
    'perf_report' as Source,
    zeroifnull(sum(BidCount)) as BidCount,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount,
    zeroifnull(sum(ClickCount)) as ClickCount,
    zeroifnull(sum(MediaCostInUSD)) as MediaCostInUSD,
    zeroifnull(sum(DataCostInUSD)) as DataCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInUSD)) as FeeFeaturesCostInUSD,
    zeroifnull(sum(TTDCostInUSD)) as TTDCostInUSD,
    zeroifnull(sum(TTDCostInAdvertiserCurrency)) as TTDCostInAdvertiserCurrency,
    zeroifnull(sum(TTDCostInPartnerCurrency)) as TTDCostInPartnerCurrency,
    zeroifnull(sum(PartnerCostInUSD)) as PartnerCostInUSD,
    zeroifnull(sum(AdvertiserCostInUSD)) as AdvertiserCostInUSD,
    zeroifnull(sum(VideoEventStartCount)) as VideoEventStartCount,
    zeroifnull(sum(VideoEventCompleteCount)) as VideoEventCompleteCount,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(CreativeWasViewableCount)) as CreativeWasViewableCount,
    zeroifnull(sum(Touch1Count)) as Touch1Count,
    zeroifnull(sum(Touch2Count)) as Touch2Count
from reports.vw_PerformanceReport
inner join provisioning2.Tenant using (TenantId)
inner join provisioning2.DataDomain using (DataDomainId)
where ReportHourUTC >= '{startDate}' and ReportHourUTC< '{endDate}'
and LateDataProviderId is null
group by 1,2
union all
select
    date_trunc('hour', ReportHourUTC) as Date,
    DataDomainName,
    'super_report' as Source,
    zeroifnull(sum(BidCount)) as BidCount,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount,
    zeroifnull(sum(ClickCount)) as ClickCount,
    zeroifnull(sum(MediaCostInUSD))::numeric(37,15)  as MediaCostInUSD,
    zeroifnull(sum(DataCostInUSD))::numeric(37,15)  as DataCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInUSD))::numeric(37,15)  as FeeFeaturesCostInUSD,
    zeroifnull(sum(TTDCostInUSD))::numeric(37,15)  as TTDCostInUSD,
    zeroifnull(sum(TTDCostInAdvertiserCurrency))::numeric(37,15)  as TTDCostInAdvertiserCurrency,
    zeroifnull(sum(TTDCostInPartnerCurrency))::numeric(37,15)  as TTDCostInPartnerCurrency,
    zeroifnull(sum(PartnerCostInUSD))::numeric(37,15)  as PartnerCostInUSD,
    zeroifnull(sum(AdvertiserCostInUSD))::numeric(37,15)  as AdvertiserCostInUSD,
    zeroifnull(sum(VideoEventStartCount)) as VideoEventStartCount,
    zeroifnull(sum(VideoEventCompleteCount)) as VideoEventCompleteCount,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(CreativeWasViewableCount)) as CreativeWasViewableCount,
    zeroifnull(sum(Touch1Count)) as Touch1Count,
    zeroifnull(sum(Touch2Count)) as Touch2Count
from reports.RTBPlatformReport
inner join provisioning2.DataDomain using (DataDomainId)
where ReportHourUTC >= '{startDate}' and ReportHourUTC< '{endDate}'
and LateDataProviderId is null
group by 1,2 ) labeled