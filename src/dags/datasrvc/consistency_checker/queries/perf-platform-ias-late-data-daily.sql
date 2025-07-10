select /*+label(PerfAndPlatformIASLateDataDailyConsistencyCheck_Airflow)*/ * from (
with raw as (
select 
    date_trunc('day', ReportHourUtc) as Date,
    nullifzero(sum( case when MediaType = 'video' and InvalidTrafficType != 1 and Measurable = true then 1
               when MediaType = 'display' and Measurable = true then 1
               else 0
    end )) as CreativeIsTrackableCount,
    count(1) as IASImpressionCount
from ttd.IntegralData
where ReportHourUtc >= '{startDate}' and ReportHourUtc < '{endDate}' and BidRequestId is not null
group by 1
union all
select 
    date_trunc('day', ReportHourUtc) as Date,
    nullifzero(sum( case when MediaType = 'video' and IASInvalidTrafficType != 1 and Measurable = true then 1
               when MediaType = 'display' and Measurable = true then 1
               else 0
          end )) as CreativeIsTrackableCount,
    count(1) as IASImpressionCount
from ttd.ViewabilityEvent
where ReportHourUtc >= '{startDate}' and ReportHourUtc < '{endDate}' and BidRequestId is not null and ViewabilityProviderId = 1 
group by 1
)
select
    Date,
    '{dataDomainName}' as DataDomainName,
    'raw' as Source,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(IASImpressionCount)) as IASImpressionCount
from raw
group by 1,2
union all
select 
    date_trunc('day', ReportHourUtc) as Date,
    DataDomainName,
    'perf_report' as Source,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(IASImpressionCount)) as IASImpressionCount
from reports.vw_PerformanceReport
inner join provisioning2.DataDomain using (DataDomainId)
where LateDataProviderID = 1 and ReportHourUtc >= '{startDate}' and ReportHourUtc < '{endDate}'
group by 1,2
union all
select 
    date_trunc('day', ReportHourUtc) as Date,
    DataDomainName,
    'plat_report' as Source,
    zeroifnull(sum(CreativeIsTrackableCount)) as CreativeIsTrackableCount,
    zeroifnull(sum(IASImpressionCount)) as IASImpressionCount
from reports.RTBPlatformReport
inner join provisioning2.DataDomain using (DataDomainId)
where LateDataProviderID = 1 and ReportHourUtc >= '{startDate}' and ReportHourUtc < '{endDate}'
group by 1,2
) labeled