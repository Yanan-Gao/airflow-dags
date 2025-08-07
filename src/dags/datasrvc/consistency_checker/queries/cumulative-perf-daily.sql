select /*+label(CumulativePerfDailyConsistencyCheck_Airflow)*/ * from (
with optimizedAccumulation as (
select
    DataDomainId,
    date(ReportDateUtc) as Date,
    CumulativeImpressionCount as ImpressionCount,
    CumulativeAudienceImpressionCount as AudienceImpressionCount,
    CumulativeTTDCostInUSD as TTDCostInUSD,
    CumulativeTTDCostInPartnerCurrency as TTDCostInPartnerCurrency,
    CumulativeTTDCostInAdvertiserCurrency as TTDCostInAdvertiserCurrency,
    CumulativePartnerCostInUSD as PartnerCostInUSD,
    CumulativePartnerCostInPartnerCurrency as PartnerCostInPartnerCurrency,
    CumulativePartnerCostInAdvertiserCurrency as PartnerCostInAdvertiserCurrency,
    CumulativeAdvertiserCostInUSD as AdvertiserCostInUSD,
    CumulativeAdvertiserCostInPartnerCurrency as AdvertiserCostInPartnerCurrency,
    CumulativeAdvertiserCostInAdvertiserCurrency as AdvertiserCostInAdvertiserCurrency
from reports.CumulativePerformanceReport
where ReportDateUtc = (date('{startDate}') - 7)
union all
select
    DataDomainId,
    date(ReportHourUtc) as Date,
    zeroifnull(ImpressionCount) as ImpressionCount,
    zeroifnull(AudienceImpressionCount) as AudienceImpressionCount,
    zeroifnull(TTDCostInUSD) as TTDCostInUSD,
    zeroifnull(TTDCostInPartnerCurrency) as TTDCostInPartnerCurrency,
    zeroifnull(TTDCostInAdvertiserCurrency) as TTDCostInAdvertiserCurrency,
    zeroifnull(PartnerCostInUSD) as PartnerCostInUSD,
    zeroifnull(PartnerCostInPartnerCurrency) as PartnerCostInPartnerCurrency,
    zeroifnull(PartnerCostInAdvertiserCurrency) as PartnerCostInAdvertiserCurrency,
    zeroifnull(AdvertiserCostInUSD) as AdvertiserCostInUSD,
    zeroifnull(AdvertiserCostInPartnerCurrency) as AdvertiserCostInPartnerCurrency,
    zeroifnull(AdvertiserCostInAdvertiserCurrency) as AdvertiserCostInAdvertiserCurrency
from reports.vw_PerformanceReport
where ReportHourUtc >= (date('{startDate}')  - 6) and ReportHourUtc < '{endDate}'
  and LateDataProviderId is null),
aggregatesByDay as (
    select
        DataDomainId,
        Date,
        sum(ImpressionCount) as ImpressionCount,
        sum(AudienceImpressionCount) as AudienceImpressionCount,
        sum(TTDCostInUSD) as TTDCostInUSD,
        sum(TTDCostInPartnerCurrency) as TTDCostInPartnerCurrency,
        sum(TTDCostInAdvertiserCurrency) as TTDCostInAdvertiserCurrency,
        sum(PartnerCostInUSD) as PartnerCostInUSD,
        sum(PartnerCostInPartnerCurrency) as PartnerCostInPartnerCurrency,
        sum(PartnerCostInAdvertiserCurrency) as PartnerCostInAdvertiserCurrency,
        sum(AdvertiserCostInUSD) as AdvertiserCostInUSD,
        sum(AdvertiserCostInPartnerCurrency) as AdvertiserCostInPartnerCurrency,
        sum(AdvertiserCostInAdvertiserCurrency) as AdvertiserCostInAdvertiserCurrency
    from optimizedAccumulation
    group by 1,2
), clouded as (
    select
        DataDomainName,
        Date,
        sum(ImpressionCount) as ImpressionCount,
        sum(AudienceImpressionCount) as AudienceImpressionCount,
        sum(TTDCostInUSD) as TTDCostInUSD,
        sum(TTDCostInPartnerCurrency) as TTDCostInPartnerCurrency,
        sum(TTDCostInAdvertiserCurrency) as TTDCostInAdvertiserCurrency,
        sum(PartnerCostInUSD) as PartnerCostInUSD,
        sum(PartnerCostInPartnerCurrency) as PartnerCostInPartnerCurrency,
        sum(PartnerCostInAdvertiserCurrency) as PartnerCostInAdvertiserCurrency,
        sum(AdvertiserCostInUSD) as AdvertiserCostInUSD,
        sum(AdvertiserCostInPartnerCurrency) as AdvertiserCostInPartnerCurrency,
        sum(AdvertiserCostInAdvertiserCurrency) as AdvertiserCostInAdvertiserCurrency
    from aggregatesByDay a
    inner join provisioning2.DataDomain using (DataDomainId)
    group by 1,2
), cumulative as (
    select
        DataDomainName,
        Date,
        sum(ImpressionCount) over cde as CumulativeImpressionCount,
        sum(AudienceImpressionCount) over cde as CumulativeAudienceImpressionCount,
        sum(TTDCostInUSD) over cde as CumulativeTTDCostInUSD,
        sum(TTDCostInPartnerCurrency) over cde as CumulativeTTDCostInPartnerCurrency,
        sum(TTDCostInAdvertiserCurrency) over cde as CumulativeTTDCostInAdvertiserCurrency,
        sum(PartnerCostInUSD) over cde as CumulativePartnerCostInUSD,
        sum(PartnerCostInPartnerCurrency) over cde as CumulativePartnerCostInPartnerCurrency,
        sum(PartnerCostInAdvertiserCurrency) over cde as CumulativePartnerCostInAdvertiserCurrency,
        sum(AdvertiserCostInUSD) over cde as CumulativeAdvertiserCostInUSD,
        sum(AdvertiserCostInPartnerCurrency) over cde as CumulativeAdvertiserCostInPartnerCurrency,
        sum(AdvertiserCostInAdvertiserCurrency) over cde as CumulativeAdvertiserCostInAdvertiserCurrency
    from clouded
    window cde as (partition by DataDomainName order by Date asc)
), reported as (
    select
        DataDomainName,
        'perf_report' as Source,
        date_trunc('Day', ReportDateUtc) as Date,
        zeroifnull(sum(CumulativeImpressionCount)) as CumulativeImpressionCount,
        zeroifnull(sum(CumulativeAudienceImpressionCount)) as CumulativeAudienceImpressionCount,
        zeroifnull(sum(CumulativeTTDCostInUSD))::numeric(37,15) as CumulativeTTDCostInUSD,
        zeroifnull(sum(CumulativeTTDCostInPartnerCurrency))::numeric(37,15) as CumulativeTTDCostInPartnerCurrency,
        zeroifnull(sum(CumulativeTTDCostInAdvertiserCurrency))::numeric(37,15) as CumulativeTTDCostInAdvertiserCurrency,
        zeroifnull(sum(CumulativePartnerCostInUSD))::numeric(37,15) as CumulativePartnerCostInUSD,
        zeroifnull(sum(CumulativePartnerCostInPartnerCurrency))::numeric(37,15) as CumulativePartnerCostInPartnerCurrency,
        zeroifnull(sum(CumulativePartnerCostInAdvertiserCurrency))::numeric(37,15) as CumulativePartnerCostInAdvertiserCurrency,
        zeroifnull(sum(CumulativeAdvertiserCostInUSD))::numeric(37,15) as CumulativeAdvertiserCostInUSD,
        zeroifnull(sum(CumulativeAdvertiserCostInPartnerCurrency))::numeric(37,15) as CumulativeAdvertiserCostInPartnerCurrency,
        zeroifnull(sum(CumulativeAdvertiserCostInAdvertiserCurrency))::numeric(37,15) as CumulativeAdvertiserCostInAdvertiserCurrency
    from reports.CumulativePerformanceReport
    inner join provisioning2.DataDomain using (DataDomainId)
    where ReportDateUtc >= '{startDate}' and ReportDateUtc < '{endDate}'
    group by 1,2,3
)
select
    DataDomainName,
    'raw' as Source,
    Date,
    CumulativeImpressionCount,
    CumulativeAudienceImpressionCount,
    CumulativeTTDCostInUSD,
    CumulativeTTDCostInPartnerCurrency,
    CumulativeTTDCostInAdvertiserCurrency,
    CumulativePartnerCostInUSD,
    CumulativePartnerCostInPartnerCurrency,
    CumulativePartnerCostInAdvertiserCurrency,
    CumulativeAdvertiserCostInUSD,
    CumulativeAdvertiserCostInPartnerCurrency,
    CumulativeAdvertiserCostInAdvertiserCurrency
from cumulative
where Date >= '{startDate}' and Date < '{endDate}'
union all
select
    DataDomainName,
    'perf_report' as Source,
    Date,
    sum(CumulativeImpressionCount) as CumulativeImpressionCount,
    sum(CumulativeAudienceImpressionCount) as CumulativeAudienceImpressionCount,
    sum(CumulativeTTDCostInUSD)::numeric(37,15) as CumulativeTTDCostInUSD,
    sum(CumulativeTTDCostInPartnerCurrency)::numeric(37,15) as CumulativeTTDCostInPartnerCurrency,
    sum(CumulativeTTDCostInAdvertiserCurrency)::numeric(37,15) as CumulativeTTDCostInAdvertiserCurrency,
    sum(CumulativePartnerCostInUSD)::numeric(37,15) as CumulativePartnerCostInUSD,
    sum(CumulativePartnerCostInPartnerCurrency)::numeric(37,15) as CumulativePartnerCostInPartnerCurrency,
    sum(CumulativePartnerCostInAdvertiserCurrency)::numeric(37,15) as CumulativePartnerCostInAdvertiserCurrency,
    sum(CumulativeAdvertiserCostInUSD)::numeric(37,15) as CumulativeAdvertiserCostInUSD,
    sum(CumulativeAdvertiserCostInPartnerCurrency)::numeric(37,15) as CumulativeAdvertiserCostInPartnerCurrency,
    sum(CumulativeAdvertiserCostInAdvertiserCurrency)::numeric(37,15) as CumulativeAdvertiserCostInAdvertiserCurrency
from reported
group by 1,2,3 ) labeled