select /*+label(PerfFeeFeatureHourlyConsistencyCheck_Airflow)*/ * from (
with raw as (
    select
        date_trunc('hour', LogEntryTime) as Date,
        count(*) as ImpressionCount,
        sum(CostInUSD) as FeeFeaturesCostInUSD,
        sum(CostInUSD*PartnerCurrencyExchangeRate) as FeeFeaturesCostInPartnerCurrency,
        sum(CostInUSD*AdvertiserCurrencyExchangeRate) as FeeFeaturesCostInAdvertiserCurrency
    from ttd.BidFeedbackFeeFeature
    where LogEntryTime >= '{startDate}' and LogEntryTime < '{endDate}'
    group by 1
)
select
    Date,
    '{dataDomainName}' as DataDomainName,
    'raw' as Source,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount,
    zeroifnull(sum(FeeFeaturesCostInUSD)) as FeeFeaturesCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInPartnerCurrency)) as FeeFeaturesCostInPartnerCurrency,
    zeroifnull(sum(FeeFeaturesCostInAdvertiserCurrency)) as FeeFeaturesCostInAdvertiserCurrency
from raw
group by 1,2,3
union
select
    date_trunc('hour', ReportHourUTC) as Date,
    DataDomainName,
    'perf_report' as Source,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount,
    zeroifnull(sum(FeeFeaturesCostInUSD))::numeric(37,15)  as FeeFeaturesCostInUSD,
    zeroifnull(sum(FeeFeaturesCostInPartnerCurrency))::numeric(37,15)  as FeeFeaturesCostInPartnerCurrency,
    zeroifnull(sum(FeeFeaturesCostInAdvertiserCurrency))::numeric(37,15)  as FeeFeaturesCostInAdvertiserCurrency
from reports.RTBPlatformFeeFeatureReport
inner join provisioning2.DataDomain using (DataDomainId)
where ReportHourUTC >= '{startDate}' and ReportHourUTC < '{endDate}'
group by 1,2,3 ) labeled