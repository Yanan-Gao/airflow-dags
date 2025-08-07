-- Consistency Checker for BidFeedbackCategoryElement and RTBPlatformCategoryElementReport
select /*+label(RawCategoryHourly_Airflow)*/ * from (
    with raw as (
        -- Aggregating data from ttd.BidFeedbackCategoryElement
        select
            date_trunc('hour', ReportHourUtc) as Date,
            count(*) as ImpressionCount
        from ttd.BidFeedbackCategoryElement
        where ReportHourUtc >= '{startDate}' and ReportHourUTC < '{endDate}'
        group by 1
    ),
    de as (
        -- Aggregating data from the performance report
        select
            date_trunc('hour', ReportHourUTC) as Date,
             DataDomainId,
            sum(ImpressionCount) as ImpressionCount
        from reports.RTBPlatformCategoryElementReport
        where ReportHourUTC >= '{startDate}' and ReportHourUTC < '{endDate}'
        group by 1,2
    )
select
    Date,
    '{dataDomainName}' as DataDomainName,
    'raw' as Source,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount
from raw
group by 1, 2
union all
select
    Date,
    DataDomainName,
    'perf_report' as Source,
    zeroifnull(sum(ImpressionCount)) as ImpressionCount
from de
inner join provisioning2.DataDomain using (DataDomainId)
group by 1,2 ) labeled