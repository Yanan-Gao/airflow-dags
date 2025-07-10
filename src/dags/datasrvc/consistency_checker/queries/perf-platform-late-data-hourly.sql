select /*+label(PerfAndPlatformLateDataHourlyConsistencyCheck_Airflow)*/ * from (
select
    date_trunc('hour', AE.DataReceivedHourUtc) as Date,
    '{dataDomainName}' as DataDomainName,
    'raw' as Source,
   (sum(CASE WHEN ((AER.AttributionMethodId = 2) AND (AER.CampaignReportingColumnId = 1)) THEN (AER.AttributedCount * AER.ConversionMultiplier) ELSE 0::numeric(18,0) END))::int AS Touch1Count,
   (sum(CASE WHEN ((AER.AttributionMethodId = 2) AND (AER.CampaignReportingColumnId = 2)) THEN (AER.AttributedCount * AER.ConversionMultiplier) ELSE 0::numeric(18,0) END))::int AS Touch2Count
from (ttdprotected.AttributedEvent AE
      join ttdprotected.AttributedEventResult AER
      using (ReportHourUtc, AttributedEventLogFileId, AttributedEventIntId1, AttributedEventIntId2, ConversionTrackerLogFileId, ConversionTrackerIntId1, ConversionTrackerIntId2))
where AE.DataReceivedHourUtc >= '{startDate}' and AE.DataReceivedHourUtc < '{endDate}'
and LateDataProviderID > 1
group by 1,2
union all
select
    date_trunc('hour', DataReceivedHourUtc) as Date,
    DataDomainName,
    'perf_report' as Source,
    zeroifnull(sum(Touch1Count)) as Touch1Count,
    zeroifnull(sum(Touch2Count)) as Touch2Count
from reports.vw_PerformanceReport
inner join provisioning2.DataDomain using (DataDomainId)
where DataReceivedHourUtc >= '{startDate}' and DataReceivedHourUtc < '{endDate}'
and LateDataProviderId > 1
group by 1,2
union all
select
    date_trunc('hour', DataReceivedHourUtc) as Date,
    DataDomainName,
    'plat_report' as Source,
    zeroifnull(sum(Touch1Count)) as Touch1Count,
    zeroifnull(sum(Touch2Count)) as Touch2Count
from reports.RTBPlatformReport
inner join provisioning2.DataDomain using (DataDomainId)
where DataReceivedHourUtc >= '{startDate}' and DataReceivedHourUtc < '{endDate}'
and LateDataProviderId > 1
group by 1,2
) labeled