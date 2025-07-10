Src = """
select distinct
    s.ScheduleId,
    PartnerId = coalesce(mr.OwnerPartnerId, rdpfp.PartnerId, 'tdpartnerid')
from Provisioning.rptsched.Schedule s
join Provisioning.rptsched.ReportDefinition rd on rd.ReportDefinitionId = s.ReportDefinitionId
left join Provisioning.rptsched.ReportDefinitionPermissionForPartner rdpfp on rdpfp.ReportDefinitionId = s.ReportDefinitionId
left join Provisioning.myreports.MyReport mr on mr.MyReportId = rd.MyReportId
where s.CreationDate >= '{StartDateUtcInclusive}'
and s.CreationDate < '{EndDateUtcExclusive}'
"""

Dst = """
insert into ttd_dpsr.metrics_ScheduleConsumers{TableSuffix} (
    ScheduleId,
    CustomerKindId,
    CustomerId
)
values (%s,3,%s)
"""
