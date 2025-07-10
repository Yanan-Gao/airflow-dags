Src = """
select
    se.ScheduleExecutionId,
    mrrs.MyReportResultSetId,
    mrrs.PhysicalTableGroupId
from rptsched.ReportDefinition rd
inner join myreports.MyReportResultSet mrrs on mrrs.MyReportId = rd.MyReportId
inner join rptsched.Schedule s on rd.ReportDefinitionId = s.ReportDefinitionId
inner join rptsched.ScheduleExecution se on s.ScheduleId = se.ScheduleId
where se.EnqueueDate >= '{StartDateUtcInclusive}'
and se.EnqueueDate < '{EndDateUtcExclusive}'
"""

Dst = """
insert into ttd_dpsr.metrics_ExecutionRSPTG{TableSuffix} (
    execution_id,
    result_set_id,
    physical_table_group
)
values (%s,%s,%s)
"""
