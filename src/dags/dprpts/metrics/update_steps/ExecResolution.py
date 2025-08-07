Pre = 'truncate table ttd_dpsr.metrics_ExecutionResolutionStage{TableSuffix}'

Src = """
select ScheduleExecutionId, ReportProviderSourceId, ResolutionTime
from rptsched.ScheduleExecutionSatisfiedReportProviderSource sedth
where sedth.ResolutionTime >= '{StartDateUtcInclusive}'
and sedth.ResolutionTime < '{EndDateUtcExclusive}'
"""

Dst = """
insert into ttd_dpsr.metrics_ExecutionResolutionStage{TableSuffix} (
    ScheduleExecutionId,
    ReportProviderSourceId,
    ResolutionTime
)
values (%s,%s,%s)
"""

Post = """
insert into ttd_dpsr.metrics_ExecutionResolution{TableSuffix} (
    ScheduleExecutionId,
    ReportProviderSourceId,
    ResolutionTime
)
select
    ScheduleExecutionId,
    ReportProviderSourceId,
    min(ResolutionTime)
from ttd_dpsr.metrics_ExecutionResolutionStage{TableSuffix} mers
where not exists (
                    select 1
                    from ttd_dpsr.metrics_ExecutionResolution{TableSuffix} mer
                    where mer.ScheduleExecutionId = mers.ScheduleExecutionId
                    and mer.ReportProviderSourceId = mers.ReportProviderSourceId
                )
group by
    ScheduleExecutionId,
    ReportProviderSourceId
"""
