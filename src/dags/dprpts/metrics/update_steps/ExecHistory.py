Src = """
select
    sesh.ScheduleExecutionId,
    sesh.TransitionDate as DateStart,
    sesh.NewStateId as StartState,
    sesh.ReportProviderSourceId
from Provisioning.rptsched.ScheduleExecutionStateHistory sesh
where sesh.NewStateId in (0,1,2,3,4,7) -- We are not interested in retry transition steps
and sesh.TransitionDate >= '{StartDateUtcInclusive}'
and sesh.TransitionDate < '{EndDateUtcExclusive}'
"""

Dst = """
insert into ttd_dpsr.metrics_ExecutionStateHistory{TableSuffix} (
    ScheduleExecutionId,
    TransitionDate,
    NewStateId,
    ReportProviderSourceId
)
values (%s,%s,%s,%s)
"""
