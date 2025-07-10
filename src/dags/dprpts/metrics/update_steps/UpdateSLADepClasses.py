Pre = "truncate table ttd_dpsr.metrics_SLADepClasses{TableSuffix}"

Post = """
insert into ttd_dpsr.metrics_SLADepClasses{TableSuffix} (
    DepClass,
    IsSingleRun,
    InActSince,
    InActUpTo,
    ReportType,
    ScheduleAddedBy,
    IsLate,
    SLASeconds
)
select
    uc.DepClass,
    msladc.IsSingleRun,
    msladc.InActSince,
    msladc.InActUpTo,
    null,
    null,
    max(msladc.IsLate),
    max(msladc.SLASeconds)
from ( -- UniqueClasses
    select medc.DepClass as DepClass
    from ttd_dpsr.metrics_ExecutionDepClasses{TableSuffix} medc
    group by medc.DepClass
) uc
join ttd_dpsr.metrics_SLADelayClasses{TableSuffix} msladc on uc.DepClass like '%-' || msladc.ReportSchedulingEventId::varchar || '-%'
group by 1,2,3,4
"""
