Pre = """truncate table ttd_dpsr.metrics_ExecutionDepClassesStage{TableSuffix}"""

Src = """
; with ExecutionBase as (
    select
        se.ScheduleExecutionId,
        rd.ReportDefinitionId,
        isnull( so.ApplyDeltaFiltering, 0 ) as ApplyDeltaFiltering
    from rptsched.ReportDefinition rd
    inner join rptsched.Schedule s on rd.ReportDefinitionId = s.ReportDefinitionId
    inner join rptsched.ScheduleExecution se on s.ScheduleId = se.ScheduleId
    left join myreports.MyReportScheduleOptions so on s.MyReportScheduleOptionsId = so.MyReportScheduleOptionsId
    where se.EnqueueDate >= '{StartDateUtcInclusive}'
    and se.EnqueueDate < '{EndDateUtcExclusive}'
),
DependenciesBase as (
    select
        eb.ScheduleExecutionId,
        rdd.ReportSchedulingEventTypeId,
        eb.ApplyDeltaFiltering
    from rptsched.ReportDefinitionDependency rdd
    inner join ExecutionBase eb on rdd.ReportDefinitionId = eb.ReportDefinitionId
),
ZeroDependencies as (
    select
        eb.ScheduleExecutionId,
        0 as ReportSchedulingEventTypeId,
        0 as ApplyDeltaFiltering
    from ExecutionBase eb
    left join rptsched.ReportDefinitionDependency rdd on rdd.ReportDefinitionId = eb.ReportDefinitionId
    where rdd.ReportDefinitionDependencyId is null
),
DeltaDependencies as (
    select
        db.ScheduleExecutionId,
        case
            when db.ReportSchedulingEventTypeId = 1 then 55
            when db.ReportSchedulingEventTypeId = 3 then 56
        end as ReportSchedulingEventTypeId,
        ApplyDeltaFiltering
    from DependenciesBase db
    where db.ApplyDeltaFiltering = 1
    and db.ReportSchedulingEventTypeId in (1, 3)
),
AllDeps as (
    select
        dbase.ScheduleExecutionId,
        dbase.ReportSchedulingEventTypeId
    from DependenciesBase dbase
    union all
    select
        ddelta.ScheduleExecutionId,
        ddelta.ReportSchedulingEventTypeId
    from DeltaDependencies ddelta
    union all
    select
        zerod.ScheduleExecutionId,
        zerod.ReportSchedulingEventTypeId
    from ZeroDependencies zerod
)
select
    ad.ScheduleExecutionId,
    DependencyClass = concat('-', string_agg(ad.ReportSchedulingEventTypeId, '-') within group ( order by ad.ReportSchedulingEventTypeId), '-')
from AllDeps ad
group by ad.ScheduleExecutionId
"""

Dst = """
insert into ttd_dpsr.metrics_ExecutionDepClassesStage{TableSuffix} (
    ScheduleExecutionId,
    DepClass
)
values (%s,%s)
"""

Post = """
merge into ttd_dpsr.metrics_ExecutionDepClasses{TableSuffix} tgt
using ttd_dpsr.metrics_ExecutionDepClassesStage{TableSuffix} stg
on tgt.ScheduleExecutionId = stg.ScheduleExecutionId
when matched then update set
    DepClass = stg.DepClass
when not matched then insert (
    ScheduleExecutionId,
    DepClass
)
values (
    stg.ScheduleExecutionId,
    stg.DepClass
)
"""
