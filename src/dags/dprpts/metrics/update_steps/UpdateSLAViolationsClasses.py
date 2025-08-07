Pre = "truncate table ttd_dpsr.metrics_SLAViolationStatsClasses{TableSuffix}"

Post = """
insert into ttd_dpsr.metrics_SLAViolationStatsClasses{TableSuffix} (
    TheDateHours,
    CustomerId,
    CustomerName,
    CreationSource,
    IsSingleRun,
    IsResourceIntensive,
    IsLate,
    Count,
    CountPastSLA,
    SumWaitDep,
    SumWaitExe,
    SumExecut,
    SumDurat,
    SumWaitDepPastSLA,
    SumWaitExePastSLA,
    SumExecutPastSLA,
    SumDuratPastSLA
)
select
    TheDateHours,
    CustomerId,
    CustomerName,
    CreationSource,
    IsSingleRun,
    IsResourceIntensive,
    IsLate,
    count(distinct(ScheduleExecutionId)),
    sum(case when PastSLASeconds > 0 then 1 else 0 end),
    sum(WaitForDependencySeconds),
    sum(WaitForExecutionSeconds),
    sum(ExecutionSeconds),
    sum(DurationInSeconds),
    sum(case when PastSLASeconds > 0 then WaitForDependencySeconds else 0 end),
    sum(case when PastSLASeconds > 0 then WaitForExecutionSeconds else 0 end),
    sum(case when PastSLASeconds > 0 then ExecutionSeconds else 0 end),
    sum(case when PastSLASeconds > 0 then DurationInSeconds else 0 end)
from (
    select
        date_trunc('HOUR', mest.DateStart) as TheDateHours,
        msc.CustomerId as CustomerId,
        mcn.CustomerName as CustomerName,
        mest.CreationSource,
        mest.IsSingleRun as IsSingleRun,
        mest.IsResourceIntensive as IsResourceIntensive,
        mest.ScheduleExecutionId as ScheduleExecutionId,
        mest.IsLate as IsLate,
        mest.PastSLASeconds as PastSLASeconds,
        mest.WaitForDependencySeconds as WaitForDependencySeconds,
        mest.WaitForExecutionSeconds as WaitForExecutionSeconds,
        mest.ExecutionSeconds as ExecutionSeconds,
        mest.DurationInSeconds as DurationInSeconds
    from ttd_dpsr.metrics_ExecutionStats{TableSuffix} mest
    join ttd_dpsr.metrics_ScheduleConsumers{TableSuffix} msc on msc.ScheduleId = mest.ScheduleId
    join ttd_dpsr.metrics_ConsumerNames{TableSuffix} mcn on mcn.CustomerId = msc.CustomerId and mcn.CustomerKindId = 3
) aliased
group by 1,2,3,4,5,6,7
"""
