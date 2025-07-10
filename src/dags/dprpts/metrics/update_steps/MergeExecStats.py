Post = """
merge into ttd_dpsr.metrics_ExecutionStats{TableSuffix} tgt
using ttd_dpsr.metrics_ExecutionStatsStage{TableSuffix} stg
on tgt.ScheduleExecutionId = stg.ScheduleExecutionId
when matched then update set
    EndState = stg.EndState,
    DateEnd = stg.DateEnd,
    LastExecutionLineCount = stg.LastExecutionLineCount,
    AttemptNumber = stg.AttemptNumber,
    DateStartRunning = stg.DateStartRunning,
    DurationInSeconds = stg.DurationInSeconds,
    WaitForDependencySeconds = stg.WaitForDependencySeconds,
    WaitForExecutionSeconds = stg.WaitForExecutionSeconds,
    ExecutionSeconds = stg.ExecutionSeconds,
    ReportProviderSourceId = stg.ReportProviderSourceId
when not matched then insert (
    ScheduleExecutionId,
    ScheduleId,
    DateStart,
    DateEnd,
    DateStartRunning,
    DurationInSeconds,
    WaitForDependencySeconds,
    WaitForExecutionSeconds,
    ExecutionSeconds,
    DateRangeHours,
    EndState,
    ReportProviderSourceId,
    LastExecutionLineCount,
    IsResourceIntensive,
    IsBackFill,
    IsSingleRun,
    IsLate,
    PastSLASeconds,
    ClassSLASeconds,
    AttemptNumber,
    CreationSource
)
values (
    stg.ScheduleExecutionId,
    stg.ScheduleId,
    stg.DateStart,
    stg.DateEnd,
    stg.DateStartRunning,
    stg.DurationInSeconds,
    stg.WaitForDependencySeconds,
    stg.WaitForExecutionSeconds,
    stg.ExecutionSeconds,
    stg.DateRangeHours,
    stg.EndState,
    stg.ReportProviderSourceId,
    stg.LastExecutionLineCount,
    stg.IsResourceIntensive,
    stg.IsBackFill,
    stg.IsSingleRun,
    stg.IsLate,
    stg.PastSLASeconds,
    stg.ClassSLASeconds,
    stg.AttemptNumber,
    stg.CreationSource
)
"""
