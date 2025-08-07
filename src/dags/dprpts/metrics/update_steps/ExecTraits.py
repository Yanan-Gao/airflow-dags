Pre = """truncate table ttd_dpsr.metrics_ExecutionTraitsStage{TableSuffix}"""

Src = """
;with execution_adjusted_traits as (
    select
        se.ScheduleExecutionId,
        s.ScheduleId,
        datediff(hour, se.ReportStartDateInclusive, se.ReportEndDateExclusive) as DateRangeHours,
        se.EnqueueDate,
        s.CreationDate,
        iif( s.Timezone in ( 'UTC', 'Etc/UTC', 'GMT' ), se.ReportEndDateExclusive, Provisioning.timezonedb.fn_ToUTCDatetime( se.ReportEndDateExclusive, s.Timezone ) ) as ReportEndDateExclusiveUTC,
        iif( s.Timezone in ( 'UTC', 'Etc/UTC', 'GMT' ), se.ScheduleExecutionDate, Provisioning.timezonedb.fn_ToUTCDatetime( se.ScheduleExecutionDate, s.Timezone ) ) as ScheduleExecutionDateUTC,
        case
        when mins.ReportStartDateInclusiveOffsetInMinutes is not null
            then mins.SingleExecution
        when mons.ReportStartDateInclusiveOffsetInMonths is not null
            then mons.SingleExecution
        else
            1
        end as IsSingleRun,
        isnull(se.LastExecutionLineCount, 0) as LastExecutionLineCount,
        rd.IsResourceIntensive | isnull(ss.IsResourceIntensive, 0) as IsResourceIntensive,
        iif(se.ScheduleExecutionDate < s.ReprocessingThresholdDateUTC, 1, 0) as IsBackFill
    from Provisioning.rptsched.ScheduleExecution se
    join Provisioning.rptsched.Schedule s on se.ScheduleId = s.ScheduleId
    join Provisioning.rptsched.ReportDefinition rd on s.ReportDefinitionId = rd.ReportDefinitionId
    left join Provisioning.rptsched.ScheduleFrequencyInMinutes mins on mins.ScheduleFrequencyInMinutesId = s.ScheduleFrequencyInMinutesId
    left join Provisioning.rptsched.ScheduleFrequencyInMonths mons on mons.ScheduleFrequencyInMonthsId = s.ScheduleFrequencyInMonthsId
    left join Provisioning.rptsched.ScheduleState ss on ss.ScheduleId = s.ScheduleId
    where
        se.LastStatusChangeDate >= '{StartDateUtcInclusive}'
        and se.LastStatusChangeDate < '{EndDateUtcExclusive}'
)
select
    ScheduleExecutionId,
    ScheduleId,
    DateRangeHours,
    case
        when IsSingleRun = 0 then
            case
                when
                    EnqueueDate > ReportEndDateExclusiveUTC
                    and EnqueueDate > ScheduleExecutionDateUTC
                    then EnqueueDate
                when
                    ReportEndDateExclusiveUTC > ScheduleExecutionDateUTC
                    then ReportEndDateExclusiveUTC
                else ScheduleExecutionDateUTC
            end
        else
            case
                when
                    CreationDate > ReportEndDateExclusiveUTC
                    and CreationDate > ScheduleExecutionDateUTC
                    then CreationDate
                when
                    ReportEndDateExclusiveUTC > ScheduleExecutionDateUTC
                    then ReportEndDateExclusiveUTC
                else ScheduleExecutionDateUTC
            end
    end as StartDate,
    ReportEndDateExclusiveUTC,
    LastExecutionLineCount,
    IsResourceIntensive,
    IsBackFill
from execution_adjusted_traits
"""

Dst = """
insert into ttd_dpsr.metrics_ExecutionTraitsStage{TableSuffix} (
    ScheduleExecutionId,
    ScheduleId,
    DateRangeHours,
    DateStart,
    ReportEndDateExclusiveUTC,
    LastExecutionLineCount,
    IsResourceIntensive,
    IsBackFill
)
values (%s,%s,%s,%s,%s,%s,%s,%s)
"""

Post = """
merge into ttd_dpsr.metrics_ExecutionTraits{TableSuffix} tgt
using ttd_dpsr.metrics_ExecutionTraitsStage{TableSuffix} stg
on tgt.ScheduleExecutionId = stg.ScheduleExecutionId
when matched then update set
    LastExecutionLineCount = stg.LastExecutionLineCount
when not matched then insert (
    ScheduleExecutionId,
    ScheduleId,
    DateRangeHours,
    DateStart,
    ReportEndDateExclusiveUTC,
    LastExecutionLineCount,
    IsResourceIntensive,
    IsBackFill
)
values (
    stg.ScheduleExecutionId,
    stg.ScheduleId,
    stg.DateRangeHours,
    stg.DateStart,
    stg.ReportEndDateExclusiveUTC,
    stg.LastExecutionLineCount,
    stg.IsResourceIntensive,
    stg.IsBackFill
)
"""
