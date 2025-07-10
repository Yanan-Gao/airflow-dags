Pre = [
    "truncate table ttd_dpsr.metrics_NewlyResolved{TableSuffix}", """
insert into ttd_dpsr.metrics_NewlyResolved{TableSuffix}
    select distinct ScheduleExecutionId
    from ttd_dpsr.metrics_ExecutionStateHistory{TableSuffix}
    where TransitionDate >= '{StartDateUtcInclusive}'
    and TransitionDate < '{EndDateUtcExclusive}'
    and NewStateId = 4
"""
]

Post = """
insert into ttd_dpsr.metrics_ExecutionStatsStage{TableSuffix} (
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
select
    met.ScheduleExecutionId,
    met.ScheduleId,
    met.DateStart,
    esac_uni.FirstCancelled,
    case when esac_uni.FirstStarted is null then '2000-01-01 00:00:00' else esac_uni.FirstStarted end,
    case when met.DateStart < esac_uni.FirstCancelled then datediff(s, met.DateStart, esac_uni.FirstCancelled) else 0 end,
    case when esac_uni.FirstResolved is not null and met.DateStart < esac_uni.FirstResolved then datediff(s, met.DateStart, esac_uni.FirstResolved) else 0 end,
    case when esac_uni.FirstResolved is not null and esac_uni.FirstStarted is not null and met.DateStart < esac_uni.FirstStarted then datediff(s, esac_uni.FirstResolved, esac_uni.FirstStarted) else 0 end,
    case when esac_uni.FirstStarted is not null and met.DateStart < esac_uni.FirstStarted then datediff(s, esac_uni.FirstStarted, esac_uni.FirstCancelled) else 0 end,
    met.DateRangeHours,
    4,
    esac_uni.ReportProviderID,
    met.LastExecutionLineCount,
    met.IsResourceIntensive,
    met.IsBackFill,
    msa.IsSingleRun,
    case when msladc.IsLate is not null then msladc.IsLate else FALSE end,
    case
        when met.IsBackFill = 1 then 0
        when msladc.SLASeconds is not null then datediff(s, met.DateStart, esac_uni.FirstCancelled) - msladc.SLASeconds
        else case
            when msa.IsSingleRun = 1 then datediff(s, met.DateStart, esac_uni.FirstCancelled) - 6*60*60
            else datediff(s, met.DateStart, esac_uni.FirstCancelled) - 6*60*60
        end
    end as PastSLASeconds,
    case
        when msladc.SLASeconds is not null then msladc.SLASeconds
        else case when msa.IsSingleRun = 1 then 2*60*60 else 6*60*60 end
    end as ClassSLASeconds,
    esac_uni.AttemptCount,
    case
        when msa.ScheduleAddedBy = 'ttd_publicapi' then 'api'
        when msa.ScheduleAddedBy = 'ttd_api' then 'ui'
        when msa.ScheduleAddedBy = 'ttd_task' then 'task'
        when msa.ScheduleAddedBy = 'ttd_platformcontrollerservice' then 'platf'
        else 'other'
    end as CreationSource
from -- ExecutionStatesAndCounts
    (
    select
        esac.ScheduleexecutionId,
        min(esac.FirstResolved) as FirstResolved,
        min(esac.FirstStarted) as FirstStarted,
        max(esac.FirstCancelled) as FirstCancelled,
        max(esac.LastReportProviderID) as ReportProviderId,
        max(esac.AttemptCount) as AttemptCount
    from (
        select
            esh.ScheduleExecutionId,
            first_value(case when esh.NewStateId = 7 then esh.TransitionDate end ignore nulls) over (execution rows between unbounded preceding and current row) as FirstResolved,
            first_value(case when esh.NewStateId = 1 then esh.TransitionDate end ignore nulls) over (execution rows between unbounded preceding and current row) as FirstStarted,
            first_value(case when esh.NewStateId = 4 then esh.TransitionDate end ignore nulls) over (execution rows between unbounded preceding and current row) as FirstCancelled,
            last_value(case when esh.NewStateId = 4 then esh.ReportProviderSourceId end ignore nulls) over (execution rows between current row and unbounded following) as LastReportProviderID,
            count(case when esh.NewStateId = 1 then esh.TransitionDate end) over (execution) as AttemptCount
        from ttd_dpsr.metrics_ExecutionStateHistory{TableSuffix} esh
        where exists (select nr.ScheduleExecutionId from ttd_dpsr.metrics_NewlyResolved{TableSuffix} nr where nr.ScheduleExecutionId = esh.ScheduleExecutionId)
        and not exists (select mest.ScheduleExecutionId from ttd_dpsr.metrics_ExecutionStatsStage{TableSuffix} mest where mest.ScheduleExecutionId = esh.ScheduleExecutionId)
        and not exists (select mest.ScheduleExecutionId from ttd_dpsr.metrics_ExecutionStats{TableSuffix} mest where mest.ScheduleExecutionId = esh.ScheduleExecutionId and mest.EndState != 3)
        window execution as (
            partition by esh.ScheduleExecutionId
            order by esh.TransitionDate
        )
    ) esac
    group by esac.ScheduleExecutionId
) as esac_uni
join ttd_dpsr.metrics_ExecutionTraits{TableSuffix} met on met.ScheduleExecutionId = esac_uni.ScheduleExecutionId
join ttd_dpsr.metrics_ScheduleAttributes{TableSuffix} msa on met.ScheduleId = msa.ScheduleId
join ttd_dpsr.metrics_ExecutionDepClasses{TableSuffix} medc on medc.ScheduleExecutionId = met.ScheduleExecutionId
left join ttd_dpsr.metrics_SLADepClasses{TableSuffix} msladc
    on msladc.DepClass = medc.DepClass and msladc.IsSingleRun = msa.IsSingleRun
    and met.DateStart >= msladc.InActSince and met.DateStart < msladc.InActUpTo
"""
