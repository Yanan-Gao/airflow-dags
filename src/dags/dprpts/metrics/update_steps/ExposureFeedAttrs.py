Pre = """truncate table ttd_dpsr.metrics_ExposureFeedAttributesStage{TableSuffix}"""

Src = """
with RecentlyModifiedFeed as (
    select
        distinct fce.FeedDefinitionId
    from SchedReporting.exposure.FeedChangeEvent fce
    where fce.EventTime >= '{StartDateUtcInclusive}'
        and fce.EventTime < '{EndDateUtcExclusive}'
)
select
    fd.CustomReportScheduleId as ScheduleId,
    isnull(ft.FeedTypeName, 'None') as FeedTypeName,
    isnull(fd2.Description, 'None') as TemplateName,
    fd.FeedExecutionInterval as FeedExecutionInterval
from SchedReporting.exposure.FeedDefinition fd
    join RecentlyModifiedFeed rmf
        on rmf.FeedDefinitionId = fd.FeedDefinitionId
    left join SchedReporting.exposure.FeedType ft
        on ft.FeedTypeid = fd.FeedTypeId
    left join SchedReporting.exposure.FeedDefinition fd2
        on fd.TemplateDefinitionId = fd2.FeedDefinitionId
where fd.CustomReportScheduleId is not null
"""

Dst = """
insert into ttd_dpsr.metrics_ExposureFeedAttributesStage{TableSuffix} (
    ScheduleId,
    FeedTypeName,
    TemplateName,
    FeedExecutionInterval
)
values (%s,%s,%s,%s)
"""

Post = """
merge into ttd_dpsr.metrics_ExposureFeedAttributes{TableSuffix} efa
using ttd_dpsr.metrics_ExposureFeedAttributesStage{TableSuffix} stg
on efa.ScheduleId = stg.ScheduleId
when matched then update set
    FeedTypeName = stg.FeedTypeName,
    TemplateName = stg.TemplateName,
    FeedExecutionInterval = stg.FeedExecutionInterval
when not matched then insert (
    ScheduleId,
    FeedTypeName,
    TemplateName,
    FeedExecutionInterval
)
values (
    stg.ScheduleId,
    stg.FeedTypeName,
    stg.TemplateName,
    stg.FeedExecutionInterval
)
"""
