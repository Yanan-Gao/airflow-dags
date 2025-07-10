Src = """
select
    s.ScheduleId,
    s.ScheduleStartDate,
    isnull(s.ScheduleEndDate, '3000-01-01 00:00:00'),
    s.CreationDate,
    MaxDurationInHours = case
        when mins.PeriodStartTypeId != 0 or mons.PeriodStartTypeId != 0 then
            case
                when mins.PeriodStartTypeId = 1 or mons.PeriodStartTypeId = 1 then 31 * 24
                when mins.PeriodStartTypeId = 2 or mons.PeriodStartTypeId = 2 then 92 * 24
                when mins.PeriodStartTypeId = 5 or mons.PeriodStartTypeId = 5 then 366 * 24
                when mins.PeriodStartTypeId = 3 or mons.PeriodStartTypeId = 3 then 92 * 24
                when mins.PeriodStartTypeId = 4 or mons.PeriodStartTypeId = 4 then 31 * 24
                else 24
            end
        when mins.ReportStartDateInclusiveOffsetInMinutes is not null then
            (mins.ReportEndDateExclusiveOffsetInMinutes - mins.ReportStartDateInclusiveOffsetInMinutes) / 60
        when mons.ReportStartDateInclusiveOffsetInMonths is not null then
            (mons.ReportEndDateExclusiveOffsetInMonths - mons.ReportStartDateInclusiveOffsetInMonths) * 31 * 24
        else 24
    end,
    IsSingleExecution = case
        when mins.ReportStartDateInclusiveOffsetInMinutes is not null then mins.SingleExecution
        when mons.ReportStartDateInclusiveOffsetInMonths is not null then mons.SingleExecution
        else 1
    end,
    IsVariableDuration = iif(mins.PeriodStartTypeId != 0 or mons.PeriodStartTypeId != 0, 1, 0),
    IsDisabled = iif(ss.IsDisabled = 1, ss.IsDisabled, rd.IsDisabled),
    IsDisabledByUser = iif(ss.UserDisabledReason is null, 0, 1),
    IsCompleted = isnull(ss.IsCompleted, 0),
    s.TenantId,
    s.ScheduleAddedBy,
    s.ScheduleSourceId,
    isnull(s.RequestedUserGroupId, ''),
    isnull(ug.UserGroupName, ''),
    isnull(s.RequestedByUserId, ''),
    isnull(u.UserName, ''),
    s.ScheduleName,
    s.Timezone,
    rd.ReportProviderId,
    ReportType = case
        when rd.MyReportId is not null then 'MyReport'
        when s.ScheduleSourceId = 4 then 'Feed' -- ScheduleSourceId 4 means Exposure Feeds
        else 'Custom'
    end,
    FormatTypeId = isnull(mr.MyReportTypeId, 1) -- Format defaults to CSV
from Provisioning.rptsched.Schedule s
join Provisioning.rptsched.ReportDefinition rd on s.ReportDefinitionId = rd.ReportDefinitionId
left join Provisioning.rptsched.ScheduleFrequencyInMinutes mins on mins.ScheduleFrequencyInMinutesId = s.ScheduleFrequencyInMinutesId
left join Provisioning.rptsched.ScheduleFrequencyInMonths mons on mons.ScheduleFrequencyInMonthsId = s.ScheduleFrequencyInMonthsId
left join Provisioning.myreports.MyReport mr on mr.MyReportId = rd.MyReportId
left join Provisioning.rptsched.ScheduleState ss on ss.ScheduleId = s.ScheduleId
left join Provisioning.dbo.UserGroup ug on ug.UserGroupId = s.RequestedUserGroupId
left join UserProfile.dbo.[User] u on u.UserId = s.RequestedByUserId
where s.CreationDate >= '{StartDateUtcInclusive}'
and s.CreationDate < '{EndDateUtcExclusive}'
"""

Dst = """
insert into ttd_dpsr.metrics_ScheduleAttributes{TableSuffix} (
    ScheduleId,
    DateStart,
    DateEnd,
    CreationDate,
    MaxDurationInHours,
    IsSingleRun,
    IsVariableDuration,
    IsDisabled,
    IsDisabledByUser,
    IsCompleted,
    TenantId,
    ScheduleAddedBy,
    ScheduleSourceId,
    RequestedUserGroupId,
    RequestedUserGroupName,
    RequestedByUserId,
    RequestedByUserName,
    ScheduleName,
    TZName,
    ReportProviderId,
    ReportType,
    FormatTypeId
)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def Transform(res_list):
    transformed = []
    for row in res_list:
        # limit the length of the strings at position 14, 16 and 17 (counting from zero)
        # and make sure that it is a correct utf-8
        ug_name = str(bytes(row[14], 'utf-8')[:127], encoding='utf-8', errors='ignore')
        ur_name = str(bytes(row[16], 'utf-8')[:255], encoding='utf-8', errors='ignore')
        sc_name = str(bytes(row[17], 'utf-8')[:255], encoding='utf-8', errors='ignore')
        transformed.append(row[:14] + tuple([ug_name]) + row[15:16] + tuple([ur_name]) + tuple([sc_name]) + row[18:])
    return transformed
