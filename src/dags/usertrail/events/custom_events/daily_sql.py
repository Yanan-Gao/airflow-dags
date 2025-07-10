requested_reports_query = """
    select ScheduleId as Id,
        s.ReportDefinitionid,
        u.Email as UserId,
        ScheduleAddedBy,
        ScheduleSourceID,
        cast(CreationDate AS DATETIME2(3)) as CreationTimestamp, -- Spark cannot read nano precision
        cast(ScheduleStartDate as date) as request_date
    FROM rptsched.Schedule s
      JOIN UserProfile.dbo.[User] u on u.UserId = s.RequestedByUserId
    WHERE ScheduleSourceId in (1, 5) and cast(ScheduleStartDate as date) = '{start_date}'
"""


def get_requested_reports(start_date: str):
    return requested_reports_query.format(start_date=start_date)
