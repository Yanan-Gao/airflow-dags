import re
from typing import TypedDict

Src = """
select
    sesh.ScheduleExecutionId,
    sesh.TransitionDate,
    sefh.ScheduleExecutionFailureTypeId,
    seft.Name,
    substring(sefh.StatusChangeMessage, 1, iif(charindex(char(10), sefh.StatusChangeMessage) > 0, charindex(char(10), sefh.StatusChangeMessage), len(sefh.StatusChangeMessage)))
from Provisioning.rptsched.ScheduleExecutionFailureHistory sefh
join Provisioning.rptsched.ScheduleExecutionStateHistory sesh on sesh.ScheduleExecutionStateHistoryId = sefh.ScheduleExecutionStateHistoryId
join Provisioning.rptsched.ScheduleExecutionFailureType seft on sefh.ScheduleExecutionFailureTypeId = seft.ScheduleExecutionFailureTypeId
where sesh.TransitionDate >= '{StartDateUtcInclusive}'
and sesh.TransitionDate < '{EndDateUtcExclusive}'
"""

Dst = """
insert into ttd_dpsr.metrics_ExecutionErrors{TableSuffix} (
    ScheduleExecutionId,
    ErrorDate,
    ErrorClassId,
    ErrorClass,
    ErrorFragment,
    DetectedFlags,
    Requested,
    Allowed
)
values (%s,%s,%s,%s,%s,%s,%s,%s)
"""


class ErrorMatcher(TypedDict):
    re: re.Pattern
    flag: int
    name: str


patterns = [
    ErrorMatcher({
        're': re.compile('Memory\\(KB\\) Exceeded: Requested = (\\d+), Free = \\d+ \\(Limit = (\\d+)'),
        'flag': 0x00000001,
        'name': 'Memory limit'
    }),
    ErrorMatcher({
        're': re.compile('Threads Exceeded: Requested = (\\d+), Free = \\d+ \\(Limit = (\\d+)'),
        'flag': 0x00000002,
        'name': 'Thread limit'
    }),
    ErrorMatcher({
        're': re.compile('ScheduledReportingTooManyRowsException.+Actual row count: (\\d+), allowed row count: (\\d+)'),
        'flag': 0x00000100,
        'name': 'Row limit'
    }),
    ErrorMatcher({
        're': re.compile('\\[tail aggregate\\]'),
        'flag': 0x00010000,
        'name': 'Tail aggregate failed'
    }),
    ErrorMatcher({
        're': re.compile('ERROR: DDL statement .+ interfered with this statement'),
        'flag': 0x00020000,
        'name': 'Transaction collision'
    })
]


def Transform(res_list):
    transformed = []
    for row in res_list:
        flags = 0
        requested = 0
        allowed = 0
        if row[4] is None:
            transformed.append(row[:4] + tuple(['', flags, requested, allowed]))
            continue
        for pat in patterns:
            res = pat['re'].search(row[4])
            if res is not None:
                flags = pat['flag']
                if res.lastindex is not None:
                    if res.lastindex > 0:
                        requested = res.group(1)
                        if res.lastindex > 1:
                            allowed = res.group(2)
                break  # for now support only single match
        msg = str(bytes(row[4], 'utf-8')[:599], encoding='utf-8', errors='ignore')
        transformed.append(row[:4] + tuple([msg, flags, requested, allowed]))
    return transformed
