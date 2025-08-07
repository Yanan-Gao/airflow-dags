-- daily spend per customer from Agile reporting in Reports
drop table if exists ttd_dpsr.metrics_SpendStatsTest;
create table ttd_dpsr.metrics_SpendStatsTest (
    day               date not null default '2000-01-01',
    partner_id        varchar(32) not null default 'xxxxxx',
    TTDCostInUSD      money(18, 8),
    PartnerCostinUSD  money(18, 8),
    constraint pk_SpendStats_day_partner primary key (day, partner_id)
);
grant select on table ttd_dpsr.metrics_SpendStatsTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_SpendStatsTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_SpendStatsTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_SpendStatsTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_SpendStatsTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ExecutionStateHistoryTest cascade;
create table ttd_dpsr.metrics_ExecutionStateHistoryTest (
    ScheduleExecutionId bigint not null,
    TransitionDate datetime not null,
    NewStateId    tinyint  not null,
    ReportProviderSourceId int,
    constraint pk_executionid_transitiondate primary key (ScheduleExecutionId,TransitionDate,NewStateId)
)
segmented by hash(ScheduleExecutionId) all nodes
partition by TransitionDate::DATE
  group by calendar_hierarchy_day(TransitionDate::DATE, 4 /*active month worth to keep partitioned per day*/, 2 /*years worth of partitioned per month*/)
;
create projection ttd_dpsr.metrics_ExecutionStateHistoryTest_Date (
    TransitionDate,
    NewStateId,
    ScheduleExecutionId
)
as select
    TransitionDate,
    NewStateId,
    ScheduleExecutionId
from ttd_dpsr.metrics_ExecutionStateHistoryTest
order by
    TransitionDate,
    NewStateId,
    ScheduleExecutionId
 segmented by hash(ScheduleExecutionId) all nodes;
grant select on table ttd_dpsr.metrics_ExecutionStateHistoryTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionStateHistoryTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionStateHistoryTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionStateHistoryTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ExecutionTraitsTest;
create table ttd_dpsr.metrics_ExecutionTraitsTest (
    ScheduleExecutionId bigint not null primary key,
    ScheduleId bigint not null,
    DateRangeHours int not null,
    DateStart datetime not null,
    ReportEndDateExclusiveUTC datetime not null,
    LastExecutionLineCount int not null,
    IsResourceIntensive boolean not null,
    IsBackFill boolean not null
)
segmented by hash(ScheduleExecutionId) all nodes
--partition by DateStart::DATE
  --group by calendar_hierarchy_day(DateStart::DATE, 4 /*active month worth to keep partitioned per day*/, 2 /*years worth of partitioned per month*/)
;
grant select on table ttd_dpsr.metrics_ExecutionTraitsTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionTraitsTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionTraitsTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ExecutionTraitsTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionTraitsTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionTraitsTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ExecutionTraitsStageTest;
create table if not exists ttd_dpsr.metrics_ExecutionTraitsStageTest (
    ScheduleExecutionId bigint not null primary key,
    ScheduleId bigint not null,
    DateRangeHours int not null,
    DateStart datetime not null,
    ReportEndDateExclusiveUTC datetime not null,
    LastExecutionLineCount int not null,
    IsResourceIntensive boolean not null,
    IsBackFill boolean not null
) segmented by hash(ScheduleExecutionId) all nodes;
grant truncate on table ttd_dpsr.metrics_ExecutionTraitsStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionTraitsStageTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionTraitsStageTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ExecutionResolutionTest cascade;
create table ttd_dpsr.metrics_ExecutionResolutionTest (
    ScheduleExecutionId bigint not null,
    ReportProviderSourceId int not null,
    ResolutionTime datetime not null,
    constraint pk_dpsr_metrics_ExecutionResolutionTest primary key (ScheduleExecutionId, ReportProviderSourceId)
) segmented by hash(ScheduleExecutionId) all nodes;
grant select on table ttd_dpsr.metrics_ExecutionResolutionTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionResolutionTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionResolutionTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ExecutionResolutionTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionResolutionTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionResolutionTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ExecutionResolutionStageTest;
create table ttd_dpsr.metrics_ExecutionResolutionStageTest (
    ScheduleExecutionId bigint not null primary key,
    ReportProviderSourceId int not null,
    ResolutionTime datetime not null
) segmented by hash(ScheduleExecutionId) all nodes;
grant truncate on table ttd_dpsr.metrics_ExecutionResolutionStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionResolutionStageTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionResolutionStageTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ExecutionStatsStageTest;
create table ttd_dpsr.metrics_ExecutionStatsStageTest (
    ScheduleExecutionId bigint not null primary key,
    ScheduleId bigint not null,
    DateStart datetime not null,
    DateEnd datetime not null,
    DateStartRunning datetime not null,
    DurationInSeconds int not null default 0,
    WaitForDependencySeconds int not null default 0,
    WaitForExecutionSeconds int not null default 0,
    ExecutionSeconds int not null default 0,
    DateRangeHours int not null,
    EndState tinyint not null,
    ReportProviderSourceId int,
    LastExecutionLineCount int not null,
    IsResourceIntensive boolean not null,
    IsBackFill boolean not null,
    IsSingleRun boolean not null,
    IsLate boolean not null,
    PastSLASeconds int not null default 0,
    ClassSLASeconds int not null default 0,
    AttemptNumber smallint not null default 0,
    CreationSource varchar(10) not null default 'other'
) segmented by hash(ScheduleExecutionId) all nodes;
grant select on table ttd_dpsr.metrics_ExecutionStatsStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionStatsStageTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ExecutionStatsStageTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ExecutionStatsTest cascade;
create table ttd_dpsr.metrics_ExecutionStatsTest (
    ScheduleExecutionId bigint not null primary key,
    ScheduleId bigint not null,
    DateStart datetime not null,
    DateEnd datetime not null,
    DateStartRunning datetime not null,
    DurationInSeconds int not null default 0,
    WaitForDependencySeconds int not null default 0,
    WaitForExecutionSeconds int not null default 0,
    ExecutionSeconds int not null default 0,
    DateRangeHours int not null,
    EndState tinyint not null,
    ReportProviderSourceId int,
    LastExecutionLineCount int not null,
    IsResourceIntensive boolean not null,
    IsBackFill boolean not null,
    IsSingleRun boolean not null,
    IsLate boolean not null,
    PastSLASeconds int not null default 0,
    ClassSLASeconds int not null default 0,
    AttemptNumber smallint not null default 0,
    CreationSource varchar(10) not null default 'other'
)
segmented by hash(ScheduleExecutionId) all nodes
partition by DateStart::DATE
  group by calendar_hierarchy_day(DateStart::DATE, 4 /*active month worth to keep partitioned per day*/, 2 /*years worth of partitioned per month*/)
;
create projection ttd_dpsr.metrics_ExecutionStatsTest_Schedule (
    ScheduleId,
    ScheduleExecutionId,
    DateStart,
    DateEnd,
    DurationInSeconds
)
as select
    ScheduleId,
    ScheduleExecutionId,
    DateStart,
    DateEnd,
    DurationInSeconds
from ttd_dpsr.metrics_ExecutionStatsTest
order by
    ScheduleId,
    ScheduleExecutionId,
    DateStart,
    DateEnd,
    DurationInSeconds
 segmented by hash(ScheduleExecutionId) all nodes;

create projection ttd_dpsr.metrics_ExecutionStatsTest_PastSLA (
    ScheduleExecutionId,
    DateStart,
    PastSLASeconds,
    IsSingleRun,
    IsBackFill,
    ScheduleSource
)
as select
    ScheduleExecutionId,
    DateStart,
    PastSLASeconds,
    IsSingleRun,
    IsBackFill,
    CreationSource
from ttd_dpsr.metrics_ExecutionStatsTest
order by
    DateStart,
    PastSLASeconds,
    IsSingleRun,
    IsBackFill,
    CreationSource
 segmented by hash(ScheduleExecutionId) all nodes;
grant select on table ttd_dpsr.metrics_ExecutionStatsTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionStatsTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionStatsTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ExecutionStatsTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionStatsTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionStatsTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_NewlyResolvedTest cascade;
create table ttd_dpsr.metrics_NewlyResolvedTest (
    ScheduleExecutionId bigint not null primary key
) segmented by hash(ScheduleExecutionId) all nodes;
grant select on table ttd_dpsr.metrics_NewlyResolvedTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_NewlyResolvedTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_NewlyResolvedTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ScheduleAttributesTest cascade;
create table ttd_dpsr.metrics_ScheduleAttributesTest (
    ScheduleId bigint not null primary key,
    DateStart datetime not null,
    DateEnd datetime not null,
    CreationDate datetime not null,
    MaxDurationInHours int not null, -- For variable duration schedules the maximum number of hours in date range of execution
    IsSingleRun boolean,
    IsVariableDuration boolean not null default 0,
    IsDisabled boolean,
    IsDisabledByUser boolean,
    IsCompleted boolean,
    TenantId int not null,
    ScheduleAddedBy varchar(128) not null,
    ScheduleSourceId int not null,
    RequestedUserGroupId varchar(32) not null,
    RequestedUserGroupName varchar(128) not null,
    RequestedByUserId varchar(32),
    RequestedByUserName varchar(256) not null,
    ScheduleName varchar(256) not null,
    TZName varchar(35),
    ReportProviderId int,
    ReportType varchar(16),
    FormatTypeId int
) segmented by hash(ScheduleId) all nodes;

create projection ttd_dpsr.metrics_ScheduleAttributesTest_StartDate (
    DateStart,
    ScheduleId,
    DateEnd,
    MaxDurationInHours,
    IsSingleRun,
    IsVariableDuration,
    IsDisabled,
    IsDisabledByUser,
    IsCompleted,
    TenantId,
    ScheduleAddedBy,
    ScheduleSourceId,
    TZName,
    ReportProviderId,
    ReportType,
    FormatTypeId
)
as select
    DateStart,
    ScheduleId,
    DateEnd,
    MaxDurationInHours,
    IsSingleRun,
    IsVariableDuration,
    IsDisabled,
    IsDisabledByUser,
    IsCompleted,
    TenantId,
    ScheduleAddedBy,
    ScheduleSourceId,
    TZName,
    ReportProviderId,
    ReportType,
    FormatTypeId
from ttd_dpsr.metrics_ScheduleAttributesTest
order by
    DateStart,
    ScheduleId,
    DateEnd,
    MaxDurationInHours,
    IsSingleRun,
    IsVariableDuration,
    IsDisabled,
    IsDisabledByUser,
    IsCompleted,
    TenantId,
    ScheduleAddedBy,
    ScheduleSourceId,
    TZName,
    ReportProviderId,
    ReportType,
    FormatTypeId
 segmented by hash(ScheduleId) all nodes;
grant select on table ttd_dpsr.metrics_ScheduleAttributesTest to "ttd_monitor";
grant insert on table ttd_dpsr.metrics_ScheduleAttributesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ScheduleAttributesTest to "ttd_taskservice";
grant delete on table ttd_dpsr.metrics_ScheduleAttributesTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ScheduleAttributesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ScheduleAttributesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ScheduleAttributesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ScheduleConsumersTest cascade;
create table ttd_dpsr.metrics_ScheduleConsumersTest (
    ScheduleId bigint not null,
    CustomerKindId tinyint,
    CustomerId varchar(32) not null,

    constraint pk_dpsr_metrics_ScheduleConsumersTest primary key (ScheduleId, CustomerKindId, CustomerId)
);
create projection ttd_dpsr.metrics_ScheduleConsumersTest_Customer (
    CustomerId,
    CustomerKindId,
    ScheduleId
)
as select
    CustomerId,
    CustomerKindId,
    ScheduleId
from ttd_dpsr.metrics_ScheduleConsumersTest
order by
    CustomerId,
    CustomerKindId,
    ScheduleId;
grant select on table ttd_dpsr.metrics_ScheduleConsumersTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ScheduleConsumersTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ScheduleConsumersTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ScheduleConsumersTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ScheduleConsumersTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ConsumerNamesTest cascade;
create table ttd_dpsr.metrics_ConsumerNamesTest (
    CustomerId varchar(32) not null,
    CustomerKindId tinyint,
    CustomerName varchar(128),

    constraint pk_dpsr_metrics_ConsumerNamesTest primary key (CustomerId, CustomerKindId)
);
grant select on table ttd_dpsr.metrics_ConsumerNamesTest to "ttd_monitor";
grant truncate on table ttd_dpsr.metrics_ConsumerNamesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ConsumerNamesTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ConsumerNamesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ConsumerNamesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ConsumerNamesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ConsumerNamesStageTest cascade;
create table ttd_dpsr.metrics_ConsumerNamesStageTest (
    CustomerId varchar(32) not null,
    CustomerKindId tinyint,
    CustomerName varchar(128)
);
grant truncate on table ttd_dpsr.metrics_ConsumerNamesStageTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ConsumerNamesStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ConsumerNamesStageTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ExecutionDepClassesTest;
create table ttd_dpsr.metrics_ExecutionDepClassesTest (
    ScheduleExecutionId bigint not null primary key,
    DepClass varchar(32) not null
);
grant select on table ttd_dpsr.metrics_ExecutionDepClassesTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionDepClassesTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionDepClassesTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ExecutionDepClassesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionDepClassesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionDepClassesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ExecutionDepClassesStageTest;
create table ttd_dpsr.metrics_ExecutionDepClassesStageTest (
    ScheduleExecutionId bigint not null primary key,
    DepClass varchar(32) not null
);
grant select on table ttd_dpsr.metrics_ExecutionDepClassesStageTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionDepClassesStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionDepClassesStageTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ExecutionDepClassesStageTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_SLADelayClassesTest;
create table ttd_dpsr.metrics_SLADelayClassesTest (
    ReportSchedulingEventId int not null,
    ReportSchedulingEventName varchar(256) not null,
    IsSingleRun boolean not null,
    ReportType varchar(16),
    ScheduleAddedBy varchar(128),
    IsLate boolean not null,
    SLASeconds int not null,
    InActSince datetime not null,
    InActUpTo datetime not null
);
grant select on table ttd_dpsr.metrics_SLADelayClassesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_SLADelayClassesTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_SLADelayClassesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_SLADelayClassesTest to "dpsr_write";

insert into ttd_dpsr.metrics_SLADelayClassesTest (
    ReportSchedulingEventId,
    ReportSchedulingEventName,
    IsSingleRun,
    ReportType,
    ScheduleAddedBy,
    IsLate,
    SLASeconds,
    InActSince,
    InActUpTo
) values
(0,'ZeroDependencySchedules',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(0,'ZeroDependencySchedules',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(0,'ZeroDependencySchedules',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(1,'HourlyVerticaPerformanceReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(1,'HourlyVerticaPerformanceReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(1,'HourlyVerticaPerformanceReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(3,'HourlyVerticaRTBPlatformReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(3,'HourlyVerticaRTBPlatformReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(3,'HourlyVerticaRTBPlatformReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(8,'HourlyVerticaDataElementReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(8,'HourlyVerticaDataElementReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(8,'HourlyVerticaDataElementReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(9,'HourlyVerticaFeeFeaturesReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(9,'HourlyVerticaFeeFeaturesReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(9,'HourlyVerticaFeeFeaturesReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(10,'HourlyVerticaPotentialSpendReportMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(10,'HourlyVerticaPotentialSpendReportMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(10,'HourlyVerticaPotentialSpendReportMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(11,'HourlyVerticaAdServerReportingMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(11,'HourlyVerticaAdServerReportingMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(11,'HourlyVerticaAdServerReportingMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(13,'DailyVerticaCumulativePerformanceReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(13,'DailyVerticaCumulativePerformanceReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(13,'DailyVerticaCumulativePerformanceReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(55,'HourlyVerticaLateDataPerformanceReportMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(55,'HourlyVerticaLateDataPerformanceReportMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(55,'HourlyVerticaLateDataPerformanceReportMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(56,'HourlyVerticaLateDataRTBPlatformReportMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(56,'HourlyVerticaLateDataRTBPlatformReportMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(56,'HourlyVerticaLateDataRTBPlatformReportMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(57,'HourlyVerticaRTBPlatformReportMergeEventDataComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(57,'HourlyVerticaRTBPlatformReportMergeEventDataComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(57,'HourlyVerticaRTBPlatformReportMergeEventDataComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(58,'HourlyVerticaRTBPlatformReportMergeAttributionDataComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(58,'HourlyVerticaRTBPlatformReportMergeAttributionDataComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(58,'HourlyVerticaRTBPlatformReportMergeAttributionDataComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(103,'HourlyVerticaCTVPlatformReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(103,'HourlyVerticaCTVPlatformReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(103,'HourlyVerticaCTVPlatformReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(104,'HourlyVerticaPGDeliveryReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(104,'HourlyVerticaPGDeliveryReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(104,'HourlyVerticaPGDeliveryReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(105,'VerticaMergeIntoFreqReportComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(105,'VerticaMergeIntoFreqReportComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(105,'VerticaMergeIntoFreqReportComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(114,'DailyWalmartSalesData14DayAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(114,'DailyWalmartSalesData14DayAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(114,'DailyWalmartSalesData14DayAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(115,'DailyWalmartSalesData30DayAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(115,'DailyWalmartSalesData30DayAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(115,'DailyWalmartSalesData30DayAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(116,'HourlyInventoryReportingLoadComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(116,'HourlyInventoryReportingLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(116,'HourlyInventoryReportingLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(117,'DailyWalmartNewBuyerReport14DayAdGroupAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(117,'DailyWalmartNewBuyerReport14DayAdGroupAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(117,'DailyWalmartNewBuyerReport14DayAdGroupAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(118,'DailyWalmartNewBuyerReport30DayAdGroupAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(118,'DailyWalmartNewBuyerReport30DayAdGroupAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(118,'DailyWalmartNewBuyerReport30DayAdGroupAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(119,'DailyWalmartNewBuyerReport14DayCampaignAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(119,'DailyWalmartNewBuyerReport14DayCampaignAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(119,'DailyWalmartNewBuyerReport14DayCampaignAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(120,'DailyWalmartNewBuyerReport30DayCampaignAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(120,'DailyWalmartNewBuyerReport30DayCampaignAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(120,'DailyWalmartNewBuyerReport30DayCampaignAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(151,'HourlySnowflakeBidRequestLoadComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(151,'HourlySnowflakeBidRequestLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(151,'HourlySnowflakeBidRequestLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(152,'HourlySnowflakeBidFeedbackLoadComplete',True,null,null,False,3*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(156,'HourlySnowflakeBidFeedbackLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(152,'HourlySnowflakeBidFeedbackLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(153,'HourlySnowflakeClickTrackerLoadComplete',True,null,null,False,3*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(153,'HourlySnowflakeClickTrackerLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(153,'HourlySnowflakeClickTrackerLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(154,'HourlySnowflakeConversionTrackerLoadComplete',True,null,null,False,3*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(154,'HourlySnowflakeConversionTrackerLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(154,'HourlySnowflakeConversionTrackerLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(155,'HourlySnowflakeVideoEventLoadComplete',True,null,null,False,3*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(155,'HourlySnowflakeVideoEventLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(155,'HourlySnowflakeVideoEventLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(157,'DailyImportISpotReachReport',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(157,'DailyImportISpotReachReport',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(157,'DailyImportISpotReachReport',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(158,'DealMetaDataVerticaMerge',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(158,'DealMetaDataVerticaMerge',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(158,'DealMetaDataVerticaMerge',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(159,'VerticaAdGroupFrequencySavingHourlyComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(159,'VerticaAdGroupFrequencySavingHourlyComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(159,'VerticaAdGroupFrequencySavingHourlyComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(161,'DailyVerticaClinchReportMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(161,'DailyVerticaClinchReportMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(161,'DailyVerticaClinchReportMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(163,'DailyIAv2DataElementReport',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(163,'DailyIAv2DataElementReport',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(163,'DailyIAv2DataElementReport',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(164,'HourlyDemographicInsightsImportComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(164,'HourlyDemographicInsightsImportComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(164,'HourlyDemographicInsightsImportComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(165,'HourlyImportOpenPathPublisherReportLoadComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(165,'HourlyImportOpenPathPublisherReportLoadComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(165,'HourlyImportOpenPathPublisherReportLoadComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(166,'DailyVerticaReportsSchemaDataElementReportMergeComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(166,'DailyVerticaReportsSchemaDataElementReportMergeComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(166,'DailyVerticaReportsSchemaDataElementReportMergeComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(167,'DailyImportNielsenOneReportComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(167,'DailyImportNielsenOneReportComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(167,'DailyImportNielsenOneReportComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(168,'DailyImportIqviaAudienceQualityComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(168,'DailyImportIqviaAudienceQualityComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(168,'DailyImportIqviaAudienceQualityComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(169,'HourlyDoohArpBaselineImportComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(169,'HourlyDoohArpBaselineImportComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(169,'HourlyDoohArpBaselineImportComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(170,'HourlyDoohArpReportImportComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(170,'HourlyDoohArpReportImportComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(170,'HourlyDoohArpReportImportComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(173,'DailyImportCrossixAudienceQualityComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(173,'DailyImportCrossixAudienceQualityComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(173,'DailyImportCrossixAudienceQualityComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(176,'HourlyBrandPositioningSavingsReportComplete',True,null,null,False,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(176,'HourlyBrandPositioningSavingsReportComplete',True,null,null,False,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(176,'HourlyBrandPositioningSavingsReportComplete',False,null,null,False,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(177,'DailyImportPoliticalSentimentScoreComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(177,'DailyImportPoliticalSentimentScoreComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(177,'DailyImportPoliticalSentimentScoreComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(178,'DailyVerticaInnovidReportMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(178,'DailyVerticaInnovidReportMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(178,'DailyVerticaInnovidReportMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(179,'DailyVerticaFlashTalkingReportMergeComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(179,'DailyVerticaFlashTalkingReportMergeComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(179,'DailyVerticaFlashTalkingReportMergeComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00'),
(200,'DailyTargetSalesData30DayAttributionLoadComplete',True,null,null,True,2*60*60,'2000-01-01 00:00:00', '2024-09-16 00:00:00'),
(200,'DailyTargetSalesData30DayAttributionLoadComplete',True,null,null,True,6*60*60,'2024-09-16 00:00:00', '3000-01-01 00:00:00'),
(200,'DailyTargetSalesData30DayAttributionLoadComplete',False,null,null,True,6*60*60,'2000-01-01 00:00:00','3000-01-01 00:00:00')
;

drop table if exists ttd_dpsr.metrics_SLADepClassesTest;
create table ttd_dpsr.metrics_SLADepClassesTest (
    DepClass varchar(32) not null,
    IsSingleRun boolean not null,
    InActSince datetime not null,
    InActUpTo datetime not null,
    ReportType varchar(16),
    ScheduleAddedBy varchar(128),
    IsLate boolean not null,
    SLASeconds int not null
);
grant select on table ttd_dpsr.metrics_SLADepClassesTest to "ttd_monitor";
grant insert on table ttd_dpsr.metrics_SLADepClassesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_SLADepClassesTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_SLADepClassesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_SLADepClassesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_SLADepClassesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_SLAViolationStatsClassesTest cascade;
create table ttd_dpsr.metrics_SLAViolationStatsClassesTest (
    TheDateHours datetime not null,
    CustomerId varchar(32) not null,
    CustomerName varchar(128),
    CreationSource varchar(32) not null,
    IsSingleRun boolean not null,
    IsResourceIntensive boolean not null,
    IsLate boolean not null,
    Count int not null default(0),
    CountPastSLA int not null default(0),
    SumWaitDep int not null default(0),
    SumWaitExe int not null default(0),
    SumExecut int not null default(0),
    SumDurat int not null default(0),
    SumWaitDepPastSLA int not null default(0),
    SumWaitExePastSLA int not null default(0),
    SumExecutPastSLA int not null default(0),
    SumDuratPastSLA int not null default(0),

    constraint pk_metrics_SLAViolationStats primary key (TheDateHours, CustomerId, CreationSource, IsSingleRun, IsResourceIntensive, IsLate)
)
order by TheDateHours, CustomerId
segmented by hash(CustomerId) all nodes
;
grant select on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_SLAViolationStatsClassesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ExecutionRSPTGTest;
create table ttd_dpsr.metrics_ExecutionRSPTGTest (
    execution_id           int not null encoding deltaval,
    result_set_id          int not null default 0,
    physical_table_group   int,
    constraint pk_metrics_session_parsed_execution_resultset_start primary key (execution_id, result_set_id)
);
grant select on table ttd_dpsr.metrics_ExecutionRSPTGTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionRSPTGTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionRSPTGTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionRSPTGTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionRSPTGTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_PTGNamesTest;
create table ttd_dpsr.metrics_PTGNamesTest (
    PhysicalTableGroupId   int,
    GroupName              varchar(256),
    constraint pk_metrics_ptg_id primary key (PhysicalTableGroupId)
);
grant select on table ttd_dpsr.metrics_PTGNamesTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_PTGNamesTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_PTGNamesTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_PTGNamesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_PTGNamesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_PTGNamesStageTest;
create table ttd_dpsr.metrics_PTGNamesStageTest (
    PhysicalTableGroupId   int,
    GroupName              varchar(256),
    constraint pk_metrics_ptg_id primary key (PhysicalTableGroupId)
);
grant truncate on table ttd_dpsr.metrics_PTGNamesStageTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_PTGNamesStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_PTGNamesStageTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ExposureFeedAttributesTest;
create table ttd_dpsr.metrics_ExposureFeedAttributesTest (
    ScheduleId bigint not null primary key,
    FeedTypeName varchar(64) not null,
    TemplateName varchar(256) not null,
    FeedExecutionInterval int not null
) segmented by hash(ScheduleId) all nodes;
drop table if exists ttd_dpsr.metrics_ExposureFeedAttributesStageTest;
create table ttd_dpsr.metrics_ExposureFeedAttributesStageTest (
    ScheduleId bigint not null primary key,
    FeedTypeName varchar(64) not null,
    TemplateName varchar(256) not null,
    FeedExecutionInterval int not null
) segmented by hash(ScheduleId) all nodes;
grant select on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "ttd_taskservice";
grant delete on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ExposureFeedAttributesStageTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExposureFeedAttributesStageTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExposureFeedAttributesStageTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExposureFeedAttributesTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ParsedQueryStatsTest cascade;
create table ttd_dpsr.metrics_ParsedQueryStatsTest (
    ScheduleExecutionId integer not null,
    ResultSetId integer not null,
    TimeStart timestamp not null,
    TimeEnd timestamp not null,
    ScheduleId integer not null,
    PTGId integer not null,
    SessionId varchar(128) not null,
    TransactionId integer not null,
    StatementId integer not null,
    Cluster varchar(128) not null,
    Subcluster varchar(15) not null,
    InitiatorNode varchar(32) not null,
    ResultRowCount integer not null,
    IsSuccess boolean not null,
    IsRetry boolean not null,
    ResourcePool varchar(128),
    PeakMemoryKB integer not null,
    InputRowsProcessed integer not null,
    BytesSpilled integer not null,
    DataBytesRead integer not null,
    NetworkBytesSent integer not null,
    IsResourceIntensive boolean not null,
    WaitTimeMS integer not null,
    DurationMS integer not null,
    DurationClass integer not null,

    constraint pk_exec_rs_start primary key (ScheduleExecutionId,ResultSetId,TimeStart)
)
segmented by hash(ScheduleExecutionId) all nodes
partition by TimeStart::DATE
  group by calendar_hierarchy_day(TimeStart::DATE, 2 /*active month worth to keep partitioned per day*/, 2 /*years worth of partitioned per month*/)
include schema privileges
;
grant select on table ttd_dpsr.metrics_ParsedQueryStatsTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ParsedQueryStatsTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ParsedQueryStatsTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ParsedQueryStatsTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ParsedQueryStatsTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ParsedQueryStatsTest to "dpsr_write";

drop table if exists ttd_dpsr.metrics_ParsedQueryStatsReadyTest cascade;
create table ttd_dpsr.metrics_ParsedQueryStatsReadyTest (
    ScheduleExecutionId integer not null,
    ResultSetId integer not null,
    TimeStart timestamp not null,
    TimeEnd timestamp not null,
    ScheduleId integer not null,
    PTGId integer not null,
    SessionId varchar(128) not null,
    TransactionId integer not null,
    StatementId integer not null,
    Cluster varchar(128) not null,
    Subcluster varchar(15) not null,
    InitiatorNode varchar(32) not null,
    ResultRowCount integer not null,
    IsSuccess boolean not null,
    IsRetry boolean not null,
    ResourcePool varchar(128),
    PeakMemoryKB integer not null,
    InputRowsProcessed integer not null,
    BytesSpilled integer not null,
    DataBytesRead integer not null,
    NetworkBytesSent integer not null,
    IsResourceIntensive boolean not null,
    WaitTimeMS integer not null,
    DurationMS integer not null,
    DurationClass integer not null,

    constraint pk_exec_rs_start primary key (ScheduleExecutionId,ResultSetId,TimeStart)
)
segmented by hash(ScheduleExecutionId) all nodes
include schema privileges
;
grant select on table ttd_dpsr.metrics_ParsedQueryStatsReadyTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ParsedQueryStatsReadyTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ParsedQueryStatsReadyTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ParsedQueryStatsReadyTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ParsedQueryStatsSummsTest cascade;
create table ttd_dpsr.metrics_ParsedQueryStatsSummsTest (
    SessionId varchar(128) not null,
    TransactionId integer not null,
    StatementId integer not null,
    Cluster varchar(128) not null,
    TheTime timestamp not null,
    ResourcePool varchar(128),
    PeakMemoryKB integer not null,
    InputRowsProcessed integer not null,
    BytesSpilled integer not null,
    DataBytesRead integer not null,
    NetworkBytesSent integer not null,
    RecordEpoch integer not null,

    constraint pk_sess_tran_stm_cluster primary key (SessionId, TransactionId, StatementId, Cluster)
)
segmented by hash(SessionId, TransactionId, StatementId, Cluster) all nodes
partition by date_trunc('hour', TheTime)
  activepartitioncount 8 /* we definitely don't care about reporting queries that took longer than 8 hours */
include schema privileges
;
grant select on table ttd_dpsr.metrics_ParsedQueryStatsSummsTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ParsedQueryStatsSummsTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ParsedQueryStatsSummsTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ParsedQueryStatsSummsTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ParsedQueryStatsEndedTest cascade;
create table ttd_dpsr.metrics_ParsedQueryStatsEndedTest (
    SessionId varchar(128) not null,
    TransactionId integer not null,
    StatementId integer not null,
    Cluster varchar(128) not null,
    ScheduleExecutionId integer not null,
    ResultSetId integer not null,
    TimeStart timestamp not null,
    TimeEnd timestamp not null,
    ScheduleId integer not null,
    PTGId integer not null,
    Subcluster varchar(15) not null,
    InitiatorNode varchar(32) not null,
    ResultRowCount integer not null,
    IsSuccess boolean not null,
    IsRetry boolean not null,
    IsResourceIntensive boolean not null,
    DurationMS integer not null,
    RecordEpoch integer not null
)
segmented by hash(SessionId, TransactionId, StatementId, Cluster) all nodes
partition by date_trunc('hour', TimeStart)
  activepartitioncount 8 /* we definitely don't care about reporting queries that took longer than 8 hours */
include schema privileges
;
grant select on table ttd_dpsr.metrics_ParsedQueryStatsEndedTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ParsedQueryStatsEndedTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ParsedQueryStatsEndedTest to "ttd_taskservice";
grant delete on table ttd_dpsr.metrics_ParsedQueryStatsEndedTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ParsedQueryStatsEndedTest to "ttd_taskservice";


drop table if exists ttd_dpsr.metrics_ParsedQueryStatsWaitTest cascade;
create table ttd_dpsr.metrics_ParsedQueryStatsWaitTest (
    SessionId varchar(128) not null,
    TransactionId integer not null,
    StatementId integer not null,
    Cluster varchar(128) not null,
    TimeWait timestamp not null,
    WaitTimeMS integer not null,
    RecordEpoch integer not null
)
segmented by hash(SessionId, TransactionId, StatementId, Cluster) all nodes
partition by date_trunc('hour', TimeWait)
  activepartitioncount 8 /* we definitely don't care about reporting queries that took longer than 8 hours */
include schema privileges
;
grant select on table ttd_dpsr.metrics_ParsedQueryStatsWaitTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ParsedQueryStatsWaitTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ParsedQueryStatsWaitTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ParsedQueryStatsWaitTest to "ttd_taskservice";

drop table if exists ttd_dpsr.metrics_ExecutionErrorsTest cascade;
create table ttd_dpsr.metrics_ExecutionErrorsTest (
    ScheduleExecutionId integer not null,
    ErrorDate timestamp not null,
    ErrorClassId int not null,
    ErrorClass varchar(128) not null,
    ErrorFragment varchar(600) not null,
    DetectedFlags integer not null,
    Requested integer not null,
    Allowed integer not null
)
segmented by hash(ScheduleExecutionId) all nodes
include schema privileges
;
grant select on table ttd_dpsr.metrics_ExecutionErrorsTest to "ttd_taskservice";
grant insert on table ttd_dpsr.metrics_ExecutionErrorsTest to "ttd_taskservice";
grant update on table ttd_dpsr.metrics_ExecutionErrorsTest to "ttd_taskservice";
grant truncate on table ttd_dpsr.metrics_ExecutionErrorsTest to "ttd_taskservice";
grant select on table ttd_dpsr.metrics_ExecutionErrorsTest to "ttd_monitor";
grant select on table ttd_dpsr.metrics_ExecutionErrorsTest to "dpsr_read";
grant select on table ttd_dpsr.metrics_ExecutionErrorsTest to "dpsr_write";

truncate table ttd_dpsr.metrics_SpendStatsTest;
truncate table ttd_dpsr.metrics_ScheduleConsumersTest;
truncate table ttd_dpsr.metrics_ExecutionStateHistoryTest;
truncate table ttd_dpsr.metrics_ExecutionTraitsTest;
truncate table ttd_dpsr.metrics_ExecutionRSPTGTest;
truncate table ttd_dpsr.metrics_ExecutionResolutionTest;
truncate table ttd_dpsr.metrics_ExecutionTraitsTest;
truncate table ttd_dpsr.metrics_ScheduleAttributesTest;
truncate table ttd_dpsr.metrics_ExecutionDepClassesTest;
truncate table ttd_dpsr.metrics_ExecutionStatsTest;
truncate table ttd_dpsr.metrics_SLAViolationStatsClassesTest;
truncate table ttd_dpsr.metrics_ExposureFeedAttributesTest;

truncate table ttd_dpsr.metrics_ParsedQueryStatsTest;
truncate table ttd_dpsr.metrics_ParsedQueryStatsReadyTest;
truncate table ttd_dpsr.metrics_ParsedQueryStatsSummsTest;
truncate table ttd_dpsr.metrics_ParsedQueryStatsEndedTest;
truncate table ttd_dpsr.metrics_ParsedQueryStatsWaitTest;
