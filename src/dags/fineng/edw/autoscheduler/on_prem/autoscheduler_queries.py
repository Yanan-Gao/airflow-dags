############################
# tableau_job_flow_queries #
############################
tableau_job_flow_from_postgres = """
with q_data as (
    select
        date(now() at time zone 'UTC') as "LOG_ENTRY_DATE_UTC"
        , now() at time zone 'UTC' as "LOG_ENTRY_TIME_UTC"
        , s.name as "SCHEDULE_NAME"
        , split_part(s.name, '_', 3) as "SCHEDULE_CUSTOM_PRIORITY"
        , s.schedule_type as "SCHEDULE_FREQUENCY"
        , s.name like '%Adhoc_Updater' as "SCHEDULE_IS_ADHOC_UPDATER"
        , t.priority as "TASK_PRIORITY"
        , t.obj_type as "TASK_OBJECT_TYPE"
        , su.email as "TASK_OWNER"
        , t.luid as "TASK_LUID"
        , case when t.obj_type = 'Workbook' then w.name when t.obj_type = 'Datasource' then d.name end as "TASK_TARGET_NAME"
        , case when t.obj_type = 'Workbook' then w.luid when t.obj_type = 'Datasource' then d.luid end as "OBJECT_LUID"
        , sum(case when dc.server in ('10.100.101.112', 'bi.useast01.vertica.adsrvr.org', 'useast01.vertica.adsrvr.org') then 1 else 0 end) > 0 as "USES_VERTICA_EAST"
        , sum(case when dc.server in ('uswest01.vertica.adsrvr.org') then 1 else 0 end) > 0 as "USES_VERTICA_WEST"
        , sum(case when dc.dbclass = 'snowflake' then 1 else 0 end) > 0 as "USES_EDW_SNOWFLAKE"
        , true as "IS_ACTIVE_QUEUE"
        , false as "IS_PREVIOUS_QUEUE"
    
    from public.schedules s
        inner join public.tasks t on t.schedule_id = s.id
        left join public.datasources d on d.id = t.obj_id
        left join public.workbooks w on  w.id = t.obj_id
        left join public.data_connections dc on dc.owner_type = t.obj_type and dc.owner_id = coalesce(d.id,w.id)
        left join public.users u on u.id = coalesce(d.modified_by_user_id, w.modified_by_user_id)
        left join public.system_users su on su.id = u.system_user_id
    
    where (s.name like 'AutoScheduler_%'
            or s.name = 'An EDW Schedule ONLY')
        --the following filters are to only return results for weekly refreshes on saturday, and monthly refreshes on the 1st of the month
        and (
            (case when extract('day' from now() at time zone 'UTC') = 1 then s.schedule_type else 0 end) = 3 or --monthly
            (case when extract(isodow from now() at time zone 'UTC') = 6 then s.schedule_type else 0 end) = 2 or --weekly
            s.schedule_type = 1 --daily
            )
    
    group by
        "SCHEDULE_NAME"
        , "SCHEDULE_CUSTOM_PRIORITY"
        , "SCHEDULE_FREQUENCY"
        , "SCHEDULE_IS_ADHOC_UPDATER"
        , "TASK_PRIORITY"
        , "TASK_OBJECT_TYPE"
        , "TASK_OWNER"
        , "TASK_LUID"
        , "TASK_TARGET_NAME"
        , "OBJECT_LUID"
)
select *
from q_data
"""
# Snowflake Queries in this function happen in the following order:

job_history_duplicate_check = "SELECT COUNT(*) FROM TABLEAU_JOB_HISTORY WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s"

update_is_active_queue_to_false = "UPDATE TABLEAU_JOB_HISTORY SET IS_PREVIOUS_QUEUE = CASE WHEN IS_ACTIVE_QUEUE = TRUE THEN TRUE ELSE FALSE END, IS_ACTIVE_QUEUE = FALSE;"

tableau_job_flow_load_to_temp_snowflake = """
CREATE OR REPLACE TEMPORARY TABLE TABLEAU_POSTGRES_QUEUE AS
    SELECT
        LOG_ENTRY_DATE_UTC
        , LOG_ENTRY_TIME_UTC
        , SCHEDULE_NAME
        , SCHEDULE_CUSTOM_PRIORITY
        , SCHEDULE_FREQUENCY
        , SCHEDULE_IS_ADHOC_UPDATER
        , TASK_PRIORITY
        , TASK_OBJECT_TYPE
        , TASK_OWNER
        , TASK_LUID
        , TASK_TARGET_NAME
        , OBJECT_LUID
        , USES_VERTICA_EAST
        , USES_VERTICA_WEST
        , USES_EDW_SNOWFLAKE
        , IS_ACTIVE_QUEUE
        , IS_PREVIOUS_QUEUE 

    FROM AUTOSCHEDULER.TABLEAU_JOB_HISTORY

    LIMIT 0"""
load_temp_job_flow_to_prod = """
INSERT INTO TABLEAU_JOB_HISTORY
WITH Q_PREP AS (
    SELECT 
        t.*
        , CASE WHEN w.OBJECT_LUID IS NULL THEN FALSE ELSE TRUE END AS IS_WHITELISTED
    FROM TABLEAU_POSTGRES_QUEUE t
    LEFT JOIN TABLEAU_REFRESH_WHITELIST w ON t.OBJECT_LUID = w.OBJECT_LUID
)
SELECT
    LOG_ENTRY_DATE_UTC
    , LOG_ENTRY_TIME_UTC
    , FALSE AS IS_READY_TO_GO
    , NULL AS IS_READY_TO_GO_TIME_UTC
    , SCHEDULE_NAME
    , SCHEDULE_CUSTOM_PRIORITY
    , SCHEDULE_FREQUENCY
    , SCHEDULE_IS_ADHOC_UPDATER
    , TASK_PRIORITY
    , TASK_OBJECT_TYPE
    , TASK_OWNER
    , TASK_LUID
    , TASK_TARGET_NAME
    , OBJECT_LUID
    , USES_VERTICA_EAST
    , USES_VERTICA_WEST
    , USES_EDW_SNOWFLAKE
    , IS_ACTIVE_QUEUE
    , IS_PREVIOUS_QUEUE
    , IS_WHITELISTED
    , ROW_NUMBER() OVER (ORDER BY
        IS_WHITELISTED DESC
        , SCHEDULE_IS_ADHOC_UPDATER DESC
        , SCHEDULE_CUSTOM_PRIORITY ASC
        , SCHEDULE_FREQUENCY ASC
        , TASK_PRIORITY ASC
    ) AS QUEUE_POSITION
    , NULL AS TASK_PICKUP_TIME_UTC
    , NULL AS GATE_ELAPSED_TIME_IN_SECONDS
    , NULL AS JOB_ID
    , NULL AS JOB_CREATED_TIME_UTC
    , NULL AS JOB_START_TIME_UTC
    , NULL AS JOB_COMPLETE_TIME_UTC
    , NULL AS JOB_RESULT_CODE
    , NULL AS JOB_RESULT
    , NULL AS JOB_RESULT_MESSAGE
    , 0::int AS ATTEMPT_COUNT

FROM Q_PREP;
"""
drop_job_flow_temp = "DROP TABLE IF EXISTS TABLEAU_POSTGRES_QUEUE"

db_ready_duplicate_check = "SELECT COUNT(*) FROM DATABASE_READINESS_CHECK WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s"

db_ready_insert_new_date = """
INSERT INTO AUTOSCHEDULER.DATABASE_READINESS_CHECK
(
    LOG_ENTRY_DATE_UTC,
    IS_VERTICA_EAST_READY,
    VERTICA_EAST_READY_TIME_UTC,
    IS_VERTICA_WEST_READY,
    VERTICA_WEST_READY_TIME_UTC,
    IS_EDW_SNOWFLAKE_READY,
    EDW_SNOWFLAKE_READY_TIME_UTC,
    ALL_DATA_READY_TIME_UTC
)
SELECT
    %(log_entry_date_utc)s,
    FALSE,
    NULL,
    FALSE,
    NULL,
    FALSE,
    NULL,
    NULL
;
"""

active_q_duplicate_check = "SELECT COUNT(*) FROM TABLEAU_JOB_ACTIVE_QUEUE WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s"

active_q_truncate_query = "TRUNCATE TABLE AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE;"

############################
# warp fast pass queries   #
############################

luid_name_mapping = """select distinct luid, name from autoscheduler.warp_object_luids;"""

bump_warp_priority = """update autoscheduler.tableau_job_history tjh set 
    tjh.queue_position = -5
    where tjh.object_luid in (select luid from autoscheduler.WARP_OBJECT_LUIDS);"""

warp_dash_count = ("""select count(distinct(name)) from autoscheduler.warp_object_luids;""")

warp_dash_fast_passed = """select count(object_luid) from autoscheduler.tableau_job_history tjh
where tjh.is_ready_to_go = 'true' and tjh.log_entry_date_utc = CURRENT_DATE and tjh.object_luid in (select luid from autoscheduler.warp_object_luids);"""

warp_mark_ready = """call autoscheduler.prc_update_luid_ready_to_go();"""

############################
# is_vertica_ready queries #
############################

# Query ProvDb to retrieve the most up-to-date date for Vertica
vertica_max_date_query = """
/*
Lookup values:
select * from rptsched.ReportProviderSource where ReportProviderSourceId in (7, 10)
;
select * from rptsched.ReportSchedulingEventType where ReportSchedulingEventTypeId in (1, 2, 3, 8, 9, 13)
;
*/

with sourceReportEvents as (
    select
        cast(EventData as date) as ReportDate
        , CloudServiceId
        , ReportProviderSourceId
        , ReportSchedulingEventTypeId
        , count(distinct (datepart(hour, EventData))) as EventCount
    
    from rptsched.ReportSchedulingEvent
    
    where cast(EventData as datetime) >= dateadd(day, -3, cast(getutcdate() as date))
        and cast(EventData as datetime) < cast(getutcdate() as date)
        and ReportProviderSourceId in (%s)
        and ReportSchedulingEventTypeId in (1, 2, 3, 8, 9, 13)

    group by
        cast(EventData as date)
        , CloudServiceId
        , ReportProviderSourceId
        , ReportSchedulingEventTypeId
)
, eventValidation as (
    select
        CloudServiceId
        , ReportProviderSourceId
        , ReportSchedulingEventTypeId
        , max(ReportDate) as MaxDate

    from sourceReportEvents

    where (EventCount = 24 and ReportSchedulingEventTypeId != 13)
        or (EventCount = 1 and ReportSchedulingEventTypeId = 13)

    group by
        CloudServiceId
        , ReportProviderSourceId
        , ReportSchedulingEventTypeId
)
select 
    min(MaxDate) as MaxDateInclusive

    from eventValidation
"""
# Validating BI Daily Agg tables in Vertica to see if they match their source tables in Reports schema
vertica_agg_data_ready_query = """
with OriginalData as (
    select 
        date(ReportHourUtc) as ReportDateUtc
        , 'Platform' as SpendType
        , sum(PartnerCostInUSD) as CostInUSD
    
    from reports.PerformanceReport

    where ReportHourUtc >= date(getutcdate()) - 1
        and ReportHourUtc < date(getutcdate())

    group by 
        date(ReportHourUtc)

    union all

    select 
        date(ReportHourUtc) as ReportDateUtc
        , 'UsedDataCost' as SpendType
        , sum(UsedDataCostInUSD) as CostInUSD
    
    from reports.RTBPlatformDataElementReport

    where ReportHourUtc >= date(getutcdate()) - 1
        and ReportHourUtc < date(getutcdate())

    group by 
        date(ReportHourUtc)
)
, AggData as (
    select 
        ReportDateUtc
        , 'Platform' as SpendType
        , sum(PartnerCostInUSD) as CostInUSD
    
    from bi.RTBPlatformReportDaily

    where ReportDateUtc >= date(getutcdate()) - 1
        and ReportDateUtc < date(getutcdate())

    group by 
        ReportDateUtc

    union all

    select 
        ReportDateUtc
        , 'UsedDataCost' as SpendType
        , sum(UsedDataCostInUSD) as CostInUSD
    
    from bi.RTBPlatformDataElementReportDaily

    where ReportDateUtc >= date(getutcdate()) - 1
        and ReportDateUtc < date(getutcdate())

    group by 
        ReportDateUtc
)
select
    min(o.CostInUSD = zeroifnull(a.CostInUSD)) as Matching
    
from OriginalData o
    left join AggData a on a.ReportDateUtc = o.ReportDateUtc
        and a.SpendType = o.SpendType
;
"""

vertica_attribution_data_ready_query = """

SELECT
    CASE
        WHEN COUNT(*) > 0 THEN TRUE
        ELSE FALSE
    END AS attributionDataExists
FROM bi.rtbplatformreportdaily
 WHERE reportdateutc = date(getutcdate()) - 1
   AND platformreportsource = 2;

"""

#####################
# db_ready_ queries #
#####################

db_ready_idempotency_check = """
SELECT LOG_ENTRY_DATE_UTC

FROM AUTOSCHEDULER.DATABASE_READINESS_CHECK

WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
    AND {snowflake_column}_READY_TIME_UTC IS NOT NULL

LIMIT 1
;
"""
db_ready_update = """
UPDATE AUTOSCHEDULER.DATABASE_READINESS_CHECK
SET
    {readiness_column} = TRUE
    , {readiness_time_column} = %(database_ready_time)s

WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

########################
# is_edw_ready queries #
########################

#retrieve the max available date from Snowflake EDW Prod
snow_edw_prod_max_date = """
WITH QUALITYCHECK AS (
    SELECT 
        SCHEMANAME
        , TABLENAME
        , MIN(DATE(UPDATEDATE)) AS UPDATEDATE
        , SUM(CASE WHEN VALIDATION_STATUS THEN 1 END) AS VALID_COLUMNS
        , COUNT(1) AS TOTAL_COLUMNS
    
    FROM EDW.REPORT.VW_EDW_DATAQUALITYCHECKS
    
    GROUP BY 
        SCHEMANAME
        , TABLENAME
)
SELECT
    MIN(UPDATEDATE) - 1 AS UPDATEDATE

FROM QUALITYCHECK
;
"""

##############################
# tableau_job_is_ready query #
##############################

tableau_job_is_ready_update = """
UPDATE AUTOSCHEDULER.TABLEAU_JOB_HISTORY AS q
SET
    IS_READY_TO_GO = TRUE
    , IS_READY_TO_GO_TIME_UTC = CURRENT_TIMESTAMP
    
FROM AUTOSCHEDULER.DATABASE_READINESS_CHECK AS drc

WHERE q.LOG_ENTRY_DATE_UTC = drc.LOG_ENTRY_DATE_UTC
    AND q.IS_ACTIVE_QUEUE = TRUE
    AND q.IS_READY_TO_GO = FALSE
    AND (NOT q.USES_VERTICA_EAST OR drc.IS_VERTICA_EAST_READY)
    AND (NOT q.USES_VERTICA_WEST OR drc.IS_VERTICA_WEST_READY)
    AND (NOT q.USES_EDW_SNOWFLAKE OR drc.IS_EDW_SNOWFLAKE_READY)
;"""

########################
# all_db_ready queries #
########################

all_db_ready_idempotency_check = """
SELECT ALL_DATA_READY_TIME_UTC
FROM AUTOSCHEDULER.DATABASE_READINESS_CHECK 
WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

all_db_ready_validity_check = """
SELECT LEAST(IS_VERTICA_EAST_READY, IS_VERTICA_WEST_READY, IS_EDW_SNOWFLAKE_READY)
FROM AUTOSCHEDULER.DATABASE_READINESS_CHECK 
WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

all_db_ready_update = """
UPDATE AUTOSCHEDULER.DATABASE_READINESS_CHECK
SET ALL_DATA_READY_TIME_UTC = GREATEST(VERTICA_EAST_READY_TIME_UTC, VERTICA_WEST_READY_TIME_UTC, EDW_SNOWFLAKE_READY_TIME_UTC)
WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

##################################
# tableau_job_active_queue query #
##################################

tableau_job_active_queue_insert = """
INSERT INTO TABLEAU_JOB_ACTIVE_QUEUE
SELECT
    h.LOG_ENTRY_DATE_UTC
    , h.TASK_OBJECT_TYPE
    , h.TASK_LUID
    , h.TASK_TARGET_NAME
    , h.QUEUE_POSITION
    , FALSE AS IS_HOT
    , h.TASK_PICKUP_TIME_UTC
    , h.GATE_ELAPSED_TIME_IN_SECONDS
    , h.JOB_ID
    , h.JOB_CREATED_TIME_UTC
    , h.JOB_START_TIME_UTC
    , h.JOB_COMPLETE_TIME_UTC
    , h.JOB_RESULT_CODE
    , h.JOB_RESULT
    , h.JOB_RESULT_MESSAGE
    , h.ATTEMPT_COUNT

FROM TABLEAU_JOB_HISTORY h

WHERE h.IS_READY_TO_GO = TRUE
    AND h.IS_ACTIVE_QUEUE = TRUE
    AND NOT EXISTS (
        SELECT 1 FROM TABLEAU_JOB_ACTIVE_QUEUE a 
        WHERE a.TASK_LUID = h.TASK_LUID
    )
;
"""
#############################
# executed_jobs_today query #
#############################
executed_jobs_today = "SELECT TASK_LUID FROM AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE WHERE IS_HOT = TRUE ;"

########################
# autoscheduler result #
########################
autoscheduler_result = """
SELECT
    COUNT(*) AS TOTAL_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE = 0 THEN 1 ELSE 0 END) AS SUCCESSFUL_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE = 1 THEN 1 ELSE 0 END) AS FAILED_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE = 2 THEN 1 ELSE 0 END) AS CANCELLED_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE IS NULL THEN 1 ELSE 0 END) AS SKIPPED_JOB_COUNT
FROM AUTOSCHEDULER.TABLEAU_JOB_HISTORY 
WHERE IS_ACTIVE_QUEUE = TRUE
;
"""

######################
# retrieve_cold_task #
######################
cold_task_fetch = """
SELECT
    LOG_ENTRY_DATE_UTC
    , TASK_OBJECT_TYPE
    , TASK_LUID
    , TASK_TARGET_NAME
    , QUEUE_POSITION
    , IS_HOT

FROM AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE 

WHERE IS_HOT = FALSE 

ORDER BY QUEUE_POSITION ASC 
LIMIT 1
;
"""

###################
# hot_task_update #
####################
is_hot_true_update = """
UPDATE AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE 
SET
    IS_HOT = TRUE
    , TASK_PICKUP_TIME_UTC = %(task_pickup_timestamp)s
    , GATE_ELAPSED_TIME_IN_SECONDS = %(gate_elapsed_time_in_seconds)s

WHERE TASK_LUID = %(task_luid)s
;
"""

###################
# job_id_update #
####################
job_id_update = "UPDATE AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE SET JOB_ID = %(job_id)s, ATTEMPT_COUNT = ATTEMPT_COUNT + 1 WHERE TASK_LUID = %(task_luid)s ;"

##################
# log_job_result #
###################
update_job_with_result = """
UPDATE AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE
SET 
    JOB_CREATED_TIME_UTC = %(job_created_at)s
    , JOB_START_TIME_UTC = %(job_started_at)s
    , JOB_COMPLETE_TIME_UTC = %(job_completed_at)s
    , JOB_RESULT_CODE = %(job_result_code)s
    , JOB_RESULT = %(job_result)s
    , JOB_RESULT_MESSAGE = %(job_result_message)s

WHERE TASK_LUID = %(task_luid)s
;
"""

####################
# in_progress_jobs #
####################
in_progress_jobs = """
SELECT
    LOG_ENTRY_DATE_UTC
    , TASK_LUID
    , TASK_TARGET_NAME
    , JOB_ID

FROM TABLEAU_JOB_ACTIVE_QUEUE

WHERE JOB_ID IS NOT NULL 
    AND JOB_RESULT_CODE IS NULL
;
"""
####################
# in_progress_task #
####################
in_progress_task = """
SELECT
    LOG_ENTRY_DATE_UTC
    , TASK_LUID
    , TASK_TARGET_NAME
    , JOB_ID

FROM TABLEAU_JOB_ACTIVE_QUEUE

WHERE TASK_LUID = %(task_luid)s
;
"""

###################################
# update_job_history_with_results #
###################################
update_job_history_with_results = """
UPDATE AUTOSCHEDULER.TABLEAU_JOB_HISTORY hist
SET
    hist.TASK_PICKUP_TIME_UTC = act.TASK_PICKUP_TIME_UTC
    , hist.GATE_ELAPSED_TIME_IN_SECONDS = act.GATE_ELAPSED_TIME_IN_SECONDS
    , hist.JOB_ID = act.JOB_ID
    , hist.JOB_CREATED_TIME_UTC = act.JOB_CREATED_TIME_UTC
    , hist.JOB_START_TIME_UTC = act.JOB_START_TIME_UTC
    , hist.JOB_COMPLETE_TIME_UTC = act.JOB_COMPLETE_TIME_UTC
    , hist.JOB_RESULT_CODE = act.JOB_RESULT_CODE
    , hist.JOB_RESULT = act.JOB_RESULT
    , hist.JOB_RESULT_MESSAGE = act.JOB_RESULT_MESSAGE
    , hist.ATTEMPT_COUNT = act.ATTEMPT_COUNT

FROM AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE act

WHERE hist.LOG_ENTRY_DATE_UTC = act.LOG_ENTRY_DATE_UTC
    AND hist.TASK_LUID = act.TASK_LUID
    AND hist.IS_ACTIVE_QUEUE = TRUE
    AND hist.TASK_PICKUP_TIME_UTC IS NULL
;
"""
