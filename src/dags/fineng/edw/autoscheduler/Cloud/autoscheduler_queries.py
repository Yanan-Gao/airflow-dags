############################
# tableau_job_flow_queries #
############################

delete_tableau_job_history = "DELETE FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY WHERE LOG_ENTRY_DATE_UTC = date(sysdate());"

job_history_idempotency_check = "SELECT date(sysdate()) as LOG_ENTRY_DATE_UTC, COUNT(*) AS ROW_COUNT FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY WHERE LOG_ENTRY_DATE_UTC = date(sysdate())"

update_is_active_queue_to_false = "UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY SET IS_PREVIOUS_QUEUE = CASE WHEN IS_ACTIVE_QUEUE = TRUE THEN TRUE ELSE FALSE END, IS_ACTIVE_QUEUE = FALSE;"

tableau_job_load_to_history = """
INSERT INTO AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY
    with TABLEAU_OBJECTS as (
        select 
            WORKBOOK_ID as OBJECT_LUID
            , ifnull(max(type = 'snowflake'), false) as USES_EDW_SNOWFLAKE
            , ifnull(max(type = 'vertica' and SERVERADDRESS ilike any ('10.100.101.112', '%east%')), false) as USES_VERTICA_EAST
            , ifnull(max(type = 'vertica' and SERVERADDRESS ilike '%west%'), false) as USES_VERTICA_WEST
            
        from EDW.TABLEAUCLOUD.VW_TABLEAU_WORKBOOKCONNECTIONS
        
        group by
            OBJECT_LUID
        
        union
        
        select 
            DATASOURCE_ID as OBJECT_LUID
            , ifnull(max(type = 'snowflake'), false) as USES_EDW_SNOWFLAKE
            , ifnull(max(type = 'vertica' and SERVERADDRESS ilike any ('10.100.101.112', '%east%')), false) as USES_VERTICA_EAST
            , ifnull(max(type = 'vertica' and SERVERADDRESS ilike '%west%'), false) as USES_VERTICA_WEST
            
        from EDW.TABLEAUCLOUD.VW_TABLEAU_DATASOURCECONNECTIONS
        
        group by
            OBJECT_LUID
    )

    select
        date(sysdate()) as LOG_ENTRY_DATE_UTC
        , sysdate() as LOG_ENTRY_TIME_UTC
        , tab.OBJECT_LUID
        , obj.NAME as OBJECT_NAME
        , obj.OBJECTTYPE AS OBJECT_TYPE
        , u.EMAIL as OBJECT_OWNER
        , sch.SCHEDULE_ID
        , sch.SCHEDULE_NAME
        , sch.SCHEDULE_PRIORITY
        , sch.SCHEDULE_FREQUENCY_ID
        , sch.SCHEDULE_FREQUENCY_NAME
        , tab.USES_VERTICA_EAST
        , tab.USES_VERTICA_WEST
        , tab.USES_EDW_SNOWFLAKE
        , ROW_NUMBER() OVER (ORDER BY
            CASE WHEN w.OBJECT_LUID IS NULL THEN FALSE ELSE TRUE END DESC
            , SCHEDULE_FREQUENCY_ID ASC
            , SCHEDULE_PRIORITY ASC
            , OBJECT_PRIORITY ASC
        ) AS QUEUE_POSITION
        , TRUE AS IS_ACTIVE_QUEUE
        , FALSE AS IS_PREVIOUS_QUEUE
        , CASE WHEN w.OBJECT_LUID IS NULL THEN FALSE ELSE TRUE END AS IS_WHITELISTED
        , FALSE AS IS_READY_TO_GO
        , NULL AS IS_READY_TO_GO_TIME_UTC
        , NULL AS JOB_PICKUP_TIME_UTC
        , NULL AS GATE_ELAPSED_TIME_IN_SECONDS
        , NULL AS JOB_ID
        , NULL AS JOB_CREATED_TIME_UTC
        , NULL AS JOB_START_TIME_UTC
        , NULL AS JOB_COMPLETE_TIME_UTC
        , NULL AS JOB_RESULT_CODE
        , NULL AS JOB_RESULT
        , NULL AS JOB_RESULT_MESSAGE
        , 0::int AS ATTEMPT_COUNT

    from TABLEAU_OBJECTS tab
        INNER JOIN EDW.AUTOSCHEDULER_CLOUD.TABLEAU_OBJECT_REFRESH_ASSIGNMENT oa on oa.OBJECT_LUID = tab.OBJECT_LUID
        INNER JOIN EDW.AUTOSCHEDULER_CLOUD.TABLEAU_REFRESH_SCHEDULES sch on sch.SCHEDULE_ID = oa.SCHEDULE_ID
        INNER JOIN EDW.TABLEAUCLOUD.VW_TABLEAU_OBJECTS obj on obj.ID = tab.OBJECT_LUID
        LEFT JOIN EDW.TABLEAUCLOUD.VW_TABLEAU_USERS u on u.USERID = obj.OWNERID
        LEFT JOIN EDW.AUTOSCHEDULER_CLOUD.TABLEAU_REFRESH_WHITELIST w ON w.OBJECT_LUID = tab.OBJECT_LUID
    
    where (
    (CASE WHEN DATE_PART('day', sysdate()) = 1 THEN sch.SCHEDULE_FREQUENCY_ID ELSE 0 END) = 3 OR --monthly
    (CASE WHEN DATE_PART('dayofweek', sysdate()) = 0 THEN sch.SCHEDULE_FREQUENCY_ID ELSE 0 END) = 2 OR --weekly, on Sunday UTC
    sch.SCHEDULE_FREQUENCY_ID = 1 --daily
    )
;
"""

db_ready_date_check = "SELECT COUNT(*) FROM DATABASE_READINESS_CHECK WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s"

db_ready_insert_new_date = """
INSERT INTO AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK
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

active_q_idempotency_check = "SELECT COUNT(*) FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s"

active_q_truncate_query = "TRUNCATE TABLE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE;"

check_previous_day_job_history = """
SELECT count(*) 
FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY 
WHERE LOG_ENTRY_DATE_UTC = (
    SELECT DISTINCT LOG_ENTRY_DATE_UTC 
    FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY 
    ORDER BY LOG_ENTRY_DATE_UTC DESC 
    LIMIT 1 OFFSET 1
);
"""

############################
# warp fast pass queries   #
############################

luid_name_mapping = """select distinct object_luid, object_name from EDW.REPORT.VW_TABLEAUCLOUD_WARP2_DASHBOARDS_OBJECTID;"""

bump_warp_priority = """update autoscheduler_cloud.tableau_job_history tjh set 
    tjh.queue_position = -5
    where tjh.object_luid in (select luid from autoscheduler.WARP_OBJECT_LUIDS);"""

warp_dash_count = """select count(distinct(wdo.object_luid)) from EDW.REPORT.VW_TABLEAUCLOUD_WARP2_DASHBOARDS_OBJECTID WDO
                    inner join autoscheduler_cloud.tableau_job_history tjh on WDO.object_luid = tjh.object_luid
                    where tjh.log_entry_date_utc = CURRENT_DATE and tjh.is_active_queue = true;
                   """

warp_dash_fast_passed = """select count(object_luid) from autoscheduler_cloud.tableau_job_history tjh
where tjh.is_ready_to_go = 'true' and tjh.log_entry_date_utc = CURRENT_DATE and tjh.object_luid in (select object_luid from EDW.REPORT.VW_TABLEAUCLOUD_WARP2_DASHBOARDS_OBJECTID);"""

warp_mark_ready = """call autoscheduler_cloud.prc_update_luid_ready_to_go();"""

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

FROM AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK

WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
    AND {snowflake_column}_READY_TIME_UTC IS NOT NULL

LIMIT 1
;
"""
db_ready_update = """
UPDATE AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK
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
UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY AS q
SET
    IS_READY_TO_GO = TRUE
    , IS_READY_TO_GO_TIME_UTC = CURRENT_TIMESTAMP
    
FROM AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK AS drc

WHERE q.LOG_ENTRY_DATE_UTC = drc.LOG_ENTRY_DATE_UTC
    AND q.IS_ACTIVE_QUEUE = TRUE
    AND q.IS_READY_TO_GO = FALSE
    AND (NOT q.USES_VERTICA_EAST OR drc.IS_VERTICA_EAST_READY)
    AND (NOT q.USES_VERTICA_WEST OR drc.IS_VERTICA_WEST_READY)
    AND (NOT q.USES_EDW_SNOWFLAKE OR drc.IS_EDW_SNOWFLAKE_READY)
    AND q.OBJECT_LUID NOT IN (
        SELECT OBJECT_LUID
        FROM EDW.REPORT.VW_TABLEAUCLOUD_WARP2_DASHBOARDS_OBJECTID
    )
;
"""

########################
# all_db_ready queries #
########################

all_db_ready_idempotency_check = """
SELECT ALL_DATA_READY_TIME_UTC
FROM AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK 
WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

all_db_ready_validity_check = """
SELECT LEAST(IS_VERTICA_EAST_READY, IS_VERTICA_WEST_READY, IS_EDW_SNOWFLAKE_READY)
FROM AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK 
WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

all_db_ready_update = """
UPDATE AUTOSCHEDULER_CLOUD.DATABASE_READINESS_CHECK
SET ALL_DATA_READY_TIME_UTC = GREATEST(VERTICA_EAST_READY_TIME_UTC, VERTICA_WEST_READY_TIME_UTC, EDW_SNOWFLAKE_READY_TIME_UTC)
WHERE LOG_ENTRY_DATE_UTC = %(log_entry_date_utc)s
;
"""

####################################
# tableau_job_active_queue queries #
####################################

tableau_job_active_queue_insert = """
INSERT INTO AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE
SELECT
    h.LOG_ENTRY_DATE_UTC
    , %(log_entry_time_utc)s as LOG_ENTRY_TIME_UTC
    , h.OBJECT_LUID
    , h.OBJECT_NAME
    , h.OBJECT_TYPE
    , h.OBJECT_OWNER
    , h.QUEUE_POSITION
    , h.IS_READY_TO_GO_TIME_UTC
    , h.JOB_PICKUP_TIME_UTC
    , h.GATE_ELAPSED_TIME_IN_SECONDS
    , h.JOB_ID
    , h.JOB_CREATED_TIME_UTC
    , h.JOB_START_TIME_UTC
    , h.JOB_COMPLETE_TIME_UTC
    , h.JOB_RESULT_CODE
    , h.JOB_RESULT
    , h.JOB_RESULT_MESSAGE
    , h.ATTEMPT_COUNT

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY h

WHERE h.IS_READY_TO_GO = TRUE
    AND h.IS_ACTIVE_QUEUE = TRUE
    AND NOT EXISTS (
        SELECT 1 FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE a
        WHERE a.OBJECT_LUID = h.OBJECT_LUID
    )
;
"""

tableau_job_active_queue_audit_insert = """
SELECT
    aq.LOG_ENTRY_DATE_UTC
    , aq.LOG_ENTRY_TIME_UTC
    , aq.OBJECT_LUID
    , aq.OBJECT_NAME
    , aq.OBJECT_TYPE
    , aq.OBJECT_OWNER
    , aq.QUEUE_POSITION
    , CASE WHEN aq.LOG_ENTRY_TIME_UTC = %(log_entry_time_utc)s THEN TRUE ELSE FALSE END AS AUDIT_INSERT
    , ROW_NUMBER() OVER (PARTITION BY OBJECT_LUID ORDER BY LOG_ENTRY_TIME_UTC ASC) AS RECORD_COUNT

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE aq
;
"""

tableau_job_delete_duplicates = """
DELETE FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE
WHERE (OBJECT_LUID, LOG_ENTRY_TIME_UTC) IN (
    SELECT OBJECT_LUID, LOG_ENTRY_TIME_UTC
    FROM (
        SELECT OBJECT_LUID, LOG_ENTRY_TIME_UTC,
               ROW_NUMBER() OVER (PARTITION BY OBJECT_LUID ORDER BY LOG_ENTRY_TIME_UTC ASC) AS RECORD_COUNT
        FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE
    ) sub
    WHERE RECORD_COUNT > 1
);
"""

#############################
# executed_jobs_today query #
#############################

executed_jobs_today = "SELECT DISTINCT OBJECT_LUID FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE WHERE JOB_PICKUP_TIME_UTC IS NOT NULL ;"

########################
# autoscheduler result #
########################

autoscheduler_result = """
SELECT
    LOG_ENTRY_DATE_UTC
    , COUNT(DISTINCT OBJECT_LUID) AS TOTAL_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE = 0 THEN 1 ELSE 0 END) AS SUCCESSFUL_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE = 1 THEN 1 ELSE 0 END) AS FAILED_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE = 2 THEN 1 ELSE 0 END) AS CANCELLED_JOB_COUNT
    , SUM(CASE WHEN JOB_RESULT_CODE IS NULL THEN 1 ELSE 0 END) AS SKIPPED_JOB_COUNT
FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY
WHERE IS_ACTIVE_QUEUE = TRUE
GROUP BY 
    LOG_ENTRY_DATE_UTC
;
"""

###############
# job_fetcher #
###############

fetch_a_job = """
SELECT
    LOG_ENTRY_DATE_UTC
    , OBJECT_TYPE
    , OBJECT_LUID
    , OBJECT_NAME
    , QUEUE_POSITION

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE 

WHERE JOB_PICKUP_TIME_UTC IS NULL

ORDER BY QUEUE_POSITION ASC 
LIMIT 1
;
"""

populator_object_fetch = """

SELECT
    LOG_ENTRY_DATE_UTC
    , OBJECT_TYPE
    , OBJECT_LUID
    , OBJECT_NAME
    , QUEUE_POSITION

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE 

WHERE JOB_PICKUP_TIME_UTC IS NULL
ORDER BY QUEUE_POSITION ASC 
;
"""

####################
# Job Delayer      #
####################

fetch_avg_run_time = """
select object_name, avg_time 
from autoscheduler_cloud.avg_job_completion_time
"""

#####################
# active_job_update #
#####################

active_job_update = """
UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE 
SET
    JOB_PICKUP_TIME_UTC = %(job_pickup_time_utc)s
    , GATE_ELAPSED_TIME_IN_SECONDS = %(gate_elapsed_time_in_seconds)s

WHERE OBJECT_LUID = %(object_luid)s
;
"""

###################
# job_id_update #
####################

job_id_update = "UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE SET JOB_ID = %(job_id)s, ATTEMPT_COUNT = ATTEMPT_COUNT + 1 WHERE OBJECT_LUID = %(object_luid)s ;"

job_error_update_without_id = "UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE SET JOB_RESULT_CODE = 1, JOB_RESULT = 'ERROR', JOB_RESULT_MESSAGE = %(job_result_message)s WHERE OBJECT_LUID = %(object_luid)s ;"

job_id_full_update = """UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE 
SET JOB_ID = %(job_id)s, 
ATTEMPT_COUNT = ATTEMPT_COUNT + 1,
JOB_PICKUP_TIME_UTC = %(job_pickup_time_utc)s,
GATE_ELAPSED_TIME_IN_SECONDS = %(gate_elapsed_time_in_seconds)s
WHERE OBJECT_LUID = %(object_luid)s ;

"""

job_id_full_error_update = """UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE
SET JOB_ID = NULL,
ATTEMPT_COUNT = ATTEMPT_COUNT + 1,
JOB_PICKUP_TIME_UTC = %(job_pickup_time_utc)s,
GATE_ELAPSED_TIME_IN_SECONDS = %(gate_elapsed_time_in_seconds)s,
JOB_RESULT_CODE = 1,
JOB_RESULT = 'ERROR',
JOB_RESULT_MESSAGE = %(job_result_message)s
WHERE OBJECT_LUID = %(object_luid)s ;
"""

#########################
# job_start_time_update #
#########################

job_start_time_update = """
UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE
SET 
    JOB_CREATED_TIME_UTC = %(job_created_time_utc)s
    , JOB_START_TIME_UTC = %(job_start_time_utc)s

WHERE JOB_ID = %(job_id)s ;
;
"""

##################
# log_job_result #
###################

update_job_with_result = """
UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE
SET 
    JOB_CREATED_TIME_UTC = %(job_created_at)s
    , JOB_START_TIME_UTC = %(job_started_at)s
    , JOB_COMPLETE_TIME_UTC = %(job_completed_at)s
    , JOB_RESULT_CODE = %(job_result_code)s
    , JOB_RESULT = %(job_result)s
    , JOB_RESULT_MESSAGE = %(job_result_message)s

WHERE OBJECT_LUID = %(object_luid)s
;
"""

########################
# all_in_progress_jobs #
########################

all_in_progress_jobs = """
SELECT
    LOG_ENTRY_DATE_UTC
    , OBJECT_LUID
    , OBJECT_NAME
    , JOB_ID

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE

WHERE JOB_ID IS NOT NULL 
    AND JOB_RESULT_CODE IS NULL
;
"""

##########################
# single_in_progress_job #
##########################

single_in_progress_job = """
SELECT
    LOG_ENTRY_DATE_UTC
    , OBJECT_LUID
    , OBJECT_NAME
    , JOB_ID

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE

WHERE OBJECT_LUID = %(OBJECT_LUID)s
;
"""

###################################
# update_job_history_with_results #
###################################

update_job_history_with_results = """
UPDATE AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY hist
SET
    hist.JOB_PICKUP_TIME_UTC = act.JOB_PICKUP_TIME_UTC
    , hist.GATE_ELAPSED_TIME_IN_SECONDS = act.GATE_ELAPSED_TIME_IN_SECONDS
    , hist.JOB_ID = act.JOB_ID
    , hist.JOB_CREATED_TIME_UTC = act.JOB_CREATED_TIME_UTC
    , hist.JOB_START_TIME_UTC = act.JOB_START_TIME_UTC
    , hist.JOB_COMPLETE_TIME_UTC = act.JOB_COMPLETE_TIME_UTC
    , hist.JOB_RESULT_CODE = act.JOB_RESULT_CODE
    , hist.JOB_RESULT = act.JOB_RESULT
    , hist.JOB_RESULT_MESSAGE = act.JOB_RESULT_MESSAGE
    , hist.ATTEMPT_COUNT = act.ATTEMPT_COUNT

FROM AUTOSCHEDULER_CLOUD.TABLEAU_JOB_ACTIVE_QUEUE act

WHERE hist.LOG_ENTRY_DATE_UTC = act.LOG_ENTRY_DATE_UTC
    AND hist.OBJECT_LUID = act.OBJECT_LUID
    AND hist.IS_ACTIVE_QUEUE = TRUE
    AND hist.JOB_PICKUP_TIME_UTC IS NULL
;
"""
