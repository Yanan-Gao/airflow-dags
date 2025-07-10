from datetime import datetime, timedelta

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from ttd.eldorado.base import TtdDag
from ttd.ttdslack import dag_post_to_slack_callback

from ttd.tasks.op import OpTask

##################################
# Job Configurations
##################################
job_name = 'snowflake-function-sync'
provisioning_conn_id = 'provdb-readonly'
snowflake_conn_id = 'snowflake'
snowflake_warehouse = 'TTD_AIRFLOW'
snowflake_database = 'THETRADEDESK'
snowflake_schema = 'REDS'

dry_run = False

##################################
# DAG Configurations
##################################
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'depends_on_past': False,
}

##################################
# Task Configurations
##################################

dag = TtdDag(
    dag_id=job_name,
    default_args=default_args,
    slack_channel='#scrum-data-services-alarms',
    on_failure_callback=dag_post_to_slack_callback(dag_name=job_name, step_name='', slack_channel='#scrum-data-services-alarms'),
    start_date=datetime(2025, 6, 1),
    tags=["DATASRVC", "Snowflake"],
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    run_only_latest=True,
)

adag = dag.airflow_dag


##################################
# Sync Configurations
##################################
class SyncTemplate:

    def __init__(self, function_name, get_mapping_query, create_mapping_query, case_expr_template):
        self.function_name = function_name
        self.get_mapping_query = get_mapping_query
        self.create_mapping_query = create_mapping_query
        self.case_expr_template = case_expr_template


# When adding new sync templates, make sure you grant the ownership of the function to
# TTD_TASKSERIVCE otherwise the DAG would fail
# GRANT OWNERSHIP ON FUNCTION {function_to_sync} TO ROLE TTD_TASKSERVICE COPY CURRENT GRANTS
sync_templates = [
    SyncTemplate(
        'REDS.FN_OSIDTOOSNAME(NUMBER)', "SELECT OSID, REPLACE(REPLACE(OSName, ' ', ''), '.', '') FROM dbo.OS;", [
            '''
                create or replace function reds.FN_OSIDTOOSNAME(OSID int) returns varchar(15)
                    as
                        $$
                            case
                                {case_statements}
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_OSIDTOOSNAME(int) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_OSIDTOOSNAME(int) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_OSIDTOOSNAME(int) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_OSIDTOOSNAME(int) to role SYSADMIN with grant option;',
        ], "when OSID = {item1} then '{item2}'"
    ),
    SyncTemplate(
        'REDS.FN_OSNAMETOOSID(VARCHAR)', "SELECT REPLACE(REPLACE(OSName, ' ', ''), '.', ''), OSID FROM dbo.OS;", [
            '''
                create or replace function reds.FN_OSNAMETOOSID(OSNAME varchar) returns number
                    as
                        $$
                            case
                                {case_statements}
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_OSNAMETOOSID(varchar) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_OSNAMETOOSID(varchar) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_OSNAMETOOSID(varchar) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_OSNAMETOOSID(varchar) to role SYSADMIN with grant option;',
        ], "when OSNAME = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(VARCHAR)', "SELECT Description, AdsTxtSellerTypeId FROM dbo.AdsTxtSellerType;", [
            '''
                create or replace function reds.FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(ADSTXTSELLERTYPENAME VARCHAR) RETURNS NUMBER
                    as
                        $$
                            case
                                {case_statements}
                                when ADSTXTSELLERTYPENAME = 'Unauthorized' then 3
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(varchar) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(varchar) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(varchar) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(varchar) to role SYSADMIN with grant option;',
            'grant usage on function reds.FN_ADSTXTSELLERTYPENAMETOADSTXTSELLERTYPEID(varchar) to role TTD_UI;',
        ], "when ADSTXTSELLERTYPENAME = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_BROWSERNAMETOBROWSERID(VARCHAR)', "SELECT BrowserName, BrowserId FROM dbo.Browser;", [
            '''
                create or replace function reds.FN_BROWSERNAMETOBROWSERID(BROWSERNAME VARCHAR) RETURNS NUMBER
                    as
                        $$
                            case
                                {case_statements}
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_BROWSERNAMETOBROWSERID(varchar) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_BROWSERNAMETOBROWSERID(varchar) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_BROWSERNAMETOBROWSERID(varchar) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_BROWSERNAMETOBROWSERID(varchar) to role SYSADMIN with grant option;',
            'grant usage on function reds.FN_BROWSERNAMETOBROWSERID(varchar) to role TTD_UI;',
        ], "when BROWSERNAME = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_DEVICETYPENAMETODEVICETYPEID(VARCHAR)', "SELECT DeviceTypeName, DeviceTypeId FROM dbo.DeviceType;", [
            '''
                create or replace function reds.FN_DEVICETYPENAMETODEVICETYPEID(DEVICETYPEID VARCHAR) RETURNS NUMBER
                    as
                        $$
                            case
                                {case_statements}
                                when deviceTypeId = 'Unknown' then 0
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_DEVICETYPENAMETODEVICETYPEID(varchar) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_DEVICETYPENAMETODEVICETYPEID(varchar) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_DEVICETYPENAMETODEVICETYPEID(varchar) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_DEVICETYPENAMETODEVICETYPEID(varchar) to role SYSADMIN with grant option;',
        ], "when DEVICETYPEID = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_MAPSUPPLYVENDORNAME(VARCHAR)',
        "SELECT LOWER(SupplyVendorName), SupplyVendorNameOverride FROM dbo.SupplyVendor WHERE SupplyVendorNameOverride IS NOT NULL;", [
            '''
                create or replace function reds.FN_MAPSUPPLYVENDORNAME(SUPPLYVENDORNAME VARCHAR) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                else SUPPLYVENDORNAME
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_MAPSUPPLYVENDORNAME(varchar) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_MAPSUPPLYVENDORNAME(varchar) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_MAPSUPPLYVENDORNAME(varchar) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_MAPSUPPLYVENDORNAME(varchar) to role SYSADMIN with grant option;',
        ], "when supplyVendorName = '{item1}' then '{item2}'"
    ),
    SyncTemplate(
        'FN_OSFAMILYNAMETOOSFAMILYID(VARCHAR)', "SELECT REPLACE(OSFamilyName, ' ', ''), OSFamilyId FROM dbo.OSFamily;", [
            '''
                create or replace function reds.FN_OSFAMILYNAMETOOSFAMILYID(OSFAMILYNAME VARCHAR) RETURNS NUMBER
                    as
                        $$
                            case
                                {case_statements}
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_OSFAMILYNAMETOOSFAMILYID(varchar) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_OSFAMILYNAMETOOSFAMILYID(varchar) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_OSFAMILYNAMETOOSFAMILYID(varchar) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_OSFAMILYNAMETOOSFAMILYID(varchar) to role SYSADMIN with grant option;',
        ], "when OSFAMILYNAME = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_PLACEMENTPOSITIONRELATIVETOFOLDIDTONAME(NUMBER)',
        "SELECT PlacementPositionRelativeToFoldId, Name FROM dbo.PlacementPositionRelativeToFold;", [
            '''
                create or replace function reds.FN_PLACEMENTPOSITIONRELATIVETOFOLDIDTONAME(POSITIONID NUMBER) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                when POSITIONID = 0 then 'None'
                                else to_varchar(POSITIONID)
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_PLACEMENTPOSITIONRELATIVETOFOLDIDTONAME(NUMBER) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_PLACEMENTPOSITIONRELATIVETOFOLDIDTONAME(NUMBER) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_PLACEMENTPOSITIONRELATIVETOFOLDIDTONAME(NUMBER) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_PLACEMENTPOSITIONRELATIVETOFOLDIDTONAME(NUMBER) to role SYSADMIN with grant option;',
        ], "when POSITIONID = {item1} then '{item2}'"
    ),
    SyncTemplate(
        'FN_POSTIMPRESSIONEVENTTYPETOPOSTIMPRESSIONEVENTTYPENAME(NUMBER)',
        "SELECT PostImpressionEventTypeId, PostImpressionEventTypeName FROM dbo.PostImpressionEventType;", [
            '''
                create or replace function reds.FN_POSTIMPRESSIONEVENTTYPETOPOSTIMPRESSIONEVENTTYPENAME(POSTIMPRESSIONEVENTTYPEID NUMBER) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                else to_varchar(postImpressionEventTypeId)
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_POSTIMPRESSIONEVENTTYPETOPOSTIMPRESSIONEVENTTYPENAME(NUMBER) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_POSTIMPRESSIONEVENTTYPETOPOSTIMPRESSIONEVENTTYPENAME(NUMBER) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_POSTIMPRESSIONEVENTTYPETOPOSTIMPRESSIONEVENTTYPENAME(NUMBER) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_POSTIMPRESSIONEVENTTYPETOPOSTIMPRESSIONEVENTTYPENAME(NUMBER) to role SYSADMIN with grant option;',
        ], "when POSTIMPRESSIONEVENTTYPEID = {item1} then '{item2}'"
    ),
    SyncTemplate(
        'FN_VIDEOPLAYBACKTYPENAMETOVIDEOPLAYBACKTYPEID(VARCHAR)',
        "SELECT VideoPlaybackTypeName, VideoPlaybackTypeId FROM dbo.VideoPlaybackType;", [
            '''
                create or replace function reds.FN_VIDEOPLAYBACKTYPENAMETOVIDEOPLAYBACKTYPEID(VIDEOPLAYBACKTYPENAME VARCHAR) RETURNS NUMBER
                    as
                        $$
                            case
                                {case_statements}
                                when VIDEOPLAYBACKTYPENAME = 'None' then 0
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_VIDEOPLAYBACKTYPENAMETOVIDEOPLAYBACKTYPEID(VARCHAR) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_VIDEOPLAYBACKTYPENAMETOVIDEOPLAYBACKTYPEID(VARCHAR) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_VIDEOPLAYBACKTYPENAMETOVIDEOPLAYBACKTYPEID(VARCHAR) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_VIDEOPLAYBACKTYPENAMETOVIDEOPLAYBACKTYPEID(VARCHAR) to role SYSADMIN with grant option;',
        ], "when VIDEOPLAYBACKTYPENAME = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_VIDEOPLAYERSIZEIDTOVIDEOPLAYERSIZE(NUMBER)', "SELECT VideoPlayerSizeId, VideoPlayerSizeName FROM dbo.VideoPlayerSize;", [
            '''
                create or replace function reds.FN_VIDEOPLAYERSIZEIDTOVIDEOPLAYERSIZE(VIDEOPLAYERSIZEID NUMBER) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                when VIDEOPLAYERSIZEID = 0 then 'Unused'
                                else to_varchar(VIDEOPLAYERSIZEID)
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_VIDEOPLAYERSIZEIDTOVIDEOPLAYERSIZE(NUMBER) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_VIDEOPLAYERSIZEIDTOVIDEOPLAYERSIZE(NUMBER) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_VIDEOPLAYERSIZEIDTOVIDEOPLAYERSIZE(NUMBER) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_VIDEOPLAYERSIZEIDTOVIDEOPLAYERSIZE(NUMBER) to role SYSADMIN with grant option;',
        ], "when VIDEOPLAYERSIZEID = {item1} then '{item2}'"
    ),
    SyncTemplate(
        'FN_VIDEOPLAYERSIZETOVIDEOPLAYERSIZEID(VARCHAR)', "SELECT VideoPlayerSizeName, VideoPlayerSizeId FROM dbo.VideoPlayerSize;", [
            '''
                create or replace function reds.FN_VIDEOPLAYERSIZETOVIDEOPLAYERSIZEID(VIDEOPLAYERSIZE VARCHAR) RETURNS NUMBER
                    as
                        $$
                            case
                                {case_statements}
                                when VIDEOPLAYERSIZE = 'Unused' then 0
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_VIDEOPLAYERSIZETOVIDEOPLAYERSIZEID(VARCHAR) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_VIDEOPLAYERSIZETOVIDEOPLAYERSIZEID(VARCHAR) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_VIDEOPLAYERSIZETOVIDEOPLAYERSIZEID(VARCHAR) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_VIDEOPLAYERSIZETOVIDEOPLAYERSIZEID(VARCHAR) to role SYSADMIN with grant option;',
        ], "when VIDEOPLAYERSIZE = '{item1}' then {item2}"
    ),
    SyncTemplate(
        'FN_DIGITALOUTOFHOMEVENUETYPE(VARCHAR)', """
             ;WITH RecursiveVenueType AS
             (
                -- Base case: Select all rows where there is no parent
                SELECT
                    DigitalOutOfHomeVenueTypeId,
                    REPLACE(DigitalOutOfHomeVenueTypeName, '''', ' ') AS DigitalOutOfHomeVenueTypeName,
                    ParentId,
                    CONVERT(nvarchar(MAX), REPLACE(DigitalOutOfHomeVenueTypeName, '''', ' ')) AS FullName
                FROM
                    bidding.DigitalOutOfHomeVenueType
                WHERE
                    ParentId IS NULL

                UNION ALL

                -- Recursive case: Join to previous result where current row's ParentId matches previous row's Id
                SELECT
                    v.DigitalOutOfHomeVenueTypeId,
                    REPLACE(v.DigitalOutOfHomeVenueTypeName, '''', ' ') AS DigitalOutOfHomeVenueTypeName,
                    v.ParentId,
                    r.FullName + '>' + REPLACE(v.DigitalOutOfHomeVenueTypeName, '''', ' ') AS FullName
                FROM
                    bidding.DigitalOutOfHomeVenueType v
                INNER JOIN
                    RecursiveVenueType r
                    ON v.ParentId = r.DigitalOutOfHomeVenueTypeId
             )

             -- Final select from the recursive CTE
             SELECT
                DigitalOutOfHomeVenueTypeId as ID,
                FullName as NAME
             FROM
                RecursiveVenueType
             ORDER BY
                DigitalOutOfHomeVenueTypeId;
             """, [
            '''
                create or replace function reds.FN_DIGITALOUTOFHOMEVENUETYPE(VENUETYPEID VARCHAR) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_DIGITALOUTOFHOMEVENUETYPE(VARCHAR) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_DIGITALOUTOFHOMEVENUETYPE(VARCHAR) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_DIGITALOUTOFHOMEVENUETYPE(VARCHAR) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_DIGITALOUTOFHOMEVENUETYPE(VARCHAR) to role SYSADMIN with grant option;',
        ], "when VENUETYPEID = '{item1}' then '{item2}'"
    ),
    SyncTemplate(
        'FN_INVENTORYCHANNELTOINVENTORYCHANNELNAME(NUMBER)', "SELECT InventoryChannelId, Name FROM dbo.InventoryChannel;", [
            '''
                create or replace function reds.FN_INVENTORYCHANNELTOINVENTORYCHANNELNAME(INVENTORYCHANNEL NUMBER) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                else null
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_INVENTORYCHANNELTOINVENTORYCHANNELNAME(NUMBER) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_INVENTORYCHANNELTOINVENTORYCHANNELNAME(NUMBER) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_INVENTORYCHANNELTOINVENTORYCHANNELNAME(NUMBER) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_INVENTORYCHANNELTOINVENTORYCHANNELNAME(NUMBER) to role SYSADMIN with grant option;',
        ], "when INVENTORYCHANNEL = {item1} then '{item2}'"
    ),
    SyncTemplate(
        'FN_AUDIOFEEDIDTOAUDIOFEEDNAME(NUMBER)',
        "SELECT AudioFeedTypeId, AudioFeedTypeName FROM dbo.AudioFeedType where AudioFeedTypeId != 0;", [
            '''
                create or replace function reds.FN_AUDIOFEEDIDTOAUDIOFEEDNAME(AUDIOFEEDID NUMBER) RETURNS VARCHAR
                    as
                        $$
                            case
                                {case_statements}
                                when AUDIOFEEDID is null then null
                                else 'Other'
                            end
                        $$
                    ;
            ''',
            'grant usage on function reds.FN_AUDIOFEEDIDTOAUDIOFEEDNAME(NUMBER) to role REDS_EXTERNAL_VIEWS;',
            'grant usage on function reds.FN_AUDIOFEEDIDTOAUDIOFEEDNAME(NUMBER) to role REDS_READ_ONLY;',
            'grant usage on function reds.FN_AUDIOFEEDIDTOAUDIOFEEDNAME(NUMBER) to role SNOWFLAKE_BATCH_LOADER_ROLE;',
            'grant usage on function reds.FN_AUDIOFEEDIDTOAUDIOFEEDNAME(NUMBER) to role SYSADMIN with grant option;',
        ], "when AUDIOFEEDID = {item1} then '{item2}'"
    ),
]


def sync_functions(**context):
    print('Starting sync functions in dry_run:', dry_run)

    sql_hook = MsSqlHook(mssql_conn_id=provisioning_conn_id)
    sql_conn = sql_hook.get_conn()
    sql_cursor = sql_conn.cursor()

    sf_hook = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id, warehouse=snowflake_warehouse, database=snowflake_database, schema=snowflake_schema
    )
    sf_conn = sf_hook.get_conn()
    sf_cursor = sf_conn.cursor()

    for sync_template in sync_templates:
        print('Sync template: ', sync_template.function_name)
        sql_cursor.execute(sync_template.get_mapping_query)
        cases = '\n'.join([sync_template.case_expr_template.format(item1=x[0], item2=x[1]) for x in sql_cursor.fetchall()])

        for query in sync_template.create_mapping_query:
            query = query.format(case_statements=cases)
            print('Snowflake query to run: ', query)

            if not dry_run:
                sf_cursor.execute(query)


sync_functions = OpTask(op=PythonOperator(dag=adag, task_id='sync_functions', python_callable=sync_functions, provide_context=True))

sync_functions
