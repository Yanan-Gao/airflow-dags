import requests
from requests.auth import HTTPBasicAuth
import json
import snowflake.connector
from dags.fineng.edw.utils.helper import bulk_insert, merge_data, write_df, write_to_execution_logs
import pytz
from datetime import datetime
import logging
import time

cols = [
    "IssueType", "Project", "ProjectCategory", "Key", "Epiclink", "EpicName", "Summary", "Status", "ParentID", "ParentLink", "ParentKey",
    "TechnologyComponent", "CreatedDate", "UpdatedDate", "ResolvedDate", "StartDate", "EndDate", "DueDate", "Description",
    "DocumentationLink", "Assignee", "AssigneeEmail", "EpicStoryPoints", "StatusCategoryID", "StatusCategoryKey", "StatusCategoryColorName",
    "StatusCategoryName", "PriorityName", "OfficialStoryPointEstimate", "OriginalStoryPointEstimate", "StoryPoints", "Reporter",
    "Resolution", "Sprint", "OrderOfImportance", "Ranking", "DocStatus", "Audience", "UXMOCK", "UXMockStatus", "labels"
]

fields = ",".join([
    "issuetype", "project", "key", "customfield_10014", "customfield_10011", "summary", "status", "parent", "customfield_10057", "created",
    "updated", "resolutiondate", "customfield_10133", "customfield_10054", "duedate", "description", "customfield_10146", "assignee",
    "customfield_10115", "statusCategory", "priority", "labels", "customfield_10091", "customfield_10101", "customfield_10100", "reporter",
    "resolution", "customfield_10020", "customfield_10859", "customfield_10549", "customfield_10546", "customfield_10550",
    "customfield_10543", "customfield_10551"
])

rows = []

payload = {}
headers = {'Accept': 'application/json'}


def get_issues(full_load, latest_update_timestamp, snowflake_conn):
    import pandas as pd

    global rows

    if full_load == 0:
        jql_query = f"updated >= \"{latest_update_timestamp}\""
    else:
        jql_query = f""
    #logging.info(f"JQL Query: {jql_query}")
    url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?jql={jql_query}&startAt=0&maxResults=500&validateQuery=strict"
    ## Fetching Total Count
    response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
    res = response.json()
    total_count = res['total']

    ## Fetching Issues
    start_at = 0
    url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?fields={fields}&jql={jql_query}&startAt={start_at}&maxResults=500&validateQuery=strict"
    logging.info("-----------------------------")
    logging.info(f"JQL Query: {jql_query}")

    total_keys = total_count
    logging.info(f"Total keys to fetch Issues: {total_keys}")

    while start_at < total_keys:
        response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
        res = response.json()
        max_results = res['maxResults']
        for issue in res.get('issues', []):
            try:
                IssueType = issue['fields'].get('issuetype', {})
                IssueType = IssueType.get('name', None) if IssueType else None
                ###

                Project = issue['fields'].get('project', {})
                Project = Project.get('name', None) if Project else None
                ####

                ProjectCategory = issue['fields'].get('project', {})
                ProjectCategory = ProjectCategory.get('projectCategory', {}) if ProjectCategory else None
                ProjectCategory = ProjectCategory.get('name', None) if ProjectCategory else None
                #####

                Key = issue.get('key', None)
                Summary = issue['fields'].get('summary', None)
                Description = issue['fields'].get('description', None)
                Labels = issue['fields'].get('labels', None)
                labels = "$;;$".join(Labels) if Labels else None
                ######

                Status = issue['fields'].get('status', {})
                Status = Status.get('name', None) if Status else None
                ######
                PriorityName = issue['fields'].get('priority', {})
                PriorityName = PriorityName.get('name', None) if PriorityName else None
                ###
                Resolution = issue['fields'].get('resolution', {})
                Resolution = Resolution.get('name', None) if Resolution else None
                #######
                Sprint = issue['fields'].get('customfield_10020', None)
                Sprint = ''.join(s.get('name', '') for s in Sprint if s.get('state', '') == 'active') if Sprint else None

                # Parent fields
                Parent = issue['fields'].get('parent', {})
                ParentID = Parent.get('id', None) if Parent else None
                ParentLink = Parent.get('self', None) if Parent else None
                ParentKey = Parent.get('key', None) if Parent else None

                # Assignee fields
                AssigneeData = issue['fields'].get('assignee', {})
                Assignee = AssigneeData.get('displayName', None) if AssigneeData else None
                AssigneeEmail = AssigneeData.get('emailAddress', None) if AssigneeData else None

                # Reporter
                Reporter = issue['fields'].get('reporter', {})
                Reporter = Reporter.get('displayName', None) if Reporter else None

                ###

                # Epic-related fields
                Epiclink = issue['fields'].get('customfield_10014', None)
                EpicName = issue['fields'].get('customfield_10011', None)
                EpicStoryPoints = issue['fields'].get('customfield_10115', None)
                OfficialStoryPointEstimate = issue['fields'].get('customfield_10091', None)
                OriginalStoryPointEstimate = issue['fields'].get('customfield_10101', None)
                StoryPoints = issue['fields'].get('customfield_10100', None)

                # Custom fields
                DocumentationLink = issue['fields'].get('customfield_10146', None)
                ##
                TechnologyComponent = issue['fields'].get('customfield_10057', {})
                TechnologyComponent = TechnologyComponent.get('value', None) if TechnologyComponent else None

                ##
                OrderOfImportance = issue['fields'].get('customfield_10859', None)
                Ranking = issue['fields'].get('customfield_10549', None)
                DocStatus = issue['fields'].get('customfield_10546', {})
                DocStatus = DocStatus.get('value', None) if DocStatus else None
                ###
                Audience = issue['fields'].get('customfield_10550', {})
                Audience = Audience.get('value', None) if Audience else None
                ##
                UXMOCK = issue['fields'].get('customfield_10543', None)
                UXMockStatus = issue['fields'].get('customfield_10551', {})
                UXMockStatus = UXMockStatus.get('value', None) if UXMockStatus else None
                ###

                # Dates with conversion
                CreatedDate = issue['fields'].get('created', None)
                CreatedDate = datetime.strptime(CreatedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if CreatedDate else None

                UpdatedDate = issue['fields'].get('updated', None)
                UpdatedDate = datetime.strptime(UpdatedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if UpdatedDate else None

                ResolvedDate = issue['fields'].get('resolutiondate', None)
                ResolvedDate = datetime.strptime(ResolvedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if ResolvedDate else None

                DueDate = issue['fields'].get('duedate', None)
                DueDate = datetime.strptime(DueDate, '%Y-%m-%d').date() if DueDate else None

                StartDate = issue['fields'].get('customfield_10133', None)
                StartDate = datetime.strptime(StartDate, '%Y-%m-%d').date() if StartDate else None

                EndDate = issue['fields'].get('customfield_10054', None)
                EndDate = datetime.strptime(EndDate, '%Y-%m-%d').date() if EndDate else None

                # Status category
                StatusCategory = issue['fields'].get('status', {})
                StatusCategory = StatusCategory.get('statusCategory', {}) if StatusCategory else None
                StatusCategoryID = StatusCategory.get('id', None) if StatusCategory else None
                StatusCategoryKey = StatusCategory.get('key', None) if StatusCategory else None
                StatusCategoryColorName = StatusCategory.get('colorName', None) if StatusCategory else None
                StatusCategoryName = StatusCategory.get('name', None) if StatusCategory else None
                row = [
                    IssueType, Project, ProjectCategory, Key, Epiclink, EpicName, Summary, Status, ParentID, ParentLink, ParentKey,
                    TechnologyComponent, CreatedDate, UpdatedDate, ResolvedDate, StartDate, EndDate, DueDate, Description,
                    DocumentationLink, Assignee, AssigneeEmail, EpicStoryPoints, StatusCategoryID, StatusCategoryKey,
                    StatusCategoryColorName, StatusCategoryName, PriorityName, OfficialStoryPointEstimate, OriginalStoryPointEstimate,
                    StoryPoints, Reporter, Resolution, Sprint, OrderOfImportance, Ranking, DocStatus, Audience, UXMOCK, UXMockStatus, labels
                ]

                rows.append(row)
            except Exception as e:
                msg = f"Skipping this record {Key} due to error {e} "
                print(msg)
                continue
        start_at += res['maxResults']
        url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?fields={fields}&jql={jql_query}&startAt={start_at}&maxResults={max_results}&validateQuery=strict"
        print(len(rows))
        if len(rows) % 20000 == 0:
            df = pd.DataFrame(rows, columns=cols)
            df['LOADDATE'] = datetime.now(pytz.utc)
            df['IsCurrent'] = 1
            df['row_hash'] = ""
            df.columns = df.columns.str.upper()
            #use_logical_types=True
            source_db = 'EDWSTAGING'
            source_schema = 'STAGING_TEMP'
            source_table = 'MASTER_TICKETS_STAGING'
            df.columns = df.columns.str.upper()
            res = write_df(conn=snowflake_conn, df=df, table=source_table, db=source_db, schema=source_schema)
            logging.info(res)
            rows = []

    if len(rows) > 0:
        df = pd.DataFrame(rows, columns=cols)
        df['LOADDATE'] = datetime.now(pytz.utc)
        df['IsCurrent'] = 1
        df['row_hash'] = ""
        df.columns = df.columns.str.upper()
        #use_logical_types=True
        source_db = 'EDWSTAGING'
        source_schema = 'STAGING_TEMP'
        source_table = 'MASTER_TICKETS_STAGING'
        df.columns = df.columns.str.upper()
        res = write_df(conn=snowflake_conn, df=df, table=source_table, db=source_db, schema=source_schema)
        logging.info(res)
        rows = []


def populate_issues(snowflakeconn, jira_username, jira_password, full_load):
    try:
        global username
        global password

        username = jira_username
        password = jira_password

        start_time = datetime.now()

        process_name = "MAIN-EDW_Jira_Master_Issues_To_EDWStaging"
        process_id = "MAIN-EDW_Jira_Master_To_EDWStaging"

        source_db = 'EDWSTAGING'
        source_schema = 'STAGING_TEMP'
        source_table = 'MASTER_TICKETS_STAGING'

        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            trunc_stage = f"TRUNCATE TABLE {source_db}.{source_schema}.{source_table}"
            connsnowflake.cursor().execute(trunc_stage)

            full_load = 0
            # fetch_row_counts
            logging.info(connsnowflake)
            latest_update_timestamp = """
            SELECT 
            CONVERT_TIMEZONE('UTC', 'America/New_York', MAX(updateddate)::TIMESTAMP_NTZ) AS max_updated_date_eastern
            FROM 
            EDWSTAGING.JIRA.MASTER_TICKETS
            """
            latest_update_timestamp = connsnowflake.cursor().execute(latest_update_timestamp).fetchone()[0]
            if latest_update_timestamp is None:
                full_load = 1
            # typecast latestupate_timestamp to yyyy-MM-dd HH:mm, remove timezone info as well
            else:
                naive_timestamp = latest_update_timestamp.replace(tzinfo=None)
                latest_update_timestamp = datetime.strftime(naive_timestamp, '%Y-%m-%d %H:%M').replace(' ', '%20')
                logging.info(f"Latest update timestamp for themes: {latest_update_timestamp}")
            ####
            logging.info("Fetching all epic keys")
            #full_load = 1
            logging.info(f"Full Load: {full_load}")
            get_issues(full_load, latest_update_timestamp, connsnowflake)
            proc_sql = "CALL EDWSTAGING.JIRA.PRC_MASTER_TICKETS_HISTORY()"
            logging.info("Calling stored procedure")
            connsnowflake.cursor().execute(proc_sql)

            #     proc_sql = "CALL EDWSTAGING.JIRA.PRC_PLANNING_ISSUES_HISTORY()"
            #     connsnowflake.cursor().execute(proc_sql)
            # if full_load == 1:
            #     trunc_table = "TRUNCATE TABLE EDWSTAGING.JIRA.PLANNING_ISSUES"
            #     connsnowflake.cursor().execute(trunc_table)

            # merge_condition = f"source.KEY = target.KEY"
            # update_cols = df.columns.tolist()

            # update_columns_when_match = [f"target.{col} = source.{col}" for col in update_cols]
            # update_columns_when_not_match = [f"source.{col}" for col in update_cols]

            # merge_data(
            #     conn=connsnowflake,
            #     source_table=source_table,
            #     source_schema=source_schema,
            #     source_db=source_db,
            #     target_table=target_table,
            #     target_schema=target_schema,
            #     target_db=target_db,
            #     merge_condition=merge_condition,
            #     update_columns_when_match=update_columns_when_match,
            #     update_columns_when_not_match=update_columns_when_not_match,
            #     update_columns=update_cols
            # )
            #     endtime = datetime.now()
            #     message = "Issues data populated successfully"
            #     write_to_execution_logs(
            #         conn=connsnowflake,
            #         processname=process_name,
            #         processid=process_id,
            #         processstatus='SUCCESS',
            #         processstarttime=start_time,
            #         processendtime=endtime,
            #         message=message
            #     )
            #     return True
            # else:
            #     endtime = datetime.now()
            #     message = "Error in populating Issues"
            #     write_to_execution_logs(
            #         conn=connsnowflake,
            #         processname=process_name,
            #         processid=process_id,
            #         processstatus='FAILURE',
            #         processstarttime=start_time,
            #         processendtime=endtime,
            #         message=message
            #     )
            #     #raise Exception("Error in writing data to Snowflake", e)
    except Exception as e:
        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            logging.error(f"Error in populating Issues {e}")
            endtime = datetime.now()
            message = "Error in populating Issues" + str(e).replace(",", "")
            write_to_execution_logs(
                conn=connsnowflake,
                processname=process_name,
                processid=process_id,
                processstatus='FAILURE',
                processstarttime=start_time,
                processendtime=endtime,
                message=message
            )
            raise e
