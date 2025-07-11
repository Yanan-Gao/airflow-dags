import requests
from requests.auth import HTTPBasicAuth
import json
import snowflake.connector
from dags.fineng.edw.utils.helper import bulk_insert, merge_data, write_df, write_to_execution_logs
import pytz
from datetime import datetime
import logging

cols = [
    "IssueType", "Project", "Key", "Epiclink", "EpicName", "Summary", "Status", "ParentID", "ParentLink", "ParentKey",
    "OfficialStoryPointEstimate", "OriginalStoryPointEstimate", "StoryPoints", "Assignee", "AssigneeEmail", "Reporter", "Resolution",
    "CreatedDate", "UpdatedDate", "ResolvedDate", "Sprint", "DueDate", "Description", "DocumentationLink", "StartDate", "EndDate",
    "StatusCategoryID", "StatusCategoryKey", "StatusCategoryColorName", "StatusCategoryName", "PriorityName", "Labels"
]
fields = ",".join([
    "key", "issuetype", "project", "customfield_10014", "customfield_10011", "summary", "status", "parent", "customfield_10091",
    "customfield_10101", "customfield_10100", "assignee", "reporter", "resolution", "created", "updated", "resolutiondate",
    "customfield_10020", "duedate", "description", "customfield_10146", "customfield_10133", "customfield_10054", "status", "priority",
    "labels"
])

rows = []

payload = {}
headers = {'Accept': 'application/json'}


def get_issues(epic_sets, full_load, latest_update_timestamp):
    for epic_set in epic_sets:
        if full_load == 0:
            jql_query = f"parentEpic in ({', '.join([f'{epic}' for epic in epic_set])}) and issuetype != Epic AND updated >= \"{latest_update_timestamp}\""
        else:
            jql_query = f"parentEpic in ({', '.join([f'{epic}' for epic in epic_set])}) and issuetype != Epic"
        #logging.info(f"JQL Query: {jql_query}")
        url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?jql={jql_query}&startAt=0&maxResults=500&validateQuery=strict"
        ## Fetching Total Count
        response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
        res = response.json()
        total_count = res['total']

        ## Fetching Issues
        start_at = 0
        url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?fields={fields}&jql={jql_query}&startAt={start_at}&maxResults=500&validateQuery=strict"
        logging.info(f"Fetching Issues for Epic Set: {epic_set}")
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
                    IssueType = IssueType.get('name', '') if IssueType else None
                    Project_Data = issue['fields'].get('project', {})
                    Project = Project_Data.get('name', '') if Project_Data else None
                    Key = issue.get('key', '')
                    # EpicLink -> customfield_10014
                    Epiclink = issue['fields'].get('customfield_10014', None)
                    # EpicName -> customfield_10011
                    EpicName = issue.get('fields', {}).get('customfield_10011', None)
                    Summary = issue['fields'].get('summary', '')
                    Status = issue['fields'].get('status', {})
                    Status = Status.get('name', '') if Status else None
                    Parent = issue['fields'].get('parent', {})
                    ParentID = Parent.get('id', '') if Parent else None
                    ParentLink = Parent.get('self', '') if Parent else None
                    ParentKey = Parent.get('key', '') if Parent else None
                    # Story Points Estimate -> customfield_10091
                    OfficialStoryPointEstimate = issue['fields'].get('customfield_10091', None)

                    # Original Story Points Estimate -> customfield_10101
                    OriginalStoryPointEstimate = issue['fields'].get('customfield_10101', None)
                    # storypoints -> customfield_10100
                    StoryPoints = issue['fields'].get('customfield_10100', None)

                    Assignee_data = issue['fields'].get('assignee', {})
                    Assignee = Assignee_data.get('displayName', '') if Assignee_data else None
                    AssigneeEmail = Assignee_data.get('emailAddress', '') if Assignee_data else None

                    Reporter = issue['fields'].get('reporter', {})
                    Reporter = Reporter.get('displayName', '') if Reporter else None
                    Resolution_data = issue['fields'].get('resolution', {})
                    Resolution = Resolution_data.get('name', '') if Resolution_data else None
                    CreatedDate = issue['fields'].get('created', '')
                    #
                    CreatedDate = datetime.strptime(CreatedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if CreatedDate else None

                    UpdatedDate = issue['fields'].get('updated', '')
                    #
                    UpdatedDate = datetime.strptime(UpdatedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if UpdatedDate else None
                    #
                    ResolvedDate = issue['fields'].get('resolutiondate', '')
                    #
                    ResolvedDate = datetime.strptime(ResolvedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if ResolvedDate else None

                    # sprint -> customfield_10020
                    Sprint = issue['fields'].get('customfield_10020', None)
                    if Sprint:
                        sp_name = ""
                        for s in Sprint:
                            if s.get('state', '') == 'active':
                                sp_name += s.get('name', '')
                        Sprint = sp_name

                    DueDate = issue['fields'].get('duedate', None)
                    #
                    DueDate = datetime.strptime(DueDate, '%Y-%m-%d').date() if DueDate else None
                    #
                    Description = issue['fields'].get('description', None)
                    # documentation link -> customfield_10146
                    DocumentationLink = issue['fields'].get('customfield_10146', None)

                    # startdate -> customfield_10133
                    StartDate = issue['fields'].get('customfield_10133', '')
                    #
                    StartDate = datetime.strptime(StartDate, '%Y-%m-%d').date() if StartDate else None
                    #
                    # enddate -> customfield_10054
                    EndDate = issue['fields'].get('customfield_10054', '')
                    #
                    EndDate = datetime.strptime(EndDate, '%Y-%m-%d').date() if EndDate else None

                    status = issue['fields'].get('status', {})
                    statusCategory = status.get('statusCategory', {})
                    StatusCategory_ID = statusCategory.get('id', '') if statusCategory else None
                    StatusCategory_Key = statusCategory.get('key', '') if statusCategory else None
                    StatusCategory_ColorName = statusCategory.get('colorName', '') if statusCategory else None
                    StatusCategory_Name = statusCategory.get('name', '') if statusCategory else None

                    priority = issue['fields'].get('priority', {})
                    PriorityName = priority.get('name', '') if priority else None

                    Labels = issue['fields'].get('labels', None)
                    Labels = '$;;$'.join(Labels) if Labels else None

                    row = [
                        IssueType, Project, Key, Epiclink, EpicName, Summary, Status, ParentID, ParentLink, ParentKey,
                        OfficialStoryPointEstimate, OriginalStoryPointEstimate, StoryPoints, Assignee, AssigneeEmail, Reporter, Resolution,
                        CreatedDate, UpdatedDate, ResolvedDate, Sprint, DueDate, Description, DocumentationLink, StartDate, EndDate,
                        StatusCategory_ID, StatusCategory_Key, StatusCategory_ColorName, StatusCategory_Name, PriorityName, Labels
                    ]
                    rows.append(row)
                except Exception as e:
                    msg = f"Skipping this record {Key} due to error {e} "
                    print(msg)
                    continue
            start_at += res['maxResults']
            url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?fields={fields}&jql={jql_query}&startAt={start_at}&maxResults={max_results}&validateQuery=strict"
            print(len(rows))


def populate_issues(snowflakeconn, jira_username, jira_password, full_load):
    try:
        global username
        global password

        username = jira_username
        password = jira_password

        start_time = datetime.now()

        process_name = "MAIN-EDW_Jira_Planning_Issues_To_EDWStaging"
        process_id = "MAIN-EDW_Jira_ProjectWhy_To_EDWStaging"

        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            import pandas as pd

            # fetch_row_counts
            logging.info(connsnowflake)
            latest_update_timestamp = """
            SELECT 
            CONVERT_TIMEZONE('UTC', 'America/New_York', MAX(updateddate)::TIMESTAMP_NTZ) AS max_updated_date_eastern
            FROM 
            EDWSTAGING.JIRA.PLANNING_ISSUES;
            
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
            get_all_epic_keys = """
            SELECT DISTINCT t.KEY FROM EDWSTAGING.JIRA.PLANNING_EPICS t
            left join EDWSTAGING.JIRA.STALE_EPICS s on t.key = s.stale_key
            where s.stale_key is null
            """
            epics = connsnowflake.cursor().execute(get_all_epic_keys).fetchall()
            logging.info(f"Total epics: {len(epics)}")
            epics = [epic[0] for epic in epics]
            epic_sets = [epics[i:i + 500] for i in range(0, len(epics), 500)]
            #####
            # overriding full_load to 1
            full_load = 1
            get_issues(epic_sets, full_load, latest_update_timestamp)
            df = pd.DataFrame(rows, columns=cols)

            # df['loaddate'] to utc timezone current timestamp
            df['LOADDATE'] = datetime.now(pytz.utc)
            df['IsCurrent'] = 1
            df['row_hash'] = ""
            # connect to snowflake

            df.columns = df.columns.str.upper()
            #use_logical_types=True
            source_db = 'EDWSTAGING'
            source_schema = 'STAGING_TEMP'
            source_table = 'PLANNING_ISSUES_STAGING'
            #
            target_db = 'EDWSTAGING'
            target_schema = 'JIRA'
            target_table = 'PLANNING_ISSUES'
            #
            trunc_stage = f"TRUNCATE TABLE {source_db}.{source_schema}.{source_table}"
            connsnowflake.cursor().execute(trunc_stage)
            # res = write_pandas(conn=connsnowflake, df=df, table_name=table, database=db, schema=schema, use_logical_type=True)
            # print(res)
            # bulk insert
            if len(df) == 0:
                logging.info("No data to insert")
                return
            df.columns = df.columns.str.upper()
            # date_columns = ['RESOLVEDDATE', 'STARTDATE', 'ENDDATE', 'DUEDATE', 'CREATEDDATE', 'UPDATEDDATE', 'LOADDATE']
            # for col in date_columns:
            #     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

            # df.fillna(value={col: None for col in df.columns}, inplace=True)

            if write_df(conn=connsnowflake, df=df, table=source_table, db=source_db, schema=source_schema):
                proc_sql = "CALL EDWSTAGING.JIRA.PRC_PLANNING_ISSUES_HISTORY()"
                connsnowflake.cursor().execute(proc_sql)
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
                endtime = datetime.now()
                message = "Issues data populated successfully"
                write_to_execution_logs(
                    conn=connsnowflake,
                    processname=process_name,
                    processid=process_id,
                    processstatus='SUCCESS',
                    processstarttime=start_time,
                    processendtime=endtime,
                    message=message
                )
                return True
            else:
                endtime = datetime.now()
                message = "Error in populating Issues"
                write_to_execution_logs(
                    conn=connsnowflake,
                    processname=process_name,
                    processid=process_id,
                    processstatus='FAILURE',
                    processstarttime=start_time,
                    processendtime=endtime,
                    message=message
                )
                #raise Exception("Error in writing data to Snowflake", e)
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
