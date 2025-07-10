import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import snowflake.connector
from dags.fineng.edw.utils.helper import bulk_insert, merge_data, write_df, write_to_execution_logs
import pytz
import logging
""" 
"statusCategory": {
                                "self": "https://thetradedesk.atlassian.net/rest/api/2/statuscategory/4",
                                "id": 4,
                                "key": "indeterminate",
                                "colorName": "yellow",
                                "name": "In Progress"
                            }

"""

cols = [
    "IssueType", "Project", "Key", "Summary", "Status", "ParentID", "ParentLink", "ParentKey", "CreatedDate", "UpdatedDate", "ResolvedDate",
    "StartDate", "EndDate", "DueDate", "Description", "DocumentationLink", "Assignee", "AssigneeEmail", "OrderOfImportance",
    "StatusCategoryID", "StatusCategoryKey", "StatusCategoryColorName", "StatusCategoryName", "PriorityName", "labels"
]

IssueType = "Theme"
Project = "TTD Planning: Themes, Initiatives, and Milestones"

rows = []

payload = {}
headers = {'Accept': 'application/json'}


def fetch_total_count(url):
    try:
        response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
        res = response.json()
        return res['total']
    except Exception as e:
        logging.error(f"Error in fetching total count {e}")
        raise e


def get_themes(full_load, latest_update_timestamp):
    try:
        start_at = 0
        if full_load == 0:
            jql = f"issuetype  = Theme AND   project = \"TTD Planning: Themes, Initiatives, and Milestones\" AND updated >=\"{latest_update_timestamp}\""
        else:
            jql = "issuetype  = Theme AND   project = \"TTD Planning: Themes, Initiatives, and Milestones\""

        url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?jql={jql}&startAt={start_at}&maxResults=500&validateQuery=strict"
        total_keys = fetch_total_count(url)
        logging.info(f"Total keys to fetch Themes: {total_keys}")
        while start_at < total_keys:
            try:
                response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
                res = response.json()
                for issue in res.get('issues', []):  # Check if 'issues' exists
                    key = issue.get('key', '')
                    summary = issue['fields'].get('summary', None)
                    status = issue['fields'].get('status', {}).get('name', None)

                    created_date = issue['fields'].get('created', '')
                    # convert created_date to utc timezone
                    created_date = datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.%f%z') if created_date else None
                    updated_date = issue['fields'].get('updated', None)
                    updated_date = datetime.strptime(updated_date, '%Y-%m-%dT%H:%M:%S.%f%z') if updated_date else None
                    description = issue['fields'].get('description', None)
                    # documentation_link -> customfield_10146
                    documentation_link = issue['fields'].get('customfield_100146', None)
                    assignee_obj = issue['fields'].get('assignee', {})
                    assignee = assignee_obj.get('displayName', '') if assignee_obj else None
                    assignee_email = assignee_obj.get('emailAddress', '') if assignee_obj else None
                    #

                    resolved_date = issue['fields'].get('resolutiondate', '')
                    resolved_date = datetime.strptime(resolved_date, '%Y-%m-%dT%H:%M:%S.%f%z') if resolved_date else None
                    due_date = issue['fields'].get('duedate', '')
                    #
                    due_date = datetime.strptime(due_date, '%Y-%m-%d').date() if due_date else None
                    """ 
                    startdate,enddate -> 2025-12-31
                    """

                    # enddate -> customfield_10054
                    endDate = issue['fields'].get('customfield_10054', '')
                    endDate = datetime.strptime(endDate, '%Y-%m-%d').date() if endDate else None

                    # startdate -> customfield_10133
                    startdate = issue['fields'].get('customfield_10133', '')

                    startdate = datetime.strptime(startdate, '%Y-%m-%d').date() if startdate else None
                    parent = issue['fields'].get('parent', {})
                    parent_id = parent.get('id', None) if parent else None
                    parent_link = parent.get('self', None) if parent else None
                    parent_key = parent.get('key', None) if parent else None

                    # Order -> customfield_10859
                    OrderOfImportance = issue['fields'].get('customfield_10859', None)

                    Status = issue['fields'].get('status', {})
                    statusCategory = Status.get('statusCategory', {})
                    StatusCategory_ID = statusCategory.get('id', '') if statusCategory else None
                    StatusCategory_Key = statusCategory.get('key', '') if statusCategory else None
                    StatusCategory_ColorName = statusCategory.get('colorName', '') if statusCategory else None
                    StatusCategory_Name = statusCategory.get('name', '') if statusCategory else None

                    priority = issue['fields'].get('priority', {})
                    PriorityName = priority.get('name', '') if priority else None

                    labels = issue['fields'].get('labels', None)
                    labels = '$;;$'.join(labels) if labels else None

                    row = [
                        IssueType, Project, key, summary, status, parent_id, parent_link, parent_key, created_date, updated_date,
                        resolved_date, startdate, endDate, due_date, description, documentation_link, assignee, assignee_email,
                        OrderOfImportance, StatusCategory_ID, StatusCategory_Key, StatusCategory_ColorName, StatusCategory_Name,
                        PriorityName, labels
                    ]
                    rows.append(row)
                start_at += res['maxResults']
                url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?jql={jql}&startAt={start_at}&maxResults=500&validateQuery=strict"
            except Exception as e:
                msg = f"Skipping this record {key} due to error {e} "
                logging.error(msg)
                continue
    except Exception as e:
        logging.error(f"Error in fetching themes {e}")
        raise e


def populate_themes(snowflakeconn, jira_username, jira_password, full_load):
    try:
        # full load 1 -> Truncate and load
        # full load 0 -> Incremental load

        logging.info(len(cols))
        global username
        global password
        username = jira_username
        password = jira_password

        start_time = datetime.now()

        process_name = "MAIN-EDW_Jira_Planning_Themes_To_EDWStaging"
        process_id = "MAIN-EDW_Jira_ProjectWhy_To_EDWStaging"

        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            import pandas as pd

            # fetch_latest_update_date for themes
            latest_update_timestamp = """
            SELECT 
            CONVERT_TIMEZONE('UTC', 'America/New_York', MAX(updateddate)::TIMESTAMP_NTZ) AS max_updated_date_eastern
            FROM 
            EDWSTAGING.JIRA.PLANNING_THEMES;
            """
            latest_update_timestamp = connsnowflake.cursor().execute(latest_update_timestamp).fetchone()[0]
            if latest_update_timestamp is None:
                full_load = 1
            # typecast latestupate_timestamp to yyyy-MM-dd HH:mm, remove timezone info as well
            else:
                naive_timestamp = latest_update_timestamp.replace(tzinfo=None)
                latest_update_timestamp = datetime.strftime(naive_timestamp, '%Y-%m-%d %H:%M').replace(' ', '%20')
                logging.info(f"Latest update timestamp for themes: {latest_update_timestamp}")
            ###
            full_load = 1
            get_themes(full_load, latest_update_timestamp)
            df = pd.DataFrame(rows, columns=cols)

            df.columns = df.columns.str.upper()

            if len(df) == 0:
                logging.info("No data to insert")
                return

            # df['loaddate'] to utc timezone current timestamp
            load_date = datetime.now(pytz.utc)
            # use est timezone
            df['LOADDATE'] = datetime.now(pytz.timezone('US/Eastern'))
            df['IsCurrent'] = 1
            df['row_hash'] = ""

            logging.info(df.columns)
            logging.info(df.head())
            # connect to snowflake

            #use_logical_types=True
            source_db = 'EDWSTAGING'
            source_schema = 'STAGING_TEMP'
            source_table = 'PLANNING_THEMES_STAGING'
            #
            target_db = 'EDWSTAGING'
            target_schema = 'JIRA'
            target_table = 'PLANNING_THEMES'
            #
            trunc_stage = f"TRUNCATE TABLE {source_db}.{source_schema}.{source_table}"
            connsnowflake.cursor().execute(trunc_stage)
            # res = write_pandas(conn=connsnowflake, df=df, table_name=table, database=db, schema=schema, use_logical_type=True)
            # print(res)
            # bulk insert

            df.columns = df.columns.str.upper()
            #date_columns = ['RESOLVEDDATE', 'STARTDATE', 'ENDDATE', 'DUEDATE', 'CREATEDDATE', 'UPDATEDDATE', 'LOADDATE']
            # for col in date_columns:
            #     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            # df.fillna(value={col: None for col in df.columns}, inplace=True)

            if write_df(conn=connsnowflake, df=df, table=source_table, db=source_db, schema=source_schema):

                # execute stored procedure
                proc_sql = "call edwstaging.jira.prc_planning_themes_history();"
                connsnowflake.cursor().execute(proc_sql)

                # if full_load == 1:
                #     trunc_table = "TRUNCATE TABLE EDWSTAGING.JIRA.PLANNING_THEMES"
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
                write_to_execution_logs(
                    conn=connsnowflake,
                    processname=process_name,
                    processid=process_id,
                    processstatus='SUCCESS',
                    processstarttime=start_time,
                    processendtime=endtime,
                    message="Successfully loaded data to Snowflake"
                )
                return True

            else:
                message = "Error in Populating Themes"
                endtime = datetime.now()
                write_to_execution_logs(
                    conn=connsnowflake,
                    processname=process_name,
                    processid=process_id,
                    processstatus='FAILURE',
                    processstarttime=start_time,
                    processendtime=endtime,
                    message=message
                )
                #raise Exception("Error in writing data to staging table", e)
    except Exception as e:
        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            message = "Error in Populating Themes" + str(e).replace(",", "")
            logging.info(message)
            endtime = datetime.now()
            write_to_execution_logs(
                conn=connsnowflake,
                processname=process_name,
                processid=process_id,
                processstatus='FAILURE',
                processstarttime=start_time,
                processendtime=endtime,
                message=message
            )
            logging.error(f"Error in populating themes {e}")
            raise e