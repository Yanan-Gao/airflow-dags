import requests
from requests.auth import HTTPBasicAuth
import json
import snowflake.connector
from dags.fineng.edw.utils.helper import bulk_insert, merge_data, write_df, write_to_execution_logs
import pytz
from datetime import datetime
import logging

cols = [
    "IssueType", "Project", "Key", "Summary", "Status", "ParentID", "ParentLink", "ParentKey", "CreatedDate", "UpdatedDate", "ResolvedDate",
    "StartDate", "EndDate", "DueDate", "Description", "DocumentationLink", "Assignee", "Assigneeemail", "OrderOfImportance",
    "StatusCategoryID", "StatusCategoryKey", "StatusCategoryColorName", "StatusCategoryName", "PriorityName", "Labels"
]

payload = {}
headers = {'Accept': 'application/json'}

url = "https://thetradedesk.atlassian.net/rest/api/latest/search?jql=issuetype  = Initiative AND   project = \"TTD Planning: Themes, Initiatives, and Milestones\"&startAt=0&maxResults=500&validateQuery=strict"

IssueType = "Initiative"
Project = "TTD Planning: Themes, Initiatives, and Milestones"

rows = []


def fetch_total_count(url):
    response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
    res = response.json()
    return res['total']


def get_initiatives(full_load, latest_update_timestamp):
    start_at = 0
    max_results = 500
    if full_load == 0:
        jql = f"issuetype  = Initiative AND   project = \"TTD Planning: Themes, Initiatives, and Milestones\" AND updated >=\"{latest_update_timestamp}\" "
    else:
        jql = "issuetype  = Initiative AND   project = \"TTD Planning: Themes, Initiatives, and Milestones\""
    url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?jql={jql}&startAt={start_at}&maxResults={max_results}&validateQuery=strict"
    total_keys = fetch_total_count(url)
    logging.info(f"Total keys to fetch Initiatives: {total_keys}")

    while start_at < total_keys:
        try:
            response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
            res = response.json()
            max_results = res['maxResults']
            #print(start_at)
            for issue in res.get('issues', []):  # Check if 'issues' exists
                key = issue.get('key', None)
                summary = issue['fields'].get('summary', None)
                status = issue['fields'].get('status', {}).get('name', None)
                created_date = issue['fields'].get('created', '')
                #
                created_date = datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.%f%z') if created_date else None
                updated_date = issue['fields'].get('updated', '')
                #
                updated_date = datetime.strptime(updated_date, '%Y-%m-%dT%H:%M:%S.%f%z') if updated_date else None

                description = issue['fields'].get('description', None)
                # Documentation Link -> customfield_10146
                documentation_link = issue['fields'].get('customfield_10146', None)
                assignee_data = issue['fields'].get('assignee', {})

                assignee = assignee_data.get('displayName', '') if assignee_data else None
                assignee_email = assignee_data.get('emailAddress', '') if assignee_data else None

                resolved_date = issue['fields'].get('resolutiondate', None)
                #
                resolved_date = datetime.strptime(resolved_date, '%Y-%m-%dT%H:%M:%S.%f%z') if resolved_date else None
                due_date = issue['fields'].get('duedate', '')
                # due date 2025-01-15'
                due_date = datetime.strptime(due_date, '%Y-%m-%d').date() if due_date else None
                #
                # # enddate -> customfield_10054
                endDate = issue['fields'].get('customfield_10054', '')
                #
                endDate = datetime.strptime(endDate, '%Y-%m-%d').date() if endDate else None

                # startdate -> customfield_10133
                startdate = issue['fields'].get('customfield_10133', '')
                #
                startdate = datetime.strptime(startdate, '%Y-%m-%d').date() if startdate else None
                parent = issue['fields'].get('parent', {})
                parent_id = parent.get('id', '') if parent else None
                parent_link = parent.get('self', '') if parent else None
                parent_key = parent.get('key', '') if parent else None

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

                Labels = issue['fields'].get('labels', None)
                Labels = "$;;$".join(Labels) if Labels else None

                row = [
                    IssueType, Project, key, summary, status, parent_id, parent_link, parent_key, created_date, updated_date, resolved_date,
                    startdate, endDate, due_date, description, documentation_link, assignee, assignee_email, OrderOfImportance,
                    StatusCategory_ID, StatusCategory_Key, StatusCategory_ColorName, StatusCategory_Name, PriorityName, Labels
                ]
                rows.append(row)
            start_at += res['maxResults']
            url = f"https://thetradedesk.atlassian.net/rest/api/latest/search?jql={jql}&startAt={start_at}&maxResults={max_results}&validateQuery=strict"
            print(len(rows))
        except Exception as e:
            msg = f"Skipping this record {key} due to error {e} "
            print(msg)
            continue


def populate_initiatives(snowflakeconn, jira_username, jira_password, full_load):
    try:
        global username
        global password

        username = jira_username
        password = jira_password

        start_time = datetime.now()

        process_name = "MAIN-EDW_Jira_Planning_Initiatives_To_EDWStaging"
        process_id = "MAIN-EDW_Jira_ProjectWhy_To_EDWStaging"

        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            import pandas as pd

            # fetch_row_counts
            latest_update_timestamp = """
            
            SELECT 
            CONVERT_TIMEZONE('UTC', 'America/New_York', MAX(updateddate)::TIMESTAMP_NTZ) AS max_updated_date_eastern
            FROM 
            EDWSTAGING.JIRA.PLANNING_INITIATIVES;
            """
            latest_update_timestamp = connsnowflake.cursor().execute(latest_update_timestamp).fetchone()[0]
            if latest_update_timestamp is None:
                full_load = 1
            # typecast latestupate_timestamp to yyyy-MM-dd HH:mm, remove timezone info as well
            else:
                naive_timestamp = latest_update_timestamp.replace(tzinfo=None)
                latest_update_timestamp = datetime.strftime(naive_timestamp, '%Y-%m-%d %H:%M').replace(' ', '%20')
                logging.info(f"Latest update timestamp for themes: {latest_update_timestamp}")
            full_load = 1
            get_initiatives(full_load, latest_update_timestamp)
            df = pd.DataFrame(rows, columns=cols)

            if len(df) == 0:
                logging.info("No data to insert")
                return

            df.columns = df.columns.str.upper()

            # df['loaddate'] to utc timezone current timestamp
            df['LOADDATE'] = datetime.now(pytz.utc)
            df['IsCurrent'] = 1
            df['row_hash'] = ""
            # connect to snowflake

            #use_logical_types=True
            source_db = 'EDWSTAGING'
            source_schema = 'STAGING_TEMP'
            source_table = 'PLANNING_INITIATIVES_STAGING'
            #
            target_db = 'EDWSTAGING'
            target_schema = 'JIRA'
            target_table = 'PLANNING_INITIATIVES'
            #
            trunc_stage = f"TRUNCATE TABLE {source_db}.{source_schema}.{source_table}"
            connsnowflake.cursor().execute(trunc_stage)
            # res = write_pandas(conn=connsnowflake, df=df, table_name=table, database=db, schema=schema, use_logical_type=True)
            # print(res)
            # bulk insert

            df.columns = df.columns.str.upper()
            if write_df(conn=connsnowflake, df=df, table=source_table, db=source_db, schema=source_schema):
                proc_sql = "CALL EDWSTAGING.JIRA.PRC_PLANNING_INITIATIVES_HISTORY()"
                connsnowflake.cursor().execute(proc_sql)
                # if full_load == 1:
                #     trunc_table = "TRUNCATE TABLE EDWSTAGING.JIRA.PLANNING_INITIATIVES"
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
                    message='Successfully loaded Initiatives'
                )
                return True
            else:
                message = "Error in Populating Initiatives"
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
                #raise Exception("Error in writing data to Snowflake", e)
    except Exception as e:
        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            message = "Error in Populating Initiatives" + str(e).replace(",", "")
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
            logging.error(f"Error in populating Initiatives {e}")
            raise e