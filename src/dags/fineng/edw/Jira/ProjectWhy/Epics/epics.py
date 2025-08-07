import requests
from requests.auth import HTTPBasicAuth
import json
import snowflake.connector
from dags.fineng.edw.utils.helper import bulk_insert, merge_data, write_df, write_to_execution_logs
from dags.fineng.edw.utils.jira_utils import extract_text_from_description
import pytz
from datetime import datetime
import logging

cols = [
    "IssueType", "Project", "Key", "EpicName", "Summary", "Status", "ParentID", "ParentLink", "ParentKey", "TechnologyComponent",
    "CreatedDate", "UpdatedDate", "ResolvedDate", "StartDate", "EndDate", "DueDate", "Description", "DocumentationLink", "Assignee",
    "AssigneeEmail", "EpicStoryPoints", "StatusCategoryID", "StatusCategoryKey", "StatusCategoryColorName", "StatusCategoryName",
    "PriorityName", "Labels"
]

payload = {}
headers = {'Accept': 'application/json'}
IssueType = "Epic"

rows = []


def fetch_total_count(url):
    response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
    res = response.json()
    return res['total']


def get_epics(full_load, latest_update_timestamp):
    if full_load == 0:
        jql = f"issueType = Epic and \"Parent Link\" is not empty AND updated >= \"{latest_update_timestamp}\""
    else:
        jql = "issueType = Epic and \"Parent Link\" is not empty"

    logging.info(f"JQL Query: {jql}")

    next_page_token = None
    total_processed = 0

    while True:
        # Build URL with nextPageToken if available
        if next_page_token:
            url = f"https://thetradedesk.atlassian.net/rest/api/3/search/jql?jql={jql}&nextPageToken={next_page_token}&maxResults=500&validateQuery=strict&fields=issuetype,project,key,summary,status,parent,created,updated,resolutiondate,customfield_10133,customfield_10054,duedate,description,customfield_10146,assignee,statusCategory,priority,labels,customfield_10859"
        else:
            url = f"https://thetradedesk.atlassian.net/rest/api/3/search/jql?jql={jql}&maxResults=500&validateQuery=strict&fields=issuetype,project,key,summary,status,parent,created,updated,resolutiondate,customfield_10133,customfield_10054,duedate,description,customfield_10146,assignee,statusCategory,priority,labels,customfield_10859"

        try:
            response = requests.get(url, headers=headers, data=payload, auth=HTTPBasicAuth(username, password))
            response.raise_for_status()  # Raise an exception for bad status codes
            res = response.json()

            # Check if the response has an error
            if 'errors' in res:
                logging.error(f"API returned errors: {res['errors']}")
                continue

            if 'issues' not in res:
                logging.error(f"API response missing 'issues' key. Response: {res}")
                continue

            for issue in res.get('issues', []):
                # Check if issue has fields
                if 'fields' not in issue:
                    logging.warning(f"Issue {issue.get('key', 'unknown')} missing 'fields' key. Skipping.")
                    continue

                Project_Data = issue['fields'].get('project', {})
                Project = Project_Data.get('name', '') if Project_Data else None
                Key = issue.get('key', '')
                #EpicName = issue.get('fields', {}).get('customfield_10011', '')
                EpicName = issue.get('fields', {}).get('customfield_10011', None)
                Summary = issue['fields'].get('summary', None)
                Status = issue['fields'].get('status', {})
                Status = Status.get('name', '') if Status else None
                Parent = issue['fields'].get('parent', {})
                ParentID = Parent.get('id', '') if Parent else None
                ParentLink = Parent.get('self', '') if Parent else None
                ParentKey = Parent.get('key', '') if Parent else None
                # TechnologyComponent = issue['fields'].get('customfield_10057', '')
                TechnologyComponentData = issue['fields'].get('customfield_10057', None)
                TechnologyComponent = TechnologyComponentData.get('value', '') if isinstance(TechnologyComponentData, dict) else None
                created_date = issue['fields'].get('created', '')
                # created date-> convert to utc timezone -> convert back to string
                created_date = datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.%f%z') if created_date else None

                updated = issue['fields'].get('updated', None)
                updated = datetime.strptime(updated, '%Y-%m-%dT%H:%M:%S.%f%z') if updated else None

                resolved_date = issue['fields'].get('resolutiondate', None)
                resolved_date = datetime.strptime(resolved_date, '%Y-%m-%dT%H:%M:%S.%f%z') if resolved_date else None

                # startdate -> customfield_10133
                startdate = issue['fields'].get('customfield_10133', None)
                startdate = datetime.strptime(startdate, '%Y-%m-%d').date() if startdate else None

                # enddate -> customfield_10054
                endDate = issue['fields'].get('customfield_10054', None)
                endDate = datetime.strptime(endDate, '%Y-%m-%d').date() if endDate else None

                due_date = issue['fields'].get('duedate', None)
                due_date = datetime.strptime(due_date, '%Y-%m-%d').date() if due_date else None

                description = issue['fields'].get('description', None)
                # Extract clean text from complex description objects
                if description and isinstance(description, dict):
                    description = extract_text_from_description(description)

                # Documentation Link -> customfield_10146
                documentation_link = issue['fields'].get('customfield_10146', None)

                assignee_data = issue['fields'].get('assignee', {})
                assignee = assignee_data.get('displayName', '') if assignee_data else None
                assignee_email = assignee_data.get('emailAddress', '') if assignee_data else None

                status_data = issue['fields'].get('status', {})
                status = status_data.get('name', None) if isinstance(status_data, dict) else None

                # Get statusCategory from the original status dict
                statusCategory = status_data.get('statusCategory', {}) if isinstance(status_data, dict) else {}
                StatusCategory_ID = statusCategory.get('id', '') if statusCategory else None
                StatusCategory_Key = statusCategory.get('key', '') if statusCategory else None
                StatusCategory_ColorName = statusCategory.get('colorName', '') if statusCategory else None
                StatusCategory_Name = statusCategory.get('name', '') if statusCategory else None

                # EpicStoryPoints = issue['fields'].get('customfield_10002?', '') -> Sum of Story Points (customfield_10043) or Story Points (customfield_10100)
                # customfield_10115
                EpicStoryPoints = issue['fields'].get('customfield_10115', None)  # StoryPoints Sum
                EpicStoryPoints = EpicStoryPoints if EpicStoryPoints else None

                priority = issue['fields'].get('priority', {})
                priortyName = priority.get('name', '') if priority else None

                Labels = issue['fields'].get('labels', None)
                Labels = '$;;$'.join(Labels) if Labels else None

                row = [
                    IssueType, Project, Key, EpicName, Summary, Status, ParentID, ParentLink, ParentKey, TechnologyComponent, created_date,
                    updated, resolved_date, startdate, endDate, due_date, description, documentation_link, assignee, assignee_email,
                    EpicStoryPoints, StatusCategory_ID, StatusCategory_Key, StatusCategory_ColorName, StatusCategory_Name, priortyName,
                    Labels
                ]
                rows.append(row)
                total_processed += 1

            # Log progress
            logging.info(f"Processed {total_processed} epics so far...")

            # Check if this is the last page
            if res.get('isLast', False):
                logging.info("Reached last page of results")
                break

            # Get next page token for next iteration
            next_page_token = res.get('nextPageToken')
            if not next_page_token:
                logging.info("No nextPageToken found, ending pagination")
                break

        except Exception as e:
            msg = f"Skipping this record {Key} due to error {e} "
            print(msg)
            continue

    logging.info(f"Total epics processed: {total_processed}")


def populate_epics(snowflakeconn, jira_username, jira_password, full_load):
    try:
        global username
        global password

        username = jira_username
        password = jira_password

        start_time = datetime.now()

        process_name = "MAIN-EDW_Jira_Planning_Epics_To_EDWStaging"
        process_id = "MAIN-EDW_Jira_ProjectWhy_To_EDWStaging"

        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            import pandas as pd

            latest_update_timestamp = """
            SELECT 
            CONVERT_TIMEZONE('UTC', 'America/New_York', MAX(updateddate)::TIMESTAMP_NTZ) AS max_updated_date_eastern
            FROM 
            EDWSTAGING.JIRA.PLANNING_EPICS;
            """
            latest_update_timestamp = connsnowflake.cursor().execute(latest_update_timestamp).fetchone()[0]
            if latest_update_timestamp is None:
                full_load = 1
            # typecast latestupate_timestamp to yyyy-MM-dd HH:mm, remove timezone info as well
            else:
                naive_timestamp = latest_update_timestamp.replace(tzinfo=None)
                latest_update_timestamp = datetime.strftime(naive_timestamp, '%Y-%m-%d %H:%M').replace(' ', '%20')
                logging.info(f"Latest update timestamp for themes: {latest_update_timestamp}")

            # override full_load to 1
            full_load = 1
            get_epics(full_load, latest_update_timestamp)
            df = pd.DataFrame(rows, columns=cols)
            if len(df) == 0:
                logging.info("No data to insert")
                return

            # df['loaddate'] to utc timezone current timestamp
            df['LOADDATE'] = datetime.now(pytz.utc)
            df['IsCurrent'] = 1
            df['row_hash'] = ""
            # connect to snowflake
            df.columns = df.columns.str.upper()

            #use_logical_types=True
            source_db = 'EDWSTAGING'
            source_schema = 'STAGING_TEMP'
            source_table = 'PLANNING_EPICS_STAGING'
            #
            target_db = 'EDWSTAGING'
            target_schema = 'JIRA'
            target_table = 'PLANNING_EPICS'
            #
            trunc_stage = f"TRUNCATE TABLE {source_db}.{source_schema}.{source_table}"
            connsnowflake.cursor().execute(trunc_stage)

            # res = write_pandas(conn=connsnowflake, df=df, table_name=table, database=db, schema=schema, use_logical_type=True)
            # print(res)
            # bulk insert

            # df.columns = df.columns.str.upper()
            # date_columns = ['RESOLVEDDATE', 'STARTDATE', 'ENDDATE', 'DUEDATE', 'CREATEDDATE', 'UPDATEDDATE', 'LOADDATE']
            # for col in date_columns:
            #     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

            # print(df.dtypes)
            # df.fillna('', inplace=True)
            # print(df.dtypes)

            if write_df(conn=connsnowflake, df=df, table=source_table, db=source_db, schema=source_schema):

                proc_sql = "CALL EDWSTAGING.JIRA.PRC_PLANNING_EPICS_HISTORY()"
                connsnowflake.cursor().execute(proc_sql)
                # if full_load == 1:
                #     trunc_table = "TRUNCATE TABLE EDWSTAGING.JIRA.PLANNING_EPICS"
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
                message = "Epics populated successfully"
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
                message = "Error in populating Epics"
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
                #logging.error("Error in writing data to snowflake", e)
    except Exception as e:
        with snowflake.connector.connect(**snowflakeconn) as connsnowflake:
            message = "Error in populating Epics" + str(e).replace(",", "")
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
            logging.error(f"Error in populating Epics {e}")
            raise e


# update the snowflake connection details
