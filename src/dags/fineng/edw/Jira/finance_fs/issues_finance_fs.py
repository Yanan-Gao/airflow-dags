import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
from datetime import datetime
import snowflake.connector
import logging
from snowflake.connector.pandas_tools import write_pandas
from dags.fineng.edw.utils.helper import write_df, write_to_execution_logs
import re


def fix_timestamp_timezone(ts):
    # Converts "-0400" → "-04:00"
    return re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', ts)


def get_asset_name(field_data, jira_username, jira_password):
    """
    Fetches the label for a Jira asset field using the objectId.

    Args:
        field_data (list): The list returned from the Jira custom field (e.g., fields.get("customfield_XXXXX")).
        workspace_id (str): The Jira Assets workspace ID.
        jira_username (str): Jira username/email for authentication.
        jira_password (str): Jira API token or password for authentication.

    Returns:
        str or None: The label of the asset if found, otherwise None.
    """
    object_id = None
    if isinstance(field_data, list) and field_data:
        object_id = field_data[0].get("objectId")

    if not object_id:
        return None

    url = f"https://api.atlassian.com/jsm/assets/workspace/c2a203d7-430c-4757-b99b-7f4079527ce7/v1/object/{object_id}"
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers, auth=HTTPBasicAuth(jira_username, jira_password))

    if response.ok:
        data = response.json()
        label = data.get("label")
        return label
    else:
        print(f"Error {response.status_code}: Could not fetch asset label.")
        return None


def extract_description_text(description_field):
    if isinstance(description_field, dict):
        content = description_field.get("content", [])
        return extract_text_from_adf(content)
    elif isinstance(description_field, str):
        return description_field
    else:
        return ""


def extract_text_from_adf(content):
    text_parts = []
    for item in content:
        if item.get("type") == "paragraph":
            paragraph_text = ""
            for subitem in item.get("content", []):
                if subitem.get("type") == "text":
                    paragraph_text += subitem.get("text", "")
            text_parts.append(paragraph_text)
        elif "content" in item:
            # Recursively process nested content
            text_parts.append(extract_text_from_adf(item["content"]))
    return "\n".join(text_parts)


def populate_finance_fs(snowflakeconn, jira_username, jira_password):

    latest_update_timestamp = """
            
            SELECT 
            CONVERT_TIMEZONE('UTC', 'America/New_York', MAX(updated)::TIMESTAMP_NTZ) AS max_updated_date_eastern
            FROM 
            EDWSTAGING.JIRA.FINANCE_FS;
            """
    with snowflake.connector.connect(**snowflakeconn, insecure_mode=True) as connsnowflake:
        latest_update_timestamp = connsnowflake.cursor().execute(latest_update_timestamp).fetchone()[0]
    print(latest_update_timestamp)
    if latest_update_timestamp is None:
        jql = (
            f'(project = FS) OR '
            f'(project = TECHOPS AND issuetype = "Change Request" AND '
            f'"Change Fulfillment Team" = "ari:cloud:cmdb::object/c2a203d7-430c-4757-b99b-7f4079527ce7/351")'
        )
    else:
        # Remove timezone info (if the timestamp is timezone-aware)
        naive_timestamp = latest_update_timestamp.replace(tzinfo=None)

        latest_update_timestamp = naive_timestamp.strftime('%Y-%m-%d %H:%M')

        logging.info(f"Latest update timestamp for themes: {latest_update_timestamp}")
        jql = (
            f'((project = FS) OR '
            f'(project = TECHOPS AND issuetype = "Change Request" AND '
            f'"Change Fulfillment Team" = "ari:cloud:cmdb::object/c2a203d7-430c-4757-b99b-7f4079527ce7/351")) AND updated >= "{latest_update_timestamp}" '
        )

    print(jql)
    # Set headers and authentication
    base_url = "https://thetradedesk.atlassian.net"
    start_time = datetime.now()

    process_name = "(MAIN-EDW) Jira-Finance-FS-Integration"
    process_id = "(MAIN-EDW) Jira-Finance-FS-Integration"
    db = 'EDWSTAGING'
    schema = 'JIRA'
    table = 'FINANCE_FS'

    try:
        headers = {"Accept": "application/json"}
        auth = HTTPBasicAuth(jira_username, jira_password)

        # Jira base URL and JQL
        max_results = 100  # max Jira allows per request
        total_to_fetch = 100000

        # Output list
        all_issues = []

        start_at = 0

        while len(all_issues) < total_to_fetch:
            url = f"{base_url}/rest/api/3/search"
            params = {"jql": jql, "startAt": start_at, "maxResults": min(max_results, total_to_fetch - len(all_issues)), "fields": "*all"}

            response = requests.get(url, headers=headers, params=params, auth=auth)
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break

            data = response.json()
            issues = data.get("issues", [])
            if not issues:
                break

            for issue in issues:
                fields = issue.get("fields", {})
                resolution = fields.get("resolution") or {}
                assignee = fields.get("assignee") or {}
                description = fields.get("description") or {}
                description_text = extract_description_text(description)
                sprint = fields.get("customfield_10020") or {}
                sprint = ', '.join(s.get('name', '') for s in sprint) if sprint else None

                FS_Application = fields.get("customfield_10373") or {}
                functional_area = fields.get("customfield_10392") or {}
                blockreason = fields.get("customfield_10371") or {}

                print('In the loop cl v1')
                UpdatedDate = issue['fields'].get('updated', None)
                UpdatedDate = datetime.strptime(UpdatedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if UpdatedDate else None

                CreatedDate = issue['fields'].get('created', None)
                CreatedDate = datetime.strptime(CreatedDate, '%Y-%m-%dT%H:%M:%S.%f%z') if CreatedDate else None

                ResolutionDate = issue['fields'].get('resolutiondate', None)
                ResolutionDate = datetime.strptime(ResolutionDate, '%Y-%m-%dT%H:%M:%S.%f%z') if ResolutionDate else None

                LastViewed = issue['fields'].get('lastViewed', None)
                LastViewed = datetime.strptime(LastViewed, '%Y-%m-%dT%H:%M:%S.%f%z') if LastViewed else None

                Change_Fulfillment_Team = fields.get("customfield_12033") or []
                Change_Fulfillment_Team_Name = get_asset_name(Change_Fulfillment_Team, jira_username, jira_password)

                Insight_Business_Owner = fields.get("customfield_12034") or []
                Insight_Business_Owner_Name = get_asset_name(Insight_Business_Owner, jira_username, jira_password)

                Application = fields.get("customfield_12011") or []
                Application_Name = get_asset_name(Application, jira_username, jira_password)

                issue_data = {
                    "Id": issue.get("id"),
                    "Key": issue.get("key"),
                    "IssueTypeId": fields.get("issuetype", {}).get("id"),
                    "IssueTypeName": fields.get("issuetype", {}).get("name"),
                    "ProjectId": fields.get("project", {}).get("id"),
                    "ProjectName": fields.get("project", {}).get("name"),
                    "ProjectKey": fields.get("project", {}).get("key"),
                    "parentid": fields.get("parent", {}).get("id"),
                    "parentkey": fields.get("parent", {}).get("key"),
                    "parentissuetypeid": fields.get("parent", {}).get("fields", {}).get("issuetype", {}).get("id"),
                    "parentissuetypename": fields.get("parent", {}).get("fields", {}).get("issuetype", {}).get("name"),
                    "parentstatusid": fields.get("parent", {}).get("fields", {}).get("status", {}).get("id"),
                    "parentstatusname": fields.get("parent", {}).get("fields", {}).get("status", {}).get("name"),
                    "resolutionname": resolution.get("name"),
                    "resolutiondescription": resolution.get("description"),
                    "resolutionid": resolution.get("id"),
                    "resolutiondate": ResolutionDate,
                    "lastviewed": LastViewed,
                    "watchcount": fields.get("watches", {}).get("watchCount"),
                    "iswatching": fields.get("watches", {}).get("isWatching"),
                    "created": CreatedDate,
                    "priorityid": fields.get("priority", {}).get("id"),
                    "priorityname": fields.get("priority", {}).get("name"),
                    "assigneedisplayname": assignee.get("displayName"),
                    "assigneekey": assignee.get("key"),
                    "assigneeemail": assignee.get("emailAddress"),
                    "updated": UpdatedDate,
                    "statusname": fields.get("status", {}).get("name"),
                    "statusid": fields.get("status", {}).get("id"),
                    "statuscategoryname": fields.get("status", {}).get("statusCategory", {}).get("name"),
                    "description": description_text,
                    "summary": fields.get("summary"),
                    "creatordisplayname": fields.get("creator", {}).get("displayName"),
                    "creatorname": fields.get("creator", {}).get("name"),
                    "creatorkey": fields.get("creator", {}).get("key"),
                    "creatoremail": fields.get("creator", {}).get("emailAddress"),
                    "reporterdisplayname": fields.get("reporter", {}).get("displayName"),
                    "reportername": fields.get("reporter", {}).get("name"),
                    "reporteremail": fields.get("reporter", {}).get("emailAddress"),
                    "duedate": fields.get("duedate"),
                    "labels": fields.get("labels"),
                    "sprint": sprint,  # customfield_10020
                    "FS_Application": FS_Application.get("value"),  # customfield_10373
                    "functional_area": functional_area.get("value"),  # customfield_10392                    
                    "Story_Points": fields.get("customfield_10100"),
                    "Story_Points_Completed": fields.get("customfield_10130"),
                    "BlockReason": blockreason.get("value"),
                    "Insight_Business_Owner": Insight_Business_Owner_Name,
                    "Application": Application_Name,
                    "Change_Fulfillment_Team": Change_Fulfillment_Team_Name,
                }
                all_issues.append(issue_data)
                # Get changelog
                issue_id = issue.get("id")
                issue_key = issue.get("key")
                project = issue["fields"]["project"]
                project_id = project["id"]
                project_key = project["key"]
                project_name = project["name"]
                created = issue["fields"]["created"]
                updated = issue["fields"]["updated"]

                changelog_url = f"https://thetradedesk.atlassian.net/rest/api/3/issue/{issue_key}/changelog"
                start_changelog = 0
                rows = []
                while True:
                    cl_params = {"startAt": start_changelog, "maxResults": 100}
                    cl_resp = requests.get(changelog_url, headers=headers, auth=auth, params=cl_params)
                    cl_data = cl_resp.json()
                    for history in cl_data.get("values", []):
                        history_id = history["id"]
                        hist_created = history["created"]
                        author = history.get("author", {})
                        account_id = author.get("accountId")
                        name = author.get("name")  # May be None
                        display_name = author.get("displayName")
                        for item in history.get("items", []):
                            rows.append({
                                "HistoryId": history_id,
                                "IssueId": issue_id,
                                "IssueKey": issue_key,
                                "Created": hist_created,
                                "IssueCreatedDate": created,
                                "IssueUpdatedDate": updated,
                                "AuthorAccountId": account_id,
                                "AuthorDisplayName": display_name,
                                "ItemField": item.get("field"),
                                "ItemFieldType": item.get("fieldtype"),
                                "ItemFrom": item.get("from"),
                                "ItemFromString": item.get("fromString"),
                                "ItemTo": item.get("to"),
                                "ItemToString": item.get("toString"),
                                "ProjectId": project_id,
                                "ProjectKey": project_key,
                                "ProjectName": project_name
                            })
                    if cl_data.get("isLast", True):  # Stop if last page
                        break
                    start_changelog += 100

                # Convert to DataFrame
                df_cl = pd.DataFrame(rows)
                if df_cl.empty != True:
                    for col in ["Created", "IssueCreatedDate", "IssueUpdatedDate"]:
                        df_cl[col] = df_cl[col].apply(fix_timestamp_timezone)
                    df_cl.columns = df_cl.columns.str.upper()

                    #print(df_cl.head())
                    with snowflake.connector.connect(**snowflakeconn, insecure_mode=True) as connsnowflake:
                        sql = "delete from edwstaging.jira.finance_fs_issue_changelog where issueid = " + issue_id
                        connsnowflake.cursor().execute(sql)
                        print(f"issue key, {issue_key}")
                        write_df(conn=connsnowflake, df=df_cl, table="FINANCE_FS_ISSUE_CHANGELOG", schema=schema, db=db)
            start_at += max_results

        df = pd.DataFrame(all_issues)
        if df.empty != True:
            df.columns = df.columns.str.upper()

        with snowflake.connector.connect(**snowflakeconn, insecure_mode=True) as connsnowflake:
            write_df(conn=connsnowflake, df=df, table=table, schema=schema, db=db)
            endtime = datetime.now()
            write_to_execution_logs(
                conn=connsnowflake,
                processname=process_name,
                processid=process_id,
                processstatus='SUCCESS',
                processstarttime=start_time,
                processendtime=endtime,
                message="LOADED SUCCESSFULLY"
            )
    except Exception as e:
        message = "Error in Populating Finance FS" + str(e).replace(",", "")
        endtime = datetime.now()
        with snowflake.connector.connect(**snowflakeconn, insecure_mode=True) as connsnowflake:
            write_to_execution_logs(
                conn=connsnowflake,
                processname=process_name,
                processid=process_id,
                processstatus='ERROR',
                processstarttime=start_time,
                processendtime=endtime,
                message=message
            )
        logging.error(f"Error in populating Finance FS {e}")
        raise e
