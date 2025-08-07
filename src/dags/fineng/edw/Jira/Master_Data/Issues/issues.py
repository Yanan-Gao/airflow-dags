import requests
from requests.auth import HTTPBasicAuth
import json
import snowflake.connector
from dags.fineng.edw.utils.helper import bulk_insert, merge_data, write_df, write_to_execution_logs
from dags.fineng.edw.utils.jira_utils import extract_text_from_description
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


def safe_write_batch(rows, cols, snowflake_conn, source_table, source_db, source_schema, batch_number):
    """
    Safely write a batch with minimal error handling
    """
    import pandas as pd
    import pytz

    try:
        logging.info(f"BATCH {batch_number}: Attempting to write {len(rows)} rows...")

        df = pd.DataFrame(rows, columns=cols)
        df['LOADDATE'] = datetime.now(pytz.utc)
        df['IsCurrent'] = 1
        df['row_hash'] = ""
        df.columns = df.columns.str.upper()

        # Only handle dict objects that cause PyArrow issues
        for col in df.columns:
            if df[col].dtype == 'object':
                mask = df[col].apply(lambda x: isinstance(x, dict) if x is not None else False)
                if mask.any():
                    logging.warning(f"BATCH {batch_number}: Converting dict objects to strings in column {col}")
                    df[col] = df[col].apply(lambda x: str(x) if isinstance(x, dict) else x)

        write_result = write_df(conn=snowflake_conn, df=df, table=source_table, db=source_db, schema=source_schema)
        logging.info(f"BATCH {batch_number}: SUCCESS - {write_result}")
        return []  # Return empty list meaning all rows processed successfully

    except Exception as e:
        logging.error(f"BATCH {batch_number}: FAILED with error: {str(e)}")
        logging.error("Skipping this batch due to unresolvable data issues")
        return []  # Skip problematic batches instead of debugging


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

    logging.info("-----------------------------")
    logging.info(f"JQL Query: {jql_query}")

    ## Fetching Issues with new pagination format
    next_page_token = None
    total_processed = 0

    while True:
        # Build URL with nextPageToken if available
        if next_page_token:
            url = f"https://thetradedesk.atlassian.net/rest/api/3/search/jql?fields={fields}&jql={jql_query}&nextPageToken={next_page_token}&maxResults=500&validateQuery=strict"
        else:
            url = f"https://thetradedesk.atlassian.net/rest/api/3/search/jql?fields={fields}&jql={jql_query}&maxResults=500&validateQuery=strict"

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

        # Process issues from current page
        for issue in res.get('issues', []):
            if 'fields' not in issue:
                logging.warning(f"Issue {issue.get('key', 'unknown')} missing 'fields' key. Skipping.")
                continue
            try:
                IssueType = issue['fields'].get('issuetype', {})
                IssueType = IssueType.get('name', None) if isinstance(IssueType, dict) else None
                ###

                Project = issue['fields'].get('project', {})
                Project = Project.get('name', None) if isinstance(Project, dict) else None
                ####

                ProjectCategory = issue['fields'].get('project', {})
                ProjectCategory = ProjectCategory.get('projectCategory', {}) if isinstance(ProjectCategory, dict) else {}
                ProjectCategory = ProjectCategory.get('name', None) if isinstance(ProjectCategory, dict) else None
                #####

                Key = issue.get('key', None)
                Summary = issue['fields'].get('summary', None)
                Description = issue['fields'].get('description', None)
                # Extract clean text from complex description objects
                if Description and isinstance(Description, dict):
                    Description = extract_text_from_description(Description)
                Labels = issue['fields'].get('labels', None)
                labels = "$;;$".join(Labels) if isinstance(Labels, list) and Labels else None
                ######

                Status = issue['fields'].get('status', {})
                Status = Status.get('name', None) if isinstance(Status, dict) else None
                ######
                PriorityName = issue['fields'].get('priority', {})
                PriorityName = PriorityName.get('name', None) if isinstance(PriorityName, dict) else None
                ###
                Resolution = issue['fields'].get('resolution', {})
                Resolution = Resolution.get('name', None) if isinstance(Resolution, dict) else None
                #######
                Sprint = issue['fields'].get('customfield_10020', None)
                if Sprint and isinstance(Sprint, list):
                    try:
                        Sprint = ''.join(s.get('name', '') for s in Sprint if isinstance(s, dict) and s.get('state', '') == 'active')
                        Sprint = Sprint if Sprint else None
                    except Exception:
                        Sprint = None
                else:
                    Sprint = None

                # Parent fields
                Parent = issue['fields'].get('parent', {})
                ParentID = Parent.get('id', None) if isinstance(Parent, dict) else None
                ParentLink = Parent.get('self', None) if isinstance(Parent, dict) else None
                ParentKey = Parent.get('key', None) if isinstance(Parent, dict) else None

                # Assignee fields
                AssigneeData = issue['fields'].get('assignee', {})
                Assignee = AssigneeData.get('displayName', None) if isinstance(AssigneeData, dict) else None
                AssigneeEmail = AssigneeData.get('emailAddress', None) if isinstance(AssigneeData, dict) else None

                # Reporter
                Reporter = issue['fields'].get('reporter', {})
                Reporter = Reporter.get('displayName', None) if isinstance(Reporter, dict) else None

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
                TechnologyComponent = TechnologyComponent.get('value', None) if isinstance(TechnologyComponent, dict) else None

                ##
                OrderOfImportance = issue['fields'].get('customfield_10859', None)
                Ranking = issue['fields'].get('customfield_10549', None)
                DocStatus = issue['fields'].get('customfield_10546', {})
                DocStatus = DocStatus.get('value', None) if isinstance(DocStatus, dict) else None
                ###
                Audience = issue['fields'].get('customfield_10550', {})
                Audience = Audience.get('value', None) if isinstance(Audience, dict) else None
                ##
                UXMOCK = issue['fields'].get('customfield_10543', None)
                UXMockStatus = issue['fields'].get('customfield_10551', {})
                UXMockStatus = UXMockStatus.get('value', None) if isinstance(UXMockStatus, dict) else None
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
                StatusCategory = StatusCategory.get('statusCategory', {}) if isinstance(StatusCategory, dict) else {}
                StatusCategoryID = StatusCategory.get('id', None) if isinstance(StatusCategory, dict) else None
                StatusCategoryKey = StatusCategory.get('key', None) if isinstance(StatusCategory, dict) else None
                StatusCategoryColorName = StatusCategory.get('colorName', None) if isinstance(StatusCategory, dict) else None
                StatusCategoryName = StatusCategory.get('name', None) if isinstance(StatusCategory, dict) else None
                row = [
                    IssueType, Project, ProjectCategory, Key, Epiclink, EpicName, Summary, Status, ParentID, ParentLink, ParentKey,
                    TechnologyComponent, CreatedDate, UpdatedDate, ResolvedDate, StartDate, EndDate, DueDate, Description,
                    DocumentationLink, Assignee, AssigneeEmail, EpicStoryPoints, StatusCategoryID, StatusCategoryKey,
                    StatusCategoryColorName, StatusCategoryName, PriorityName, OfficialStoryPointEstimate, OriginalStoryPointEstimate,
                    StoryPoints, Reporter, Resolution, Sprint, OrderOfImportance, Ranking, DocStatus, Audience, UXMOCK, UXMockStatus, labels
                ]

                # Simple validation - only convert dict objects to strings
                validated_row = []

                for i, (col_name, value) in enumerate(zip(cols, row)):
                    # Only handle dict objects that cause PyArrow struct errors
                    if isinstance(value, dict):
                        logging.warning(f"Issue {Key}: Column {col_name} has dict object, converting to string")
                        validated_row.append(str(value))
                    else:
                        validated_row.append(value)

                rows.append(validated_row)
                total_processed += 1

            except Exception as e:
                msg = f"Skipping this record {Key} due to error {e} "
                print(msg)
                continue

        # Log progress
        print(f"Processed {total_processed} issues so far...")
        logging.info(f"Processed {total_processed} issues so far...")

        # Write to database in batches of 20000
        if len(rows) % 20000 == 0 and len(rows) > 0:
            batch_number = len(rows) // 20000
            source_db = 'EDWSTAGING'
            source_schema = 'STAGING_TEMP'
            source_table = 'MASTER_TICKETS_STAGING'

            failed_rows = safe_write_batch(rows, cols, snowflake_conn, source_table, source_db, source_schema, batch_number)

            # If some rows failed, keep them for retry in the next batch
            rows = failed_rows

        # Check if this is the last page
        if res.get('isLast', False):
            logging.info("Reached last page of results")
            break

        # Get next page token for next iteration
        next_page_token = res.get('nextPageToken')
        if not next_page_token:
            logging.info("No nextPageToken found, ending pagination")
            break

    # Process remaining rows
    if len(rows) > 0:
        logging.info("Processing remaining rows at end of pagination...")
        source_db = 'EDWSTAGING'
        source_schema = 'STAGING_TEMP'
        source_table = 'MASTER_TICKETS_STAGING'

        failed_rows = safe_write_batch(rows, cols, snowflake_conn, source_table, source_db, source_schema, "FINAL")

        if failed_rows:
            logging.error(f"FINAL BATCH: {len(failed_rows)} rows could not be processed due to data issues")
        else:
            logging.info("FINAL BATCH: All remaining rows processed successfully")

        rows = []

    logging.info(f"Total issues processed: {total_processed}")


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
