from datetime import date, timedelta, datetime, time, timezone
import logging
import time as time_module
import xml.etree.ElementTree as ET
import pymssql
import vertica_python
import snowflake.connector
import psycopg2
import tableauserverclient as TSC
import concurrent.futures
import threading
import functools
import re
from requests.exceptions import ConnectionError
from dags.fineng.edw.autoscheduler.on_prem import autoscheduler_queries
from ttd.ttdslack import dag_post_to_slack_callback, get_slack_client
from ttd.el_dorado.v2.base import TtdDag
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

autoscheduler_version = '*EDW-BI On-Prem AutoScheduler v1.0.1*'

alarm_slack_channel = '#edw-autoscheduler-test'

default_args = {
    'owner': 'FINENG',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
    'catchup': False,
}

dag = TtdDag(
    dag_id='tableau-autoscheduler',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=23, minutes=55),
    on_failure_callback=
    dag_post_to_slack_callback(dag_name='tableau-autoscheduler', step_name='parent dagrun', slack_channel=alarm_slack_channel),
    schedule_interval='0 1 * * *',  # Start at 01:00 UTC every day, in line with #bi-data-refreshes
    max_active_runs=1,
    run_only_latest=True,
)
adag = dag.airflow_dag


# Centralized slack alert function
def send_slack_alert(slack_message, thread_ts=None, **kwargs):
    try:
        # Send the message, optionally as a thread
        response = get_slack_client().chat_postMessage(
            channel=alarm_slack_channel, text=f"{autoscheduler_version}: {slack_message}", thread_ts=thread_ts
        )
        logging.info(slack_message)
        return response
    except slack.errors.SlackApiError as e:
        logging.error(f"Failed to send message to Slack due to API error: {e.response['error']}")
    except RequestException as e:
        logging.error(f"Failed to send message to Slack due to network error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error occurred when sending message to Slack: {e}")


# Send a slack message to alert the team that the day's run is starting
def init_slack(**kwargs):
    current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    slack_message = f"AutoScheduler is starting the day's run at {current_timestamp} UTC"
    send_slack_alert(slack_message)


def term_slack(**kwargs):
    autoscheduler_result = autoscheduler_result_fetch()

    # Extract the results from the dict
    total_job_count = autoscheduler_result["total_job_count"]
    successful_job_count = autoscheduler_result["successful_job_count"]
    failed_job_count = autoscheduler_result["failed_job_count"]
    cancelled_job_count = autoscheduler_result["cancelled_job_count"]
    skipped_job_count = autoscheduler_result["skipped_job_count"]

    current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    tasks_completed_slack_message = f"""Done running for the day at {current_timestamp} UTC. We had a total queue of *{total_job_count} job(s).*\n    • {successful_job_count} job(s) succeeded\n    • {failed_job_count} job(s) failed\n    • {cancelled_job_count} job(s) were cancelled\n    • {skipped_job_count} job(s) were skipped\nFind the full history of today's run in Snowflake at: `select * from EDWANALYTICS.AUTOSCHEDULER.VW_BI_AUTOSCHEDULER_RESULT ;`"""

    send_slack_alert(tasks_completed_slack_message)
    """
    # how to send a message to a thread
    thread_message = f"This is a test to see if I sent a message to the correct thread."
    
    init_response = send_slack_alert(tasks_completed_slack_message)
    thread_id = init_response['message']['ts']
    
    send_slack_alert(thread_message, thread_ts=thread_id)
    """


# Send a slack message to warn the team of certain tasks retrying
def notify_on_retry(context, **kwargs):
    task_instance = context['ti']
    attempt = task_instance.try_number

    if {context['task_instance'].task_id} == 'is_vertica_east_ready' or {context['task_instance'].task_id} == 'is_vertica_west_ready' or {
            context['task_instance'].task_id
    } == 'is_edw_snowflake_ready':
        if attempt == 2:
            retry_warning_message = f":warning: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` is running attempt {attempt} of 2. There are no retries left after this attempt. Please visit the Airflow UI to investigate."
            send_slack_alert(retry_warning_message)
            logging.warning(retry_warning_message)
    elif {context['task_instance'].task_id} == 'bi_autoscheduler':
        if attempt == 2:
            autsch_retry_message = f":warning: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` triggered a retry! This is not expected behavior. Please visit the UI to investigate."
            send_slack_alert(autsch_retry_message)
            logging.warning(autsch_retry_message)
        elif attempt == 3:
            autsch_final_retry_message = f":alert: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` is running it's final retry. Something must be wrong. Please visit the UI to investigate."
            send_slack_alert(autsch_final_retry_message)
            logging.warning(autsch_final_retry_message)
    elif {context['task_instance'].task_id} == 'tableau_job_flow_load':
        if attempt == 4:
            job_flow_retry_message = f":alert: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` has failed 3 times and is on _Attempt 4_. This is not expected behavior. There is likely an issue with the Tableau Postgres Database. There is 1 retry left. Please visit the UI to investigate."
            send_slack_alert(job_flow_retry_message)
            logging.warning(job_flow_retry_message)
        elif attempt == 5:
            autsch_final_retry_message = f":alert: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` is running it's final retry. Please visit the UI to investigate and run manually if necessary."
            send_slack_alert(autsch_final_retry_message)
            logging.warning(autsch_final_retry_message)
    else:
        logging.info(f"Task {context['task_instance'].task_id} is not relevant for retry alerting. Skipping...")


def get_db_connection(conn_name):
    if conn_name == 'provdb_bi':
        conn = BaseHook.get_connection('provdb_bi')
        return {'host': conn.host, 'user': conn.login, 'password': conn.password, 'database': conn.schema}
    elif conn_name == 'vertica_useast01_bi':
        conn = BaseHook.get_connection('vertica_useast01_bi')
        return {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}
    elif conn_name == 'vertica_uswest01':
        conn = BaseHook.get_connection('vertica_uswest01')
        return {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}
    elif conn_name == 'ttd_tableau_snowflake_prod':
        conn = BaseHook.get_connection('ttd_tableau_snowflake_prod')
        return {
            'account': conn.extra_dejson['account'],
            'warehouse': conn.extra_dejson['warehouse'],
            'user': conn.login,
            'password': conn.password,
            'database': conn.extra_dejson['database']
        }
    elif conn_name == 'ttd_tableau_snowflake_dev':
        conn = BaseHook.get_connection('ttd_tableau_snowflake_dev')
        return {
            'account': conn.extra_dejson['account'],
            'warehouse': conn.extra_dejson['warehouse'],
            'user': conn.login,
            'password': conn.password,
            'database': conn.extra_dejson['database']
        }
    elif conn_name == 'autoscheduler_edw_snowflake_dev':
        conn = BaseHook.get_connection('autoscheduler_edw_snowflake_dev')
        return {
            'account': conn.extra_dejson['account'],
            'user': conn.login,
            'password': conn.password,
            'warehouse': conn.extra_dejson['warehouse'],
            'role': conn.extra_dejson['role'],
            'database': conn.extra_dejson['database'],
            'schema': conn.schema
        }
    elif conn_name == 'autoscheduler_edw_snowflake_prod':
        conn = BaseHook.get_connection('autoscheduler_edw_snowflake_prod')
        return {
            'account': conn.extra_dejson['account'],
            'user': conn.login,
            'password': conn.password,
            'warehouse': conn.extra_dejson['warehouse'],
            'role': conn.extra_dejson['role'],
            'database': conn.extra_dejson['database'],
            'schema': conn.schema
        }

    # prod: tableau-postgres.thetradedesk.com
    # Blue Server IP: 10.100.129.253
    # Green Server IP: 10.100.129.40

    elif conn_name == 'tableau_postgres':
        conn = BaseHook.get_connection('tableau_postgres')
        return {
            'host': 'tableau-postgres.thetradedesk.com',
            'port': conn.port,
            'database': conn.schema,
            'user': conn.login,
            'password': conn.password
        }
    elif conn_name == 'tableau_server':
        conn = BaseHook.get_connection('tableau_server')
        return {
            'server': 'https://tableau-old.thetradedesk.com',
            'site': conn.extra_dejson['site_id'],
            'username': conn.login,
            'password': conn.password
        }
    else:
        raise ValueError(f"Unknown connection name: {conn_name}")


# Construct the full data pipeline used by BI AutoScheduler
def tableau_job_flow(**kwargs):
    # fetch DB connection info
    tableau_postgres_conn_info = get_db_connection('tableau_postgres')
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')
    logging.info(f"tableau_postgres_conn_info: {tableau_postgres_conn_info}")
    logging.info(f"edw_snowflake_prod_conn_info: {autoscheduler_edw_snowflake_prod_conn_info}")

    # Load the Tableau Postgres data (the full queue for the day) into Snowflake and create all other Snowflake infra AutSch will use to run for the day
    with psycopg2.connect(**tableau_postgres_conn_info) as conn:
        try:
            import pandas as pd

            logging.info('Querying Tableau Postgres DB for the queue.')
            tableau_job_flow = pd.read_sql(autoscheduler_queries.tableau_job_flow_from_postgres, conn)
            logging.info('Got the queue! Checking to make sure there is data in it...')

            if tableau_job_flow.empty:
                logging.error("Tableau Postgres returned empty!! This is very unexpected. Aborting the rest of this task.")
                return
        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred in retrieving Postgres data: {}".format(str(e)))

        # Establish DataFrame schema to match Snowflake schema
        dataframe_schema = {
            "LOG_ENTRY_DATE_UTC": "datetime64[ns]",
            "LOG_ENTRY_TIME_UTC": "datetime64[ns]",
            "SCHEDULE_NAME": "str",
            "SCHEDULE_CUSTOM_PRIORITY": "int64",
            "SCHEDULE_FREQUENCY": "int64",
            "SCHEDULE_IS_ADHOC_UPDATER": "bool",
            "TASK_PRIORITY": "int64",
            "TASK_OBJECT_TYPE": "str",
            "TASK_OWNER": "str",
            "TASK_LUID": "str",
            "TASK_TARGET_NAME": "str",
            "OBJECT_LUID": "str",
            "USES_VERTICA_EAST": "bool",
            "USES_VERTICA_WEST": "bool",
            "USES_EDW_SNOWFLAKE": "bool",
            "IS_ACTIVE_QUEUE": "bool",
            "IS_PREVIOUS_QUEUE": "bool"
        }

        # Convert DataFrame columns to match Snowflake schema
        for col, dtype in dataframe_schema.items():
            try:
                tableau_job_flow[col] = tableau_job_flow[col].astype(dtype)
            except ValueError:
                logging.warning(f"Error converting {col} to {dtype}")
                raise

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_cur = sf_conn.cursor()
            # converting timestamps to string in dataframe because binding is CRAZY sensitive for whatever reason and will kick an undiagnosable error if this isn't done (literally took me a week to figure out)
            tableau_job_flow['LOG_ENTRY_DATE_UTC'] = tableau_job_flow['LOG_ENTRY_DATE_UTC'].astype(str)
            tableau_job_flow['LOG_ENTRY_TIME_UTC'] = tableau_job_flow['LOG_ENTRY_TIME_UTC'].astype(str)
            log_entry_date_utc = tableau_job_flow['LOG_ENTRY_DATE_UTC'].iloc[0]
            query_param_log_entry_date_utc = {'log_entry_date_utc': log_entry_date_utc}

            # Pushing the date we're inserting data for to xcoms to use downstream
            kwargs['ti'].xcom_push(key='tableau_job_flow_log_entry_date_utc', value=log_entry_date_utc)

            # Check if LOG_ENTRY_DATE_UTC already exists in TABLEAU_JOB_HISTORY
            logging.debug("TABLEAU_JOB_HISTORY update.")
            logging.info("Checking to see if the Tableau Postgres data I'm trying to load into TABLEAU_JOB_HISTORY already exists.")
            job_queue_count = sf_cur.execute(autoscheduler_queries.job_history_duplicate_check,
                                             query_param_log_entry_date_utc).fetchone()[0]

            if job_queue_count > 0:
                logging.warning(
                    "LOG_ENTRY_DATE_UTC = {} already exists in TABLEAU_JOB_HISTORY. Skipping data load. Attempted at {} UTC"
                    .format(log_entry_date_utc,
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                )
            else:
                logging.info("Tableau Refresh Queue for today does not exist. Going to attempt to load it now!")
                logging.info('This is 4 Step Process. If a step does not log a "Success!" then please investigate that step. Wish me luck!')
                logging.info(
                    "Step 1: Marking all rows in TABLEAU_JOB_HISTORY to IS_ACTIVE_QUEUE = FALSE and marking the previous run as IS_PREVIOUS_QUEUE"
                )
                # update the table to mark the previous queue as IS_PREVIOUS_QUEUE and set the whole table to IS_ACTIVE_QUEUE = FALSE
                sf_cur.execute(autoscheduler_queries.update_is_active_queue_to_false)
                sf_conn.commit()
                logging.info("Step 1: Success!")

                # prepare Tableau Postgres data for insertion
                logging.info("Step 2: Uploading the Tableau Postgres data to a temp table for staging.")
                sf_cur.execute(autoscheduler_queries.tableau_job_flow_load_to_temp_snowflake)
                data_tuples = [tuple(x) for x in tableau_job_flow.values]
                num_cols = len(tableau_job_flow.columns)
                query = f"INSERT INTO TABLEAU_POSTGRES_QUEUE VALUES ({','.join(['%s'] * num_cols)})"
                sf_cur.executemany(query, data_tuples)
                sf_conn.commit()
                logging.info("Step 2: Success!")

                # Insert the queue into production table
                logging.info("Step 3: Insert the queue from the temp table into the production table.")
                sf_cur.execute(autoscheduler_queries.load_temp_job_flow_to_prod)
                sf_conn.commit()
                logging.info("Step 3: Success!")

                # drop temp_table used
                logging.info("Step 4: Dropping all temp sources used in Snowflake")
                sf_cur.execute(autoscheduler_queries.drop_job_flow_temp)
                sf_conn.commit()
                logging.info("Step 4: Success!")
                # logging.info(("Step 5: Bumping priority for all warp dashboards"))
                # sf_cur.execute(autoscheduler_queries.bump_warp_priority)
                # sf_conn.commit()
                # logging.info("All succesful on loading the day's queue! Let's-a-go!")

                print(
                    "Successfully loaded the Tableau Refresh Queue to Snowflake [EDWANALYTICS.AUTOSCHEDULER.TABLEAU_JOB_HISTORY] at " +
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                )
        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred in loading the Tableau Postgres data to Snowflake: {}".format(str(e)))

        try:
            # prepare DATABASE_READINESS_CHECK for the day's run
            logging.debug("DATABASE_READINESS_CHECK update")
            logging.info(
                "Now that the Tableau queue for the day has been loaded, I'm going to update the DATABASE_READINESS_CHECK table for today as well"
            )
            logging.info(
                "Checking to see if the LOG_ENTRY_DATE_UTC for DATABASE_READINESS_CHECK table I'm going to insert already exists..."
            )
            db_ready_count = sf_cur.execute(autoscheduler_queries.db_ready_duplicate_check, query_param_log_entry_date_utc).fetchone()[0]
            if db_ready_count > 0:
                logging.warning(
                    "LOG_ENTRY_DATE_UTC = {} already exists in DATABASE_READINESS_CHECK. Skipping data load. Attempted at {} UTC"
                    .format(log_entry_date_utc,
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                )
            else:
                # Insert the LOG_ENTRY_DATE_UTC into the DATABASE_READINESS_CHECK table and set other values
                logging.info(
                    "The LOG_ENTRY_DATE_UTC I checked for did not exist in DATABASE_READINESS_CHECK. Going to attempt to insert it now."
                )
                sf_cur.execute(autoscheduler_queries.db_ready_insert_new_date, query_param_log_entry_date_utc)
                logging.info("DATABASE_READINESS_CHECK table updated successfully!")
                print(
                    "Successfully prepped the DB check table [EDWANALYTICS.AUTOSCHEDULER.DATABASE_READINESS_CHECK] for the day at " +
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                )
        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred in updating DATABASE_READINESS_CHECK: {}".format(str(e)))

        try:
            # prepare TABLEAU_JOB_ACTIVE_QUEUE for the day's run
            logging.debug("TABLEAU_JOB_ACTIVE_QUEUE update")
            logging.info(
                "Now that TABLEAU_JOB_HISTORY and DATABASE_READINESS_CHECK are done, I'm going to see if I need to clear the TABLEAU_JOB_ACTIVE_QUEUE table for today as well"
            )
            active_q_count = sf_cur.execute(autoscheduler_queries.active_q_duplicate_check, query_param_log_entry_date_utc).fetchone()[0]
            if active_q_count > 0:
                logging.warning(
                    "LOG_ENTRY_DATE_UTC = {} already exists in TABLEAU_JOB_ACTIVE_QUEUE. Skipping truncate. Attempted at {} UTC"
                    .format(log_entry_date_utc,
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                )
            else:
                # Truncate TABLEAU_JOB_ACTIVE_QUEUE if the date inside of it is not the same date as everything else we just loaded
                logging.info(
                    "The LOG_ENTRY_DATE_UTC I checked for did not exist in TABLEAU_JOB_ACTIVE_QUEUE. Truncating the table so we can use it for today's run."
                )
                sf_cur.execute(autoscheduler_queries.active_q_truncate_query)
                logging.info(
                    "Successfully truncated [EDWANALYTICS.AUTOSCHEDULER.TABLEAU_JOB_ACTIVE_QUEUE] for the day at " +
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                )
        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred in truncating TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


# Check if Vertica databases data are ready
def is_vertica_ready(db, **kwargs):
    context = kwargs.get('context')
    logging.info(f"kwargs: {kwargs}")
    logging.info(f"context: {context}")
    if db == 'vertica_us_east':
        source = 7
        vertica_conn_info = get_db_connection('vertica_useast01_bi')
        cluster_name = 'Vertica East'
        xcoms_source = 'is_vertica_east_ready'
    elif db == 'vertica_us_west':
        source = 10
        vertica_conn_info = get_db_connection('vertica_uswest01')
        cluster_name = 'Vertica West'
        xcoms_source = 'is_vertica_west_ready'
    else:
        logging.error("Invalid DB parameter provided.")
        return

    # Setting threshold for when to signal that a Vertica DB is delayed
    db_delayed_tolerance_time = datetime.utcnow().replace(
        hour=4, minute=30, second=0, microsecond=0
    )  # 4:30am UTC for Vertica DBs as per Vivek from EDW team

    # Initialize slack alert tracking variables
    has_alerted_provdb_delay = False
    has_alerted_provdb_2hr_delay = False
    has_alerted_provdb_4hr_delay = False
    has_alerted_provdb_6hr_delay = False
    has_alerted_provdb_8hr_delay = False
    has_alerted_vertica_delay = False
    has_alerted_vertica_2hr_delay = False
    has_alerted_vertica_4hr_delay = False
    has_alerted_vertica_6hr_delay = False
    has_alerted_vertica_8hr_delay = False
    has_alerted_attribution_split_delay = False
    has_alerted_attribution_split_2hr_delay = False
    has_alerted_attribution_split_4hr_delay = False
    has_alerted_attribution_split_6hr_delay = False
    has_alerted_attribution_split_8hr_delay = False

    # Loop Init
    sleep_time = 900  # 15 minutes
    max_date = None
    today_date = datetime.utcnow().date()
    yesterday = today_date - timedelta(days=1)
    matching = False
    max_date_is_yesterday = False

    # fetch ProvDB connection info
    provdb_conn_info = get_db_connection('provdb_bi')

    # enter two while loops. first loop checks if ProvDB has marked the data as ready. second loop checks if the Vertica BI schema daily agg tables are ready.
    logging.info("Entering ProvDB loop")
    while not max_date_is_yesterday:
        try:
            with pymssql.connect(**provdb_conn_info) as conn:
                logging.info(f'Retrieving max_date for {cluster_name} from ProvDB')
                cur = conn.cursor(as_dict=True)
                cur.execute(autoscheduler_queries.vertica_max_date_query, (source, ))
                provdb_result = cur.fetchone()
                if provdb_result:
                    max_date = provdb_result['MaxDateInclusive']
                    logging.info(f'ProvDB max_date for {cluster_name}: {max_date}')
                else:
                    logging.warning(f"No results found for {cluster_name} max_date query.")
        except Exception as e:
            logging.exception("Error occurred while querying SQL Server for max_date: %s", str(e))

        # have all the logs been loaded into Vertica for yesterday's data?
        max_date_is_yesterday = max_date == yesterday
        logging.info(f"max_date_is_yesterday: {max_date_is_yesterday}")

        if not max_date_is_yesterday:
            # Slack Alerts for delays at prime, 2, 4, 6, and 8 hours past the delay tolerance time
            # if these prove fruitful, it would make the most sense to turn this into a function because it's used 3x and is quite verbose
            if datetime.utcnow() >= db_delayed_tolerance_time and datetime.utcnow() < (db_delayed_tolerance_time +
                                                                                       timedelta(hours=2)) and not has_alerted_provdb_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's past the delay tolerance time {db_delayed_tolerance_time.strftime('%H:%M UTC')} and data for {cluster_name} has not been fully loaded according to _ProvDb_. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_provdb_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_provdb_2hr_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's been 2 hours since the previous alert and the data for {cluster_name} is still not fully loaded according to _ProvDb_. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_provdb_2hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_provdb_4hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the data for {cluster_name} is still not fully loaded according to _ProvDb_. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_provdb_4hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_provdb_6hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the data for {cluster_name} is still not fully loaded according to _ProvDb_. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_provdb_6hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_provdb_8hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the data for {cluster_name} is still not fully loaded according to _ProvDb_. Please check #dev-vertica. *This is the final alert I will send about the {cluster_name} ProvDB check* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_provdb_8hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 1800
                time_module.sleep(sleep_time)

            current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            logging.warning(f"{current_timestamp}: ProvDb says {cluster_name} data is not ready.")
            logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
            time_module.sleep(sleep_time)

    logging.info(f"ProvDB says {cluster_name} data is ready.")

    logging.info("Entering Vertica loop")
    while not matching:
        sleep_time = 900  # 15 minutes
        try:
            with vertica_python.connect(**vertica_conn_info) as conn:
                logging.info(f"Going to check if {cluster_name} agg tables are ready")
                cur = conn.cursor('dict')
                cur.execute(autoscheduler_queries.vertica_agg_data_ready_query)
                res = cur.fetchall()
                matching = res[0]['Matching']
                logging.info(
                    f"Vertica BI Daily Agg Result: {matching}"
                )  # Do DailyAgg/BI schema tables match reports schema tables in Vertica [True/False]
        except Exception as e:
            logging.exception(f"Error occurred while checking {cluster_name} agg tables: ", str(e))

        if not matching:
            # Slack Alerts for delays at prime, 2, 4, 6, and 8 hours past the delay tolerance time
            if datetime.utcnow() >= db_delayed_tolerance_time and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=2)) and not has_alerted_vertica_delay and not has_alerted_provdb_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's past the delay tolerance time {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables are not ready in {cluster_name}. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_vertica_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow(
            ) < (db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_vertica_2hr_delay and not has_alerted_provdb_2hr_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's 2 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables in {cluster_name} are still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_vertica_2hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow(
            ) < (db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_vertica_4hr_delay and not has_alerted_provdb_4hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables in {cluster_name} are still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_vertica_4hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow(
            ) < (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_vertica_6hr_delay and not has_alerted_provdb_6hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables in {cluster_name} are still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_vertica_6hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_vertica_8hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables for {cluster_name} are still not ready. Please check #dev-vertica. *This is the final alert I will send about BI Daily Agg Tables in {cluster_name}* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_vertica_8hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 1800
                time_module.sleep(sleep_time)

            current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            logging.warning(f"{current_timestamp}: Vertica says BI Daily Agg tables on {cluster_name} are not ready.")
            logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
            time_module.sleep(sleep_time)

    logging.info("Entering Attribution loop")
    attr_check = False
    sleep_time = 900
    while not attr_check:
        try:
            with vertica_python.connect(**vertica_conn_info) as conn:
                logging.info(f"Checking if Attribution Data for {cluster_name} is complete for the day")
                cur = conn.cursor('dict')
                cur.execute(autoscheduler_queries.vertica_attribution_data_ready_query)
                res = cur.fetchall()
                attr_check = res[0]['attributionDataExists']
        except Exception as e:
            logging.exception(f"Error occurred while checking {cluster_name} attribution split: ", str(e))
        if not attr_check:
            if datetime.utcnow() >= db_delayed_tolerance_time and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=2)) and not has_alerted_attribution_split_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's past the delay tolerance time {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_attribution_split_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_attribution_split_2hr_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's 2 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution  data for {cluster_name} is still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_attribution_split_2hr_delay = True
                sleep_time = 600
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_attribution_split_4hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_attribution_split_4hr_delay = True
                sleep_time = 600
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_attribution_split_6hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_attribution_split_6hr_delay = True
                sleep_time = 600
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_attribution_split_8hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is still not ready. Please check #dev-vertica. *This is the final alert I will send about the {cluster_name} Attribution Split data* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_attribution_split_8hr_delay = True
                sleep_time = 1800
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)

            current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            logging.warning(f"{current_timestamp}: Vertica says Attribution data for {cluster_name} is not ready.")
            logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
            time_module.sleep(sleep_time)

    logging.info(f"{cluster_name} Attribution Data is ready.")
    logging.info("Vertica says the data is ready.")

    # Data is ready
    if max_date_is_yesterday and matching and attr_check:
        database_ready_time = datetime.utcnow()
        db_success_message = f'{cluster_name} data is ready at {database_ready_time.strftime("%Y-%m-%d %H:%M")} UTC :check_green:'
        logging.info(db_success_message)
        send_slack_alert(db_success_message)

        # xcom to push truth checks in subsequent tasks
        max_date_plus_one = max_date + timedelta(days=1)
        kwargs['ti'].xcom_push(key='db_max_date', value=max_date_plus_one)
        logging.info(f"xcom_push [db_max_date]: {max_date_plus_one}")
        kwargs['ti'].xcom_push(key='database_ready_time', value=database_ready_time)
        logging.info(f"xcom_push [database_ready_time]: {database_ready_time}")


# Check if EDW databases are ready
def is_edw_ready(db, **kwargs):
    context = kwargs.get('context')
    logging.info(f"kwargs: {kwargs}")
    logging.info(f"context: {context}")
    if db == 'ttd_tableau_snowflake_prod':
        connection_info = get_db_connection('ttd_tableau_snowflake_prod')
        query = autoscheduler_queries.snow_edw_prod_max_date
        db_name = 'Snowflake-Prod'
        connector = snowflake.connector.connect
        xcoms_source = 'is_edw_snowflake_ready'
        threshold_delta = timedelta(hours=1)

    elif db == 'autoscheduler_edw_snowflake_dev':
        connection_info = get_db_connection('autoscheduler_edw_snowflake_dev')
        query = autoscheduler_queries.snow_edw_prod_max_date  # there isn't one yet
        db_name = 'Snowflake-Dev'
        connector = snowflake.connector.connect
        xcoms_source = 'is_edw_snowflake_dev_ready'
        threshold_delta = timedelta(hours=1)

    else:
        logging.error("Invalid DB parameter provided.")
        return

    # setting threshold for when to signal that an EDW DB is delayed
    database_ready_time = kwargs['ti'].xcom_pull(task_ids='is_vertica_east_ready', key='database_ready_time')
    logging.info(f"xcom_pull [database_ready_time]: {database_ready_time}")
    db_delayed_tolerance_time = database_ready_time + threshold_delta
    logging.info(f"db_delayed_tolerance_time: {db_delayed_tolerance_time}")

    # Initialize slack alert tracking variables
    has_alerted_edw_delay = False
    has_alerted_edw_2hr_delay = False
    has_alerted_edw_4hr_delay = False
    has_alerted_edw_6hr_delay = False
    has_alerted_edw_8hr_delay = False

    # Loop Init
    sleep_time = 900  # 15 minutes
    today_date = datetime.utcnow().date()
    yesterday = today_date - timedelta(days=1)
    max_date = None
    max_date_is_yesterday = False

    logging.info(f'Entering a loop that checks {db_name} data to see if all data has been loaded for the day and is ready to be queried')
    while not max_date_is_yesterday:
        try:
            with connector(**connection_info) as conn:
                logging.info(f'Establishing {db_name} connection')
                cur = conn.cursor()
                cur.execute(query)
                max_date = cur.fetchone()[0]
                logging.info(f'grabbed the max_date from {db_name}: {max_date}')

        except Exception as e:
            logging.exception(str(e))
            raise Exception(f"Error occurred in retrieving max_date from {db_name}: {str(e)}")

        max_date_is_yesterday = max_date == yesterday

        if not max_date_is_yesterday:
            # Slack Alerts for delays at prime, 2, 4, 6, and 8 hours past the delay tolerance time
            if datetime.utcnow() >= db_delayed_tolerance_time and not has_alerted_edw_delay:
                db_delayed_message = f":warning: *{db_name} DELAY ALERT* - It's past the delay threshold time {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                has_alerted_edw_delay = True
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_edw_2hr_delay:
                db_delayed_message = f":warning: *{db_name} DELAY ALERT* - It's 2 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_edw_2hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_edw_4hr_delay:
                db_delayed_message = f":alert: *{db_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. Please ask in #techops-edw. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_edw_4hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_edw_6hr_delay:
                db_delayed_message = f":alert: *{db_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. Please ask in #techops-edw. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_edw_6hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_edw_8hr_delay:
                db_delayed_message = f":alert: *{db_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. Please ask in #techops-edw. *This is the final alert I will send about {db_name}* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_alert(db_delayed_message)
                has_alerted_edw_8hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)

            current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            logging.warning(f"{current_timestamp}: {db_name} Data is not complete. Waiting for 15 minutes before trying again.")
            time_module.sleep(900)

    if max_date_is_yesterday:
        database_ready_time = datetime.utcnow()
        db_success_message = f'{db_name} data is ready at {database_ready_time.strftime("%Y-%m-%d %H:%M")} UTC :check_green:'
        logging.info(db_success_message)
        send_slack_alert(db_success_message)

        # Calculate max_date_plus_one after the loop completes successfully
        max_date_plus_one = max_date + timedelta(days=1)

        # Pushing data to xcoms to use in following tasks
        kwargs['ti'].xcom_push(key='db_max_date', value=max_date_plus_one)
        logging.info(f"xcom_push [db_max_date]: {max_date_plus_one}")
        kwargs['ti'].xcom_push(key='database_ready_time', value=database_ready_time)
        logging.info(f"xcom_push [database_ready_time]: {database_ready_time}")


# Update DATABASE_READINESS_CHECK for any DB that is ready
def db_ready(db, **kwargs):
    # Configuration based on the `db` parameter
    if db == 'vertica_us_east':
        snowflake_column = 'VERTICA_EAST'
        xcoms_source = 'is_vertica_east_ready'
    elif db == 'vertica_us_west':
        snowflake_column = 'VERTICA_WEST'
        xcoms_source = 'is_vertica_west_ready'
    elif db == 'ttd_tableau_snowflake_prod':
        snowflake_column = 'EDW_SNOWFLAKE'
        xcoms_source = 'is_edw_snowflake_ready'
    elif db == 'autoscheduler_edw_snowflake_dev':
        snowflake_column = 'not live as of now, just setting up the pipe in case we need it'
        xcoms_source = 'is_edw_snowflake_dev_ready'
    else:
        logging.error("Invalid DB parameter provided.")
        return

    # Receiving xcoms
    context = kwargs.get('context')
    ti = kwargs['ti']
    logging.info(f"kwargs: {kwargs}")
    max_date_plus_one = ti.xcom_pull(task_ids=xcoms_source, key='db_max_date')
    logging.info(f"xcom_pull [db_max_date]: {max_date_plus_one}")
    database_ready_time = ti.xcom_pull(task_ids=xcoms_source, key='database_ready_time')
    logging.info(f"xcom_pull [database_ready_time]: {database_ready_time}")
    database_ready_time_insert = database_ready_time.time()
    logging.info(f"database_ready_time_insert: {database_ready_time_insert}")
    query_params = {'log_entry_date_utc': max_date_plus_one, 'database_ready_time': database_ready_time_insert}
    logging.info(f"query_params: {query_params}")

    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_cur = sf_conn.cursor()
            # Check if the data has already been marked as ready. If so, I dead this function fam
            logging.debug(
                "Checking DATABASE_READINESS_CHECK to make sure I haven't previously done this and accidentally overwrite true timestamps"
            )
            db_ready_check_query = autoscheduler_queries.db_ready_idempotency_check.format(snowflake_column=snowflake_column)
            db_ready_date = sf_cur.execute(db_ready_check_query, query_params).fetchone()

            if db_ready_date and db_ready_date[0] == max_date_plus_one:
                logging.error(
                    f"{max_date_plus_one} has previously been updated in DATABASE_READINESS_CHECK for {snowflake_column}. Skipping the rest of this task."
                )
                return

            logging.debug(f"Updating the DATABASE_READINESS_CHECK for {snowflake_column} readiness...")
            # Using f-string to dynamically construct the SQL query + a parameter for max_date
            readiness_column = f"IS_{snowflake_column}_READY"
            readiness_time_column = f"{snowflake_column}_READY_TIME_UTC"

            db_ready_update_query = autoscheduler_queries.db_ready_update.format(
                readiness_column=readiness_column, readiness_time_column=readiness_time_column
            )
            sf_cur.execute(db_ready_update_query, query_params)
            sf_conn.commit()

        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred while updating the DATABASE_READINESS_CHECK table: {}".format(str(e)))


# Update DATABASE_READINESS_CHECK with the time that all data became ready
def all_db_ready(**kwargs):
    # Receiving xcoms
    ti = kwargs['ti']
    log_entry_date_utc = ti.xcom_pull(task_ids='tableau_job_flow_load', key='tableau_job_flow_log_entry_date_utc')
    logging.info(f"xcom_pull [tableau_job_flow_log_entry_date_utc]: {log_entry_date_utc}")
    query_param_log_entry_date_utc = {'log_entry_date_utc': log_entry_date_utc}

    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_cur = sf_conn.cursor()
            logging.debug("Checking DATABASE_READINESS_CHECK for idempotency")
            idempotency_check = sf_cur.execute(autoscheduler_queries.all_db_ready_idempotency_check,
                                               query_param_log_entry_date_utc).fetchone()

            if idempotency_check and idempotency_check[0] is not None:
                logging.error(
                    f"{log_entry_date_utc} has previously been updated in DATABASE_READINESS_CHECK for ALL_DATA_READY_TIME_UTC. Skipping the rest of this task."
                )
                return

            # Check to see if all DBs have been marked ready so we can insert the final timestamp into DATABASE_READINESS_CHECK
            logging.debug("Checking DATABASE_READINESS_CHECK to see if all data has been marked ready")
            validity_check = sf_cur.execute(autoscheduler_queries.all_db_ready_validity_check, query_param_log_entry_date_utc).fetchone()

            if validity_check and validity_check[0]:
                logging.debug("All databases are marked ready. Continuing with the rest of the script...")
            else:
                logging.debug("Not all databases are marked ready. Skipping the rest of this task.")
                return

            logging.debug(f"Updating the ALL_DATA_READY_TIME_UTC in DATABASE_READINESS_CHECK")
            sf_cur.execute(autoscheduler_queries.all_db_ready_update, query_param_log_entry_date_utc)
            sf_conn.commit()

        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred while updating ALL_DATA_READY_TIME_UTC in DATABASE_READINESS_CHECK table: {}".format(str(e)))


# Flag Tableau tasks that are ready to run on Tableau Server in TABLEAU_JOB_HISTORY
def tableau_job_is_ready(**kwargs):
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_cur = sf_conn.cursor()
            logging.debug("Updating the TABLEAU_JOB_HISTORY with whatever can be marked IS_READY_TO_GO...")
            sf_cur.execute(autoscheduler_queries.tableau_job_is_ready_update)
            sf_conn.commit()
        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred while updating TABLEAU_JOB_HISTORY: {}".format(str(e)))


# Function to load tasks into TABLEAU_JOB_ACTIVE_QUEUE so BI AutoScheduler can pick them up
def tableau_job_active_queue(**kwargs):
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            logging.debug("Loading the TABLEAU_JOB_ACTIVE_QUEUE with whatever can be queued for refresh..")
            sf_conn.cursor().execute(autoscheduler_queries.tableau_job_active_queue_insert)
            sf_conn.commit()

        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred while updating TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


#########################
# Warp Fast Pass Lane   #
#########################


def get_fast_passed_dashboards():
    try:
        log_prefix = '[warp_fast_pass lane - stats_collector]'
        autoscheduler_edw_snowflake_dev_conn_info = get_db_connection("autoscheduler_edw_snowflake_dev")
        autoscheduler_edw_snowflake_prod_conn_info = get_db_connection("autoscheduler_edw_snowflake_prod")
        with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            cursor = sf_conn.cursor()
            fast_passed_dash = cursor.execute(autoscheduler_queries.warp_dash_fast_passed).fetchone()[0]
            logging.info(f'{log_prefix} Number of dashboards that have been fast-passed: {fast_passed_dash}')
        return fast_passed_dash
    except Exception as e:
        logging.exception(f"Error occurred in get_fast_passed_dashboards(): {str(e)}")
        return 0


def warp_fast_pass(**kwargs):
    # check if tableau job history operator is complete
    try:
        log_prefix = '[warp_fast_pass lane - poller]'
        slack_msg = ':checkered_flag: *Warp Fast Pass Lane* :checkered_flag:  - Starting the warp fast pass lane'
        send_slack_alert(slack_msg)
        autoscheduler_edw_snowflake_dev_conn_info = get_db_connection("autoscheduler_edw_snowflake_dev")
        luid_name_mapper = {}
        autoscheduler_edw_snowflake_prod_conn_info = get_db_connection("autoscheduler_edw_snowflake_prod")
        with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            logging.info(f'{log_prefix} Building luid-name mapper')
            cursor = sf_conn.cursor()
            res = cursor.execute(autoscheduler_queries.luid_name_mapping).fetchall()
            for row in res:
                luid_name_mapper[row[0]] = row[1]
            logging.info(f'{log_prefix} luid-name mapper built')
            logging.info(f'{log_prefix} Luid Name Mapper: {luid_name_mapper}')
            logging.info(f'{log_prefix} Counting Warp Dashboards that are to be considered for fast-pass...')
            cursor = sf_conn.cursor()
            dash_count = cursor.execute(autoscheduler_queries.warp_dash_count).fetchone()[0]
            # logging.info(f"Number of dashboards to fast-pass: {dash_count}")
            #dash_count = 14
        fast_passed = get_fast_passed_dashboards()

        while (fast_passed < dash_count) and (datetime.utcnow().time() < time(23, 30)):
            try:
                logging.info("Checking warp dash ready for refesh....")
                with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
                    cursor = sf_conn.cursor()
                    res = cursor.execute(autoscheduler_queries.warp_mark_ready).fetchall()
                    if len(res) > 0:
                        logging.info(f"{log_prefix} - Marked {len(res)} warp dashboards as ready for refresh.")
                        # include a race car at the end of the message
                        msg = f":checkered_flag: *Warp Fast Pass Lane* :checkered_flag: - Marked these warp dashboards as ready for refresh "
                        send_slack_alert(msg)
                        for row in res:
                            msg = f"Dashboard {luid_name_mapper[row[0]]} :racing_car: \n "
                            send_slack_alert(msg)
                # call function to pass to active queue
                tableau_job_active_queue()
                time_module.sleep(15)
                fast_passed = get_fast_passed_dashboards()

            except Exception as e:
                logging.exception(f"Error occurred in warp_fast_pass() loop: {str(e)}")
                continue
        logging.info(f"{log_prefix} Warp Fast Pass Lane complete")
        end_msg = ":checkered_flag: *Warp Fast Pass Lane* :checkered_flag: - Warp Fast Pass Lane complete"
        send_slack_alert(end_msg)
    except Exception as e:
        logging.exception(f"Error occurred in warp_fast_pass(): {str(e)}")


##########################
# BI AutoScheduler Stack #
##########################
def autsch_art():
    art = """

    ____  ____   ___         __       _____      __             __      __         
   / __ )/  _/  /   | __  __/ /_____ / ___/_____/ /_  ___  ____/ /_  __/ /__  _____
  / __  |/ /   / /| |/ / / / __/ __ \\__ \/ ___/ __ \/ _ \/ __  / / / / / _ \/ ___/
 / /_/ // /   / ___ / /_/ / /_/ /_/ /__/ / /__/ / / /  __/ /_/ / /_/ / /  __/ /    
/_____/___/  /_/  |_\__,_/\__/\____/____/\___/_/ /_/\___/\__,_/\__,_/_/\___/_/     
                                                                                   

    """
    print(art)


#######################
# Snowflake Functions #
#######################


# Function to retrieve pertinent metadata for BI AutoScheduler to use
def autoscheduler_result_fetch():
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            logging.debug("Querying for the result of all the jobs ran on Tableau Server from AUTOSCHEDULER.TABLEAU_JOB_HISTORY")
            cursor = sf_conn.cursor()
            cursor.execute(autoscheduler_queries.autoscheduler_result)
            autoscheduler_result = cursor.fetchone()

            if autoscheduler_result is None:
                raise Exception("No results found for autoscheduler_result query")

            # Extract values from the result tuple
            total_job_count, successful_job_count, failed_job_count, cancelled_job_count, skipped_job_count = autoscheduler_result

            result_summary = {
                "total_job_count": total_job_count,
                "successful_job_count": successful_job_count,
                "failed_job_count": failed_job_count,
                "cancelled_job_count": cancelled_job_count,
                "skipped_job_count": skipped_job_count
            }

        except Exception as e:
            logging.exception(str(e))
            raise Exception("autoscheduler_result_fetch(): Error occurred: {}".format(str(e)))

    return result_summary


# Retrieve how many jobs have been executed today to feed the while loop in the case of task retries
def executed_jobs_today():
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            # Retrieving all task_luids that have been executed today
            logging.debug("Querying for all task_luids that have been executed today from AUTOSCHEDULER.TABLEAU_JOB_HISTORY")
            result = sf_conn.cursor().execute(autoscheduler_queries.executed_jobs_today).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logging.exception(str(e))
            raise Exception(
                "executed_jobs_today(): Error occurred in retrieving the total job count from TABLEAU_JOB_HISTORY in Snowflake: {}"
                .format(str(e))
            )


def retrieve_in_progress_jobs(task_luid=None):
    log_prefix = "[JOB RETRIEVER]"

    if task_luid:
        query_params = {'task_luid': task_luid}
        in_progress_jobs_query = autoscheduler_queries.in_progress_task
        logging.info(f"{log_prefix}: Retrieving JOB_ID from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake for TASK_LUID = {task_luid}")
    else:
        in_progress_jobs_query = autoscheduler_queries.in_progress_jobs
        logging.info(f"{log_prefix}: Retrieving in-progress jobs from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake.")

    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            retrieve_jobs = sf_conn.cursor().execute(in_progress_jobs_query)
            in_progress_jobs = []
            for row in retrieve_jobs:
                jobs_dict = {'LOG_ENTRY_DATE_UTC': row[0], 'TASK_LUID': row[1], 'TASK_TARGET_NAME': row[2], 'JOB_ID': row[3]}
                in_progress_jobs.append(jobs_dict)
        except Exception as e:
            logging.error(str(e))
            raise Exception(f"{log_prefix}: Error occurred in retrieving jobs from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake: {str(e)}")

    return in_progress_jobs


# Retrieve a task from TABLEAU_JOB_ACTIVE_QUEUE for BI AutoScheduler to run
def retrieve_cold_task():
    log_prefix = '[COLD TASK]'
    #stop_time = time(23, 15)  # should consider making this global but need to scope it properly
    global stop_time
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        while True:
            try:
                current_time = datetime.utcnow().time()

                if current_time >= stop_time:
                    logging.error(f"{log_prefix}: Current time is past stop time. Stopping BI AutoScheduler.")
                    #raise RuntimeError("Stop time reached in retrieve_cold_task()")
                    return -1

                # Retrieve a single task to pass into autsch so we can run the refresh
                logging.debug("Querying for a new task from TABLEAU_JOB_ACTIVE_QUEUE...")
                retrieve_task = sf_conn.cursor().execute(autoscheduler_queries.cold_task_fetch)

                cold_task = []
                for row in retrieve_task:
                    cold_task_dict = {
                        'LOG_ENTRY_DATE_UTC': row[0],
                        'TASK_OBJECT_TYPE': row[1],
                        'TASK_LUID': row[2],
                        'TASK_TARGET_NAME': row[3],
                        'QUEUE_POSITION': row[4],
                        'IS_HOT': row[5]
                    }
                    cold_task.append(cold_task_dict)

                if cold_task:
                    return cold_task  # Exit the loop and function if a task is found

                # If no tasks found, wait for 10 minutes before checking again
                logging.warning(f"{log_prefix}: No task available! Waiting 10 minutes before checking again.")
                time_module.sleep(600)
                logging.info(f"{log_prefix}: Waited 10 minutes, attempting to retrieve a task again.")

            except Exception as e:
                logging.error(str(e))
                raise Exception(f"{log_prefix}: Error occurred in retrieving a task from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake: ", str(e))


# Flag a retrieved task as "hot" in TABLEAU_JOB_ACTIVE_QUEUE
def hot_task_update(task_pickup_timestamp, gate_elapsed_time_in_seconds, task_luid):
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        query_params = {
            'task_pickup_timestamp': task_pickup_timestamp,
            'gate_elapsed_time_in_seconds': gate_elapsed_time_in_seconds,
            'task_luid': task_luid
        }
        try:
            sf_conn.cursor().execute(autoscheduler_queries.is_hot_true_update, query_params)
            sf_conn.commit()
            logging.info(f"hot_task_update(): successful for TASK_LUID = {task_luid}")
        except Exception as e:
            logging.exception(str(e))
            raise Exception("hot_task_update(): Error occurred updating IS_HOT in TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


# Log the Job ID returned from Tableau Server into TABLEAU_JOB_HISTORY and increase ATTEMPT_COUNT by 1
def job_id_update(job_id, task_luid):
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    # Parameters for the query
    job_and_task_ids = {'job_id': job_id, 'task_luid': task_luid}

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.job_id_update, job_and_task_ids)
            sf_conn.commit()
            logging.info(f"job_id_update(): succesful for TASK_LUID = {task_luid}")
        except Exception as e:
            logging.exception(str(e))
            raise Exception("job_id_update(): Error occurred updating JOB_ID in TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


# Log the result of a job into TABLEAU_JOB_ACTIVE_QUEUE
def log_job_result(job_result_dict):
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    # Parameters for the query
    job_result_query_params = {
        'job_created_at': job_result_dict['job_created_at'],
        'job_started_at': job_result_dict['job_started_at'],
        'job_completed_at': job_result_dict['job_completed_at'],
        'job_result_code': job_result_dict['job_result_code'],
        'job_result': job_result_dict['job_result'],
        "job_result_message": job_result_dict['job_result_message'],
        'task_luid': job_result_dict['task_luid']
    }

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.update_job_with_result, job_result_query_params)
            sf_conn.commit()
        except Exception as e:
            logging.exception(str(e))
            raise Exception(f"log_job_result(): Error occurred in trying to log job result to Snowflake: {str(e)}")

    logging.info(f"log_job_result(): succesful for TASK_LUID = {job_result_dict['task_luid']}")


# Update TABLEAU_JOB_HISTORY with the results of all jobs that were executed today
def job_history_update():
    # fetch DB connection info
    autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.update_job_history_with_results)
            sf_conn.commit()
            logging.info(f"job_history_update(): successful!")
        except Exception as e:
            logging.exception(str(e))
            raise Exception(
                "job_history_update(): Error occurred updating TABLEAU_JOB_HISTORY with all job results for the day: {}".format(str(e))
            )


def snowflake_daily_wrap_up(**kwargs):
    current_time = datetime.utcnow().time()
    logging.info("Starting Snowflake daily wrap-up, this is a 3 step process.")
    logging.info("Step 1: Calling in the Job Resolver to see if there are any jobs that need to be resolved.")
    job_resolver()
    logging.info("Step 1: Complete!")
    logging.info("Step 2: Updating TABLEAU_JOB_HISTORY with the results of all jobs that were executed today.")
    job_history_update()
    logging.info("Step 2: Complete!")
    logging.info("Step 3: Updating DATABASE_READINESS_CHECK with the time that all data became ready today.")
    all_db_ready(**kwargs)
    logging.info("Step 3: Complete!")
    logging.info("Snowflake daily wrap-up complete!")


#########################
# Tableau API Functions #
#########################

# Global scope for Tableau API functions
alerted_api_down = False
alerted_api_restored = False
api_status_lock = threading.Lock()
_SERVER_CACHE = {}


# Authenticate to Tableau Server and return the server object
def tableau_server():
    # fetch server creds
    if "server" not in _SERVER_CACHE:
        try:
            tableau_server_conn_info = get_db_connection('tableau_server')
            tableau_auth = TSC.TableauAuth(
                tableau_server_conn_info['username'], tableau_server_conn_info['password'], tableau_server_conn_info['site']
            )
            _SERVER_CACHE["server"] = TSC.Server(tableau_server_conn_info['server'], use_server_version=True)
            _SERVER_CACHE["server"].auth.sign_in(tableau_auth)
        except Exception as e:
            raise e
    return _SERVER_CACHE["server"]


# Decorator to handle errors for all Tableau API functions
def tableau_server_error_handling(func):
    global alerted_api_down, alerted_api_restored, api_status_lock
    log_prefix = "[DECORATOR]"
    MAX_LOGIN_RETRIES = 3
    RETRY_BACKOFF = [120] * 5 + [240, 360, 480, 600]  # 2 minute pause for the first 5 retries, then 4, 6, 8, 10 minutes

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        while retries < MAX_LOGIN_RETRIES:
            server = tableau_server()
            try:
                return func(server, *args, **kwargs)
            except TSC.NotSignedInError:
                error_msg = f"{log_prefix}: I'm not signed in to Tableau Server! Going to authenticate now."
                logging.warning(error_msg)
                _SERVER_CACHE.pop("server", None)
            except (OSError, ConnectionError) as e:
                # Handle these exceptions and attempt re-authentication
                if "Connection aborted" in str(e):
                    error_msg = f"{log_prefix}: Connection aborted! This could be due to a network issue. Attempting to re-authenticate."
                    logging.warning(error_msg)
                    _SERVER_CACHE.pop("server", None)
            except Exception as e:
                if "Missing site ID. You must sign in first." in str(e):
                    error_msg = f"{log_prefix}: Authentication required due to missing site ID. Attempting to re-authenticate."
                    logging.warning(error_msg)
                    _SERVER_CACHE.pop("server", None)

                # Special handling for API version error or API 502 error (Tableau API is completely dark)
                if "not available in API" in str(e) or "Could not get version info from server" in str(
                        e) or "Error status code: 502" in str(e):
                    logging.warning(f"{log_prefix}: API is down or version mismatch. Retrying in 10 minutes.")
                    with api_status_lock:
                        if not alerted_api_down:
                            api_down_slack_alert = (
                                f"Tableau API is down! I encountered the following error: {e}. I'll keep trying to reach the server indefinitely, but please check on the server if this was not expected."
                            )
                            send_slack_alert(api_down_slack_alert)
                            alerted_api_down = True
                    while True:  # Try to authenticate every 10 minutes until successful
                        time_module.sleep(600)  # Sleep for 10 minutes
                        try:
                            tableau_server()
                            logging.info(f"{log_prefix}: Successfully authenticated after API was down.")
                            with api_status_lock:
                                if not alerted_api_restored:
                                    api_restored_message = "Tableau API is back up! I was able to successfully authenticate. Continuing with normal operations."
                                    send_slack_alert(api_restored_message)
                                    alerted_api_restored = True
                            break  # Exit the infinite loop if authentication is successful
                        except Exception as retry_exception:
                            logging.warning(
                                f"{log_prefix}: Failed to authenticate after API was down. Error: {retry_exception}. Retrying in 10 minutes."
                            )
                # For any other unexpected exceptions, raise them immediately without retrying
                else:
                    logging.error(f"{log_prefix}: Unexpected error: {e}")
                    raise

            logging.warning(f"{error_msg}. Trying attempt {retries + 1} of {MAX_LOGIN_RETRIES}.")

            # Implementing exponential backoff
            if retries == 0:
                retry_interval = 0  # No sleep on the first try
            elif retries < len(RETRY_BACKOFF):
                retry_interval = RETRY_BACKOFF[retries]
            else:
                retry_interval = RETRY_BACKOFF[-1]  # After Retry 10, wait 10 mins for the last 5 retries

            if retries > 0:
                logging.info(f"Waiting for {retry_interval} seconds before retrying...")

            time_module.sleep(retry_interval)
            retries += 1

            try:
                tableau_server()
                logging.info(f"Successfully authenticated on attempt {retries}.")
            except Exception as auth_error:
                logging.warning(f"Error during authentication: {auth_error}")
        else:
            logging.error(f"Max retries reached ({MAX_LOGIN_RETRIES}). Unable to authenticate.")
            raise Exception("Exceeded maximum authentication retries while trying to sign in to Tableau Server API.")

    return wrapper


@tableau_server_error_handling
def job_resolver(server):
    global thread_start_times, thread_task_info
    log_prefix = "[JOB RESOLVER]"

    logging.info(f"{log_prefix}: Checking to see if we left any in-progress jobs hanging from any earlier run(s).")
    in_progress_jobs = retrieve_in_progress_jobs()

    if not in_progress_jobs:
        logging.info(f"{log_prefix}: No jobs in need of resolving! We're good to go.")
        return

    logging.info(f"{log_prefix}: Found {len(in_progress_jobs)} job(s) left hanging. Checking Tableau Server for their status.")

    # Making the watcher aware of our presence
    thread_id = threading.get_ident()
    thread_start_times[thread_id] = time_module.time()
    thread_task_info[thread_id] = {'task_name': 'Job Resolver', 'task_luid': 'Job Resolver'}

    # Lists to hold job results
    jobs_on_server_finished = []
    jobs_on_server_failed = []
    jobs_on_server_in_progress = []

    # Mapping for job finish codes
    FINISH_CODE_MAPPING = {0: "Success", 1: "Failure", 2: "Cancelled"}  # -1 = In Progress

    for jobs in in_progress_jobs:
        try:
            job_id = jobs['JOB_ID']
            job_result = server.jobs.get_by_id(job_id)
            job_result_message = None
            # Create a dictionary for the job result
            job_result_dict = {
                "job_created_at": job_result.created_at,
                "job_started_at": job_result.started_at,
                "job_completed_at": job_result.completed_at,
                "job_result_code": job_result.finish_code,
                "job_result": FINISH_CODE_MAPPING.get(job_result.finish_code, "Unknown"),
                "job_result_message": job_result_message,
                "task_luid": jobs['TASK_LUID'],
                "job_id": jobs['JOB_ID']
            }
            # Categorize the job based on its finish code
            if job_result_dict['job_result_code'] == -1:
                jobs_on_server_in_progress.append(job_result_dict)
            elif job_result_dict['job_result_code'] in (1, 2):
                jobs_on_server_failed.append(job_result_dict)
            else:
                jobs_on_server_finished.append(job_result_dict)

        except Exception as e:
            logging.error(f"{log_prefix}: Error retrieving status for Job ({job_id}): {str(e)}")

    logging.info(
        f"{log_prefix}: Of all jobs left hanging: {len(jobs_on_server_finished)} job(s) finished, {len(jobs_on_server_failed)} job(s) failed, and {len(jobs_on_server_in_progress)} job(s) still in-progress on Tableau Server."
    )

    if jobs_on_server_finished:
        logging.info(f"{log_prefix}: Logging the results of the finished job(s) to Snowflake.")
        for job in jobs_on_server_finished:
            log_job_result(
                job
            )  # worst case scenario is 6 jobs left hanging that all need to be updated at once. so 1 by 1 update instead of bulk update is ok for now. bigger scale, we can look into bulk update

    if jobs_on_server_failed or jobs_on_server_in_progress:
        logging.info(f"{log_prefix}: Waiting for the in-progress job(s) to finish on Tableau Server.")
        for job_list in [jobs_on_server_failed, jobs_on_server_in_progress]:
            for job in job_list:
                try:
                    updated_job_result = None
                    current_time = datetime.utcnow().time()
                    job_id = job['job_id']
                    while current_time < time(23, 30):
                        try:
                            updated_job_result = server.jobs.wait_for_job(job_id, timeout=300)
                            break
                        # except Exception as e:
                        #     if "Timeout" in str(e):
                        #         logging.warning(f"{log_prefix}: Timeout while waiting for Job ({job_id}). Retrying...")
                        #         current_time = datetime.utcnow().time()
                        #         continue
                        #     else:
                        #         logging.error(f"{log_prefix}: Unknown Error while waiting for Job ({job_id}): {str(e)}")
                        #         break

                        except Exception as e:
                            if "Timeout" in str(e):
                                logging.warning(f"{log_prefix}: Timeout while waiting for Job ({job_id}). Retrying...")
                                current_time = datetime.utcnow().time()
                                continue
                            if "TableauRuntimeException" in str(e):
                                logging.warning(f"Job ({job_id}) kicked JobFailedException. Pinging Tableau Server for full details.")
                                time_module.sleep(15)  # Wait 15 seconds before pinging Tableau Server again
                                updated_job_result = server.jobs.get_by_id(job_id)
                                entire_message = str(e)
                                start_idx = entire_message.find("TableauRuntimeException")
                                if start_idx != -1:
                                    job_result_message = entire_message[start_idx + len("TableauRuntimeException: "):]
                                else:
                                    job_result_message = entire_message[:1024
                                                                        ]  # Fallback to the entire message if the substring isn't found
                                # various cleaning steps to make the message more readable, less verbose, and clean to log into Snowflake
                                job_result_message = job_result_message.replace("\\n", " ")
                                job_result_message = re.sub(r'tableau_error_source=.*?0x[0-9a-fA-F]+', '', job_result_message)
                                job_result_message = re.sub(' +', ' ', job_result_message)
                                job_result_message = job_result_message.lstrip()
                                job_result_message = job_result_message[:1024]
                                # Update the job dictionary with the new results
                                job['job_created_at'] = updated_job_result.created_at
                                job['job_started_at'] = updated_job_result.started_at
                                job['job_completed_at'] = updated_job_result.completed_at
                                job['job_result_code'] = updated_job_result.finish_code
                                job['job_result'] = FINISH_CODE_MAPPING.get(updated_job_result.finish_code, "Unknown")
                                job['job_result_message'] = job_result_message
                                log_job_result(job)
                                break
                            else:
                                logging.error(f"{log_prefix}: Unknown Error while waiting for Job ({job_id}): {str(e)}")
                                break
                    if updated_job_result:
                        # Update the job dictionary with the new results
                        job['job_created_at'] = updated_job_result.created_at
                        job['job_started_at'] = updated_job_result.started_at
                        job['job_completed_at'] = updated_job_result.completed_at
                        job['job_result_code'] = updated_job_result.finish_code
                        job['job_result'] = FINISH_CODE_MAPPING.get(updated_job_result.finish_code, "Unknown")
                        log_job_result(job)
                except Exception as e:
                    logging.error(f"{log_prefix}: Error while waiting for Job ({job_id}): {str(e)}")
    logging.info(f"{log_prefix}: Job Resolver has finished!")
    future_truncator(thread_id)


# Check how many backgrounders are currently in use on Tableau Server
@tableau_server_error_handling
def get_active_backgrounders_count(server):
    try:
        req = TSC.RequestOptions()
        req.filter.add(TSC.Filter("status", TSC.RequestOptions.Operator.Equals, 'InProgress'))
        count = len(list(TSC.Pager(server.jobs, request_opts=req)))
        return count
    except Exception as e:
        logging.error(f"[BACKGROUNDER COUNT]: Error occurred while checking backgrounders: {str(e)}")
        raise e


# Function to gate autsch until backgrounder bandwidth is optimal
def tableau_task_gate():
    # Consistent logging prefix
    log_prefix = "[TASK GATE]"

    backgrounder_count = get_active_backgrounders_count()

    logging.info(f"{log_prefix}: Backgrounders in use: {backgrounder_count}")
    while backgrounder_count > 7:
        current_time = datetime.now(timezone.utc)
        if current_time.time() >= stop_time:
            logging.warning(f"{log_prefix}: Current time is past stop time. Stopping Task Gating")
            return -1
        logging.warning(f'{log_prefix}: All backgrounders currently in use! Waiting 30 seconds.')
        time_module.sleep(30)
        backgrounder_count = get_active_backgrounders_count()
        logging.info(f"{log_prefix}: Backgrounders in use: {backgrounder_count}")


# Execute a task on Tableau Server
@tableau_server_error_handling
def tableau_task_runner(server, task_name, task_luid, thread_id, thread_name):
    # Consistent logging prefix
    global shutdown_event
    global stop_time
    log_prefix = f"[TASK RUNNER] Future {thread_name} ({thread_id})"
    hot_task = f"Task - [{task_name} ({task_luid})]"

    logging.info(f"{log_prefix}: {hot_task} received by a runner, sending to Tableau Server.")

    # Send the task to Tableau Server
    task = server.tasks.get_by_id(task_luid)
    execute_task = server.tasks.run(task)

    # Parse the XML response since this isn't native for tasks; however, it is native for refreshes ¯\_(ツ)_/¯
    root = ET.fromstring(execute_task)
    namespace = {"ts": "http://tableau.com/api"}
    job_id_element = root.find("ts:job", namespaces=namespace)
    if job_id_element is not None:
        job_id = job_id_element.get("id")
    else:
        logging.warning(
            f"{log_prefix}: Submitted {hot_task} but could not find Job ID in the XML response!! This is unforseen, please investigate."
        )  # This should never happen, but just in case, if it ever does let's refactor this to capture the XML response and log it

    if job_id:
        try:
            job_result_message = None
            job_result = None
            # Update the job_id in TABLEAU_JOB_ACTIVE_QUEUE
            job_id_update(job_id, task_luid)
            # Wait for the job to complete
            logging.info(
                f"{log_prefix}: {hot_task} successfully submitted to Tableau Server! Responded with [Job ID {job_id}]. Going to wait for the job to complete..."
            )
            while not shutdown_event.is_set():
                try:
                    current_time = datetime.utcnow().time()
                    if current_time >= stop_time:
                        logging.error(f"{log_prefix}: It's past stop time. Stopping the runner.")
                        shutdown_event.set()
                        break
                    job_result = server.jobs.wait_for_job(job_id, timeout=300)
                    logging.info(f"{log_prefix}: Job complete for {hot_task}. Reporting back to the orchestrator.")
                    break
                except Exception as e:
                    if "Timeout" in str(e):
                        if not shutdown_event.is_set():
                            logging.warning(f"{log_prefix}: Job ({job_id}) timed out. Retrying...")
                            continue
                        else:
                            logging.error(f"{log_prefix}: Job ({job_id}) timed out. Shutting down.")
                            break
                    if "TableauRuntimeException" in str(e):
                        logging.warning(f"Job ({job_id}) kicked JobFailedException. Pinging Tableau Server for full details.")
                        time_module.sleep(15)  # Wait 15 seconds before pinging Tableau Server again
                        job_result = server.jobs.get_by_id(job_id)
                        entire_message = str(e)
                        start_idx = entire_message.find("TableauRuntimeException")
                        if start_idx != -1:
                            job_result_message = entire_message[start_idx + len("TableauRuntimeException: "):]
                        else:
                            job_result_message = entire_message[:1024]  # Fallback to the entire message if the substring isn't found
                        # various cleaning steps to make the message more readable, less verbose, and clean to log into Snowflake
                        job_result_message = job_result_message.replace("\\n", " ")
                        job_result_message = re.sub(r'tableau_error_source=.*?0x[0-9a-fA-F]+', '', job_result_message)
                        job_result_message = re.sub(' +', ' ', job_result_message)
                        job_result_message = job_result_message.lstrip()
                        job_result_message = job_result_message[:1024]
                        break
                    else:
                        logging.error(f"{log_prefix}: Unknown Error while waiting for Job ({job_id}): {str(e)}")
                        break
            if job_result:
                runner_job_result = {
                    "job_id": job_result.id,
                    "job_type": job_result.type,
                    "job_created_at": job_result.created_at,
                    "job_started_at": job_result.started_at,
                    "job_completed_at": job_result.completed_at,
                    "job_result_code": job_result.finish_code,
                    "job_result_message": job_result_message
                }
                return runner_job_result
            else:
                return None
        except Exception as e:
            logging.error(f"{log_prefix}: Error occurred while waiting for Job ({job_id}): {str(e)}")
            return None
    else:
        return None


##########################################
# Multi-Threaded Orchestration Functions #
##########################################

# Global scope for use in all futures dependent functions
active_futures = []  # track active futures
thread_start_times = {}
thread_task_info = {}
shutdown_event = threading.Event()
MAX_FUTURES = 6  # futures == threads == concurrent tableau jobs
stop_time = time(23, 30)


def future_truncator(thread_id):
    if thread_id in thread_start_times:
        del thread_start_times[thread_id]
    if thread_id in thread_task_info:
        del thread_task_info[thread_id]


# Function to monitor active futures
def future_watcher():
    global active_futures, thread_start_times, thread_task_info
    global shutdown_event
    global stop_time
    current_time = datetime.utcnow().time()
    log_prefix = "[FUTURE WATCHER]"
    while True:
        if current_time >= stop_time:
            logging.error(f"{log_prefix} - It's past the stop time . Stopping the orchestrator.")
            shutdown_event.set()
            return
        # Remove completed futures from the list
        active_futures = [f for f in active_futures if not f.done()]

        # Gather info for all active tasks into a list of dictionaries
        futures_list = []
        for thread_number, (thread_id, start_time) in enumerate(thread_start_times.items(), start=1):
            # Calculate how long the thread has been running
            elapsed_time = time_module.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            formatted_elapsed_time = f"{minutes}:{seconds:02}"

            # Get the associated task info
            futures_info = thread_task_info.get(thread_id, {})

            # Add this task's info to the list
            futures_list.append({
                'thread_number': thread_number,
                'thread_id': thread_id,
                'elapsed_time': formatted_elapsed_time,
                'task_name': futures_info.get('task_name', 'Unknown'),
                'task_luid': futures_info.get('task_luid', 'Unknown')
            })

        # Print the table of task info
        print_futures(futures_list)

        # Print the number of active threads
        active_thread_count = len(active_futures)
        print(f"Active Futures: {active_thread_count}")

        time_module.sleep(60)  # Log every 1 minute


def print_futures(futures_list):
    # Define a format string with columns of a fixed width
    format_string = "{:<6} {:<15} {:<50} {:<30} {:<20}"

    # Print the header
    print(format_string.format("Future", "Future ID", "Task Name", "Task LUID", "Elapsed Time (min)"))
    print("-" * 125)  # Print a line of dashes as a separator

    # Print each row of task info
    for task_info in futures_list:
        print(
            format_string.format(
                task_info['thread_number'], task_info['thread_id'], task_info['task_name'], task_info['task_luid'],
                str(task_info['elapsed_time'])
            )
        )


# Function to start the thread that monitors the futures if it's not already active
def wake_the_future_watcher():
    if not hasattr(wake_the_future_watcher, "thread_activity_thread") or not wake_the_future_watcher.thread_activity_thread.is_alive():
        wake_the_future_watcher.thread_activity_thread = threading.Thread(target=future_watcher)
        wake_the_future_watcher.thread_activity_thread.daemon = True
        wake_the_future_watcher.thread_activity_thread.start()
        logging.info("Future Watcher is not awake! The bell tolls for thee...")
    else:
        logging.info("the future is being watched...")


def tableau_futures_orchestrator(task_name, task_luid, task_pickup_timestamp, gate_elapsed_time_in_seconds, gate_elapsed_time_pretty):
    global thread_start_times, thread_task_info
    global shutdown_event
    global stop_time  # Stop submitting jobs to Tableau after 23:30 UTC
    # Start the thread that monitors the active futures
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    thread_start_times[thread_id] = time_module.time()
    thread_task_info[thread_id] = {'task_name': task_name, 'task_luid': task_luid}
    log_prefix = f"[ORCHESTRATOR] Future [{thread_name} ({thread_id})]"

    try:
        current_time = datetime.utcnow().time()
        if current_time >= stop_time:
            logging.error(f"{log_prefix} - It's past stop_time. Stopping the orchestrator.")
            shutdown_event.set()
            return
        # Future Scope
        wake_the_future_watcher()
        hot_task = f"Task - [{task_name} ({task_luid})]"
        logging.info(f"{log_prefix}: Received {hot_task}")

        logging.info(f"{log_prefix}: Submitting {hot_task} to the runners.")
        job_result = tableau_task_runner(task_name, task_luid, thread_id, thread_name)

        if job_result is None:
            logging.warning(f"{log_prefix}: Received no job result for {hot_task}. This is very unexpected, please investigate.")
            return
        logging.info(f"{log_prefix}: {hot_task} job result received! Logging to Snowflake.")

        # Prepare data for pretty logging and Snowflake insert
        FINISH_CODE_MAPPING = {0: "Success", 1: "Failure", 2: "Cancelled"}
        job_result_dict = {
            "thread_id": thread_id,
            "thread_name": thread_name,
            "future_time": time_module.time() - thread_start_times[thread_id]  # Time on thread
            ,
            "task_name": task_name,
            "task_luid": task_luid,
            "job_id": job_result['job_id'],
            "job_type": job_result['job_type'],
            "task_pickup_time": task_pickup_timestamp,
            "gate_elapsed_time_in_seconds": gate_elapsed_time_in_seconds,
            "gate_elapsed_time_pretty": gate_elapsed_time_pretty,
            "job_created_at": job_result['job_created_at'],
            "job_started_at": job_result['job_started_at'],
            "job_completed_at": job_result['job_completed_at'],
            "job_result_code": job_result['job_result_code'],
            "job_result": FINISH_CODE_MAPPING.get(job_result['job_result_code'], "Unknown"),
            "job_result_message": job_result['job_result_message']
        }
        job_result_pretty = f"""
        +---------------------------------------------+
        | Future ID: {job_result_dict['thread_id']}
        | Future Name: {job_result_dict['thread_name']}
        | Futured Time: {job_result_dict['future_time']:.2f}
        | Task Name: {job_result_dict['task_name']}
        | Task ID: {job_result_dict['task_luid']}
        | Job ID: {job_result_dict['job_id']}
        | Job Type: {job_result_dict['job_type']}
        | Task Pickup Time: {job_result_dict['task_pickup_time']}
        | Gate Elapsed Time: {job_result_dict['gate_elapsed_time_pretty']}
        | Job Created At: {job_result_dict['job_created_at']}
        | Job Started At: {job_result_dict['job_started_at']}
        | Job Completed At: {job_result_dict['job_completed_at']}
        | Job Result Code: {job_result_dict['job_result_code']}
        | Job Result: {job_result_dict['job_result']}
        | Job Result Message: {job_result_dict['job_result_message']}
        +---------------------------------------------+
        """
        logging.info(job_result_pretty)
        log_job_result(job_result_dict)

    except Exception as e:
        logging.error(f"{log_prefix}: Error encountered in task {hot_task}")
        logging.exception(e)

    finally:
        future_truncator(thread_id)


##############################
# BI AutoScheduler Vena Cava #
##############################
def bi_autoscheduler(**kwargs):
    global active_futures
    global shutdown_event
    global stop_time  # Stop submitting jobs to Tableau after 23:30 UTC
    log_prefix = "[BI AUTOSCHEDULER]"
    autsch_art()

    # Receive what the total job count is for the day
    ti = kwargs['ti']
    TOTAL_JOB_COUNT_FOR_DAY = ti.xcom_pull(task_ids='autsch', key='total_job_count_for_day')
    if TOTAL_JOB_COUNT_FOR_DAY is None:
        logging.info(f"{log_prefix}: TOTAL_JOB_COUNT_FOR_DAY is not available in xcom! Fetching from Snowflake.")
        TOTAL_JOB_COUNT_FOR_DAY = autoscheduler_result_fetch()['total_job_count']
        ti.xcom_push(key='total_job_count_for_day', value=TOTAL_JOB_COUNT_FOR_DAY)
    logging.info(f"{log_prefix}: Total Job Count = {TOTAL_JOB_COUNT_FOR_DAY}")

    # when to notify that we've reached the halfway point
    halfway_point = TOTAL_JOB_COUNT_FOR_DAY // 2

    # Initialize executed_jobs with tasks we've already sent to Tableau Server today
    executed_jobs = set()  # Changing O(N) to O(1) for lookup using set instead of list
    if not executed_jobs:
        executed_jobs.update(executed_jobs_today())
        logging.info(f"{log_prefix}: Total Executed Jobs = {len(executed_jobs)}")
    if len(executed_jobs) > 0:
        logging.info(f"{log_prefix}: This is a rerun! Invoking the Job Resolver to check on the jobs we ran earlier today.")
        resolver_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        resolver_future = resolver_executor.submit(job_resolver)
        active_futures.append(resolver_future)
        resolver_executor.shutdown(wait=False)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FUTURES) as executor:
        while len(executed_jobs) < TOTAL_JOB_COUNT_FOR_DAY:
            # Check the current time against the stop_time
            current_time = datetime.utcnow().time()
            if current_time >= stop_time:
                current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                logging.warning(
                    f"{log_prefix}: Current time is {current_timestamp} which is past stop time of {stop_time}. Stopping BI AutoScheduler Vena Cava. Orchestrator will remain alive."
                )
                shutdown_event.set()
                executor.shutdown(wait=False, cancel_futures=True)
                break
            if len(executed_jobs) == halfway_point:
                current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                logging.info(f"{log_prefix}: Halfway Point reached!- {len(executed_jobs)} of {TOTAL_JOB_COUNT_FOR_DAY} Tasks Exectued")
                send_slack_alert(
                    f"We have reached the halfway point at {current_timestamp} UTC! {len(executed_jobs)} of {TOTAL_JOB_COUNT_FOR_DAY} tasks have been executed. If you'd like to see my progress, please #visit the Snowflake workbook in the _Canvas_ of this channel :thumbsup:"
                )
                halfway_point = -1  # Only send the halfway point alert once
            # Main Block
            if len(active_futures) < MAX_FUTURES:
                try:
                    logging.info(f"{log_prefix}: Progress Check-In - {len(executed_jobs)} of {TOTAL_JOB_COUNT_FOR_DAY} Tasks Exectued")
                    logging.info(f"{log_prefix}: Waiting at the gate...")
                    gate_entry_time = time_module.time()
                    tableau_task_gate()
                    gate_elapsed_time = time_module.time() - gate_entry_time
                    minutes = int(gate_elapsed_time // 60)
                    seconds = int(gate_elapsed_time % 60)
                    gate_elapsed_time_in_seconds = int(gate_elapsed_time)
                    gate_elapsed_time_pretty = f"{minutes}:{seconds:02}"
                    logging.info(
                        f"{log_prefix}: Backgrounder(s) available! Opening the gate ..Gate Elapsed Time: {gate_elapsed_time_pretty} minute(s)"
                    )
                except Exception as e:
                    logging.error(f"{log_prefix}: Error occurred in tableau_task_gate(): {str(e)}")
                    err_msg = f":alert: Error occurred in tableau_task_gate(): {str(e)}"
                    send_slack_alert(err_msg)
                    return
                try:
                    # Retrieve a task to run
                    task_to_run = retrieve_cold_task()
                    if task_to_run == -1:
                        logging.warning(f"{log_prefix}: Stop Time breached. Stopping BI AutoScheduler Vena Cava.")
                        shutdown_event.set()
                        break
                except RuntimeError as e:
                    logging.error(f"{log_prefix}: RuntimeError encourented in retrieve_cold_task(): {str(e)}")
                    continue
                task_pickup_timestamp = datetime.utcnow()
                task_luid = task_to_run[0]['TASK_LUID']
                task_name = task_to_run[0]['TASK_TARGET_NAME']
                queue_position = task_to_run[0]['QUEUE_POSITION']
                hot_task = f"Task - [{task_name} ({task_luid})]"

                if task_luid in executed_jobs:
                    logging.warning(
                        f"{log_prefix}: Job with TASK_LUID {task_luid} was already executed. Marking as hot and querying for a new task."
                    )
                    hot_task_update(task_luid)
                    continue

                logging.info(f"{log_prefix}: {hot_task} has been retrieved from the queue. Queue Position: {queue_position}")
                # Submit the task to the executor
                try:
                    future = executor.submit(
                        tableau_futures_orchestrator, task_name, task_luid, task_pickup_timestamp, gate_elapsed_time_in_seconds,
                        gate_elapsed_time_pretty
                    )
                    active_futures.append(future)
                    logging.info(f"{log_prefix}: {hot_task} sent to the orchestrator!")
                    # Mark the task as hot (HOT = sent to Tableau Server for execution)
                    hot_task_update(task_pickup_timestamp, gate_elapsed_time_in_seconds, task_luid)
                    logging.info(f"{log_prefix}: {hot_task} marked as hot in Snowflake.")
                    # Add it to list of executed jobs
                    executed_jobs.add(task_luid)
                    logging.info(f"{log_prefix}: {hot_task} added to the list of executed jobs.")
                    time_module.sleep(30)  # 30 sec delay before submitting the next task

                except Exception as e:
                    logging.error(f"{log_prefix}: Error occurred in submitting {hot_task} to orchestrator: {str(e)}")
                    continue
            else:
                logging.warning(f"{log_prefix}: Reached maximum number of active futures. Waiting 2 minutes before checking again.")
                time_module.sleep(120)  # 2 min delay before checking the number of active futures again
        if shutdown_event.is_set():
            logging.info(f"{log_prefix}: BI AutoScheduler Vena Cava stopping due to stop time.")
            executor.shutdown(wait=False, cancel_futures=True)
            return

    logging.info(f"{log_prefix}: Signing off for the day. Goodbye!")


#################
# DAG Task Flow #
#################

# slack comms
send_init_slack = PythonOperator(
    task_id='init_slack',
    python_callable=init_slack,
    provide_context=True,
    retries=2,
    retry_delay=timedelta(minutes=5),
    dag=dag.airflow_dag,
)
send_term_slack = PythonOperator(
    task_id='term_slack',
    python_callable=term_slack,
    provide_context=True,
    retries=2,
    retry_delay=timedelta(minutes=5),
    dag=dag.airflow_dag,
)
# Load and prepare all Snowflake data architecture for the day's run
tableau_job_flow_load = PythonOperator(
    task_id='tableau_job_flow_load',
    python_callable=tableau_job_flow,
    provide_context=True,
    retries=5,
    retry_delay=timedelta(minutes=10),
    on_retry_callback=notify_on_retry,
    dag=dag.airflow_dag,
)
# check if databases are ready
is_vertica_east_ready = PythonOperator(
    task_id='is_vertica_east_ready',
    python_callable=is_vertica_ready,
    op_kwargs={'db': 'vertica_us_east'},
    provide_context=True,
    execution_timeout=timedelta(hours=12),
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_retry_callback=notify_on_retry,
    dag=dag.airflow_dag,
)
is_vertica_west_ready = PythonOperator(
    task_id='is_vertica_west_ready',
    python_callable=is_vertica_ready,
    op_kwargs={'db': 'vertica_us_west'},
    provide_context=True,
    execution_timeout=timedelta(hours=12),
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_retry_callback=notify_on_retry,
    dag=dag.airflow_dag,
    priority_weight=5
)
is_edw_snowflake_ready = PythonOperator(
    task_id='is_edw_snowflake_ready',
    python_callable=is_edw_ready,
    op_kwargs={'db': 'ttd_tableau_snowflake_prod'},
    provide_context=True,
    execution_timeout=timedelta(hours=12),
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_retry_callback=notify_on_retry,
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag.airflow_dag,
)
# Mark database as ready in AUTOSCHEDULER.DATABASE_READINESS_CHECK
db_ready_vertica_east = PythonOperator(
    task_id='db_ready_vertica_east',
    python_callable=db_ready,
    op_kwargs={'db': 'vertica_us_east'},
    provide_context=True,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
db_ready_vertica_west = PythonOperator(
    task_id='db_ready_vertica_west',
    python_callable=db_ready,
    op_kwargs={'db': 'vertica_us_west'},
    provide_context=True,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
    priority_weight=5
)
db_ready_snowflake_edw = PythonOperator(
    task_id='db_ready_snowflake_edw',
    python_callable=db_ready,
    op_kwargs={'db': 'ttd_tableau_snowflake_prod'},
    provide_context=True,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
# Update AUTOSCHEDULER.TABLEAU_JOB_HISTORY with what tasks are ready to go
job_is_ready_no_db = PythonOperator(
    task_id='job_is_ready_no_db',
    python_callable=tableau_job_is_ready,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
job_is_ready_vertica_east = PythonOperator(
    task_id='job_is_ready_vertica_east',
    python_callable=tableau_job_is_ready,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
job_is_ready_vertica_west = PythonOperator(
    task_id='job_is_ready_vertica_west',
    python_callable=tableau_job_is_ready,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
    priority_weight=5
)
job_is_ready_snowflake_edw = PythonOperator(
    task_id='job_is_ready_snowflake_edw',
    python_callable=tableau_job_is_ready,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
# Load ready-to-go tasks into AUTOSCHEDULER.TABLEAU_ACTIVE_QUEUE
job_active_queue_no_db = PythonOperator(
    task_id='job_active_queue_no_db',
    python_callable=tableau_job_active_queue,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
job_active_queue_vertica_east = PythonOperator(
    task_id='job_active_queue_vertica_east',
    python_callable=tableau_job_active_queue,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag
)
job_active_queue_vertica_west = PythonOperator(
    task_id='job_active_queue_vertica_west',
    python_callable=tableau_job_active_queue,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
job_active_queue_snowflake_edw = PythonOperator(
    task_id='job_active_queue_snowflake_edw',
    python_callable=tableau_job_active_queue,
    retries=2,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
# BI AutoScheduler
autoscheduler = PythonOperator(
    task_id='bi_autoscheduler',
    python_callable=bi_autoscheduler,
    provide_context=True,
    execution_timeout=timedelta(hours=23, minutes=45),
    retries=5,
    retry_delay=timedelta(minutes=5),
    on_retry_callback=notify_on_retry,
    dag=dag.airflow_dag
)
daily_wrap_up = PythonOperator(
    task_id='daily_wrap_up',
    python_callable=snowflake_daily_wrap_up,
    retries=2,
    provide_context=True,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
    trigger_rule='all_done'
)

warp_fast_pass_lane = PythonOperator(
    task_id='warp_fast_pass_lane',
    python_callable=warp_fast_pass,
    retries=5,
    provide_context=True,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
    priority_weight=10
)

# DAG Task Flow
send_init_slack
tableau_job_flow_load >> job_is_ready_no_db >> job_active_queue_no_db >> autoscheduler >> daily_wrap_up >> send_term_slack
tableau_job_flow_load >> [is_vertica_east_ready, is_vertica_west_ready, warp_fast_pass_lane]
is_vertica_east_ready >> db_ready_vertica_east >> job_is_ready_vertica_east >> job_active_queue_vertica_east >> send_term_slack
is_vertica_west_ready >> db_ready_vertica_west >> job_is_ready_vertica_west >> job_active_queue_vertica_west >> send_term_slack
is_vertica_east_ready >> is_edw_snowflake_ready >> db_ready_snowflake_edw >> job_is_ready_snowflake_edw >> job_active_queue_snowflake_edw >> send_term_slack
