from datetime import date, timedelta, datetime, time, timezone
import logging
import random
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
from dags.fineng.edw.autoscheduler.Cloud import autoscheduler_queries
from dags.fineng.edw.autoscheduler.Cloud.server_manager import ServerManager
from ttd.ttdslack import dag_post_to_slack_callback, get_slack_client
from ttd.el_dorado.v2.base import TtdDag
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from dags.fineng.edw.utils.helper import write_to_execution_logs
import queue

autoscheduler_version = '*EDW-BI Cloud AutoScheduler v1.0.2*'

notification_slack_channel = '#bi-autoscheduler-notifications'  # change to #bi-autoscheduler-notifications
alarm_slack_channel = '#bi-autoscheduler-notifications'

default_args = {
    'owner': 'FINENG',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 5),
    'catchup': False,
}

dag = TtdDag(
    dag_id='tableau-cloud-autoscheduler',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=23, minutes=55),
    on_failure_callback=
    dag_post_to_slack_callback(dag_name='tableau-autoscheduler', step_name='parent dagrun', slack_channel=alarm_slack_channel),
    schedule_interval='0 1 * * *',  # Start at 01:00 UTC every day, in line with #bi-data-refreshes
    max_active_runs=1,
    run_only_latest=True,
)
adag = dag.airflow_dag

process_name = 'tableau-cloud-autoscheduler'


def send_slack_message(slack_message, thread_ts=None, **kwargs):
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
    slack_message = f"BI AutoScheduler is starting the day's run at {current_timestamp} UTC"
    send_slack_message(slack_message)


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

    send_slack_message(tasks_completed_slack_message)
    """
    # how to send a message to a thread
    thread_message = f"This is a test to see if I sent a message to the correct thread."
    
    init_response = send_slack_message(notification_slack_channel, tasks_completed_slack_message)
    thread_id = init_response['message']['ts']
    
    send_slack_message(notification_slack_channel, thread_message, thread_ts=thread_id)
    """


def delayer(delay_minutes=None, target_time=None, **kwargs):
    logging.info(f"kwargs: {kwargs}")
    logging.info(
        "This task will wait until the specified time or duration has passed. The purpose of this is to delay the execution of the next task."
    )

    starting_time = datetime.utcnow()

    if target_time:
        # If target_time is provided, wait until that time
        final_target_time = datetime.combine(starting_time.date(), target_time)
    elif delay_minutes:
        # If delay_minutes is provided, wait for that duration
        final_target_time = starting_time + timedelta(minutes=delay_minutes)
    logging.info(f"Waiting until: {final_target_time}")

    max_sleep_time = 30  # 30 minutes
    current_time = datetime.utcnow()

    while current_time < final_target_time:
        remaining_time = (final_target_time - current_time).total_seconds() / 60  # Calculate remaining time in minutes
        sleep_time = min(remaining_time, max_sleep_time)  # Sleep for the remaining time or up to 30 minutes
        logging.info(f"\nCurrent Time: {current_time}\nTarget TIme: {final_target_time}\nSleeping for {sleep_time} minutes")
        time_module.sleep(sleep_time * 60)  # Sleep for sleep_time minutes
        current_time = datetime.utcnow()

    final_wait_time = (final_target_time - starting_time).total_seconds() / 60

    logging.info(
        f"\nDELAY COMPLETE!!\nCurrent Time: {current_time}\nTarget TIme: {final_target_time}\nTotal Time Delayed {final_wait_time} minutes"
    )


# Send a slack message to warn the team of certain tasks retrying
def notify_on_retry(context, **kwargs):
    task_instance = context['ti']
    attempt = task_instance.try_number

    if {context['task_instance'].task_id} == 'is_vertica_east_ready' or {context['task_instance'].task_id} == 'is_vertica_west_ready' or {
            context['task_instance'].task_id
    } == 'is_edw_snowflake_ready':
        if attempt == 2:
            retry_warning_message = f":warning: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` is running attempt {attempt} of 2. There are no retries left after this attempt. Please visit the Airflow UI to investigate."
            send_slack_message(retry_warning_message)
            logging.warning(retry_warning_message)
    elif {context['task_instance'].task_id} == 'bi_autoscheduler':
        if attempt == 2:
            autsch_retry_message = f":warning: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` triggered a retry! This is not expected behavior. Please visit the UI to investigate."
            send_slack_message(autsch_retry_message)
            logging.warning(autsch_retry_message)
        elif attempt == 3:
            autsch_final_retry_message = f":alert: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` is running it's final retry. Something must be wrong. Please visit the UI to investigate."
            send_slack_message(autsch_final_retry_message)
            logging.warning(autsch_final_retry_message)
    elif {context['task_instance'].task_id} == 'tableau_job_flow_load':
        if attempt == 4:
            job_flow_retry_message = f":alert: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` has failed 3 times and is on _Attempt 4_. This is not expected behavior. There is likely an issue with the Tableau Postgres Database. There is 1 retry left. Please visit the UI to investigate."
            send_slack_message(job_flow_retry_message)
            logging.warning(job_flow_retry_message)
        elif attempt == 5:
            autsch_final_retry_message = f":alert: TASK RETRY ALERT - Task `{context['task_instance'].task_id}` is running it's final retry. Please visit the UI to investigate and run manually if necessary."
            send_slack_message(autsch_final_retry_message)
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
    elif conn_name == 'cloud_autoscheduler_edw_snowflake_prod':
        conn = BaseHook.get_connection('cloud_autoscheduler_edw_snowflake_prod')
        return {
            'account': conn.extra_dejson['account'],
            'user': conn.login,
            'password': conn.password,
            'warehouse': conn.extra_dejson['warehouse'],
            'role': conn.extra_dejson['role'],
            'database': conn.extra_dejson['database'],
            'schema': conn.schema
        }
    elif conn_name == 'cloud_autoscheduler_edw_snowflake_dev':
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

    # prod: tableau-postgres.thetradedesk.com
    # test: tableau-test-postgres.thetradedesk.com
    elif conn_name == 'tableau_postgres':
        conn = BaseHook.get_connection('tableau_postgres')
        return {
            'host': 'tableau-postgres.thetradedesk.com',
            'port': conn.port,
            'database': conn.schema,
            'user': conn.login,
            'password': conn.password
        }

    # prod: https://tableau.thetradedesk.com
    # test: https://test-tableau.thetradedesk.com
    elif conn_name == 'tableau_server':
        conn = BaseHook.get_connection('tableau_server')
        return {
            'server': 'https://tableau.thetradedesk.com',
            'site': conn.extra_dejson['site_id'],
            'username': conn.login,
            'password': conn.password
        }

    elif conn_name == 'tableau_cloud':
        token_name = Variable.get('pat_token_name')
        token_secret = Variable.get('pat_token_secret')
        tableau_cloud_server = Variable.get('tableau_cloud_server')
        site_id = Variable.get('tableau_site_id')
        return {'server': tableau_cloud_server, 'site': site_id, 'token_name': token_name, 'token_secret': token_secret}

    else:
        # critical error -> logging to process execution logs
        process_id = 'get_db_connection'
        process_status = 'ERROR'
        process_start_time = datetime.utcnow()
        process_end_time = datetime.utcnow()
        message = f"Error occurred in getting the connection info for {conn_name}."
        write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
        raise ValueError(f"Unknown connection name: {conn_name}")


def check_load_anomaly(**kwargs):
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        sf_cur = sf_conn.cursor()
        job_history_row_count = sf_cur.execute(autoscheduler_queries.job_history_idempotency_check).fetchone()[1]

        #check 1: check if the number of rows in TABLEAU_JOB_HISTORY is less than 400
        # if job_history_row_count < 400:
        #     logging.warning(
        #         "The number of rows inserted into TABLEAU_JOB_HISTORY was less than 400. This is likely an error, please investigate."
        #     )
        #     msg = f":yellow_alert: The number of rows inserted into TABLEAU_JOB_HISTORY was less than 400. Please investigate."
        #     return True,msg
        #Check 2: Check if load is within 10% of previous day's load
        previous_day_count = sf_conn.cursor().execute(autoscheduler_queries.check_previous_day_job_history).fetchone()[0]
        if job_history_row_count < 0.9 * previous_day_count or job_history_row_count > 1.1 * previous_day_count:
            logging.warning(
                f"The number of rows inserted into TABLEAU_JOB_HISTORY was not within 10% of the last run's load of {previous_day_count} Dashboards. Please investigate."
            )
            msg = f"*The number of rows inserted into TABLEAU_JOB_HISTORY was not within 10% of the previous day's load of {previous_day_count} Dashboards*"
            return True, job_history_row_count, msg
        return False, job_history_row_count, None


# Construct the full data pipeline used by BI AutoScheduler
def tableau_job_flow(**kwargs):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    logging.info(f"edw_snowflake_prod_conn_info: {cloud_autoscheduler_edw_snowflake_prod_conn_info}")
    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            # print("This is a test print statement")
            # raise Exception("This is a test exception")
            sf_cur = sf_conn.cursor()
            # Idempotency check for TABLEAU_JOB_HISTORY
            logging.info("TABLEAU_JOB_HISTORY idempotency check.")
            logging.info("Checking to see if the Tableau Job data I'm trying to load into TABLEAU_JOB_HISTORY already exists.")
            log_entry_date_utc, job_history_row_count = sf_cur.execute(autoscheduler_queries.job_history_idempotency_check).fetchone()
            logging.info(f"Log Entry Date UTC: {log_entry_date_utc}")
            logging.info(f"Job History Row Count: {job_history_row_count}")
            kwargs['ti'].xcom_push(key='tableau_job_flow_log_entry_date_utc', value=log_entry_date_utc)

            if job_history_row_count > 0:
                logging.warning(f"LOG_ENTRY_DATE_UTC = {log_entry_date_utc} already exists in TABLEAU_JOB_HISTORY. Skipping data load.")
                # Pushing the date we're inserting data for to xcoms to use downstream

            else:
                logging.info("Tableau Job queue for today does not exist. Going to attempt to load it now!")
                logging.info('This is 3 Step Process. If a step does not log a "Success!" then please investigate that step. Wish me luck!')
                logging.info(
                    "Step 1: Marking all rows in TABLEAU_JOB_HISTORY to IS_ACTIVE_QUEUE = FALSE and marking the previous run as IS_PREVIOUS_QUEUE"
                )
                # update the table to mark the previous queue as IS_PREVIOUS_QUEUE and set the whole table to IS_ACTIVE_QUEUE = FALSE
                sf_cur.execute(autoscheduler_queries.update_is_active_queue_to_false)
                sf_conn.commit()
                logging.info("Step 1: Success!")
                # Insert the queue into production table
                logging.info("Step 2: Inserting all of today's jobs from Snowflake.")
                sf_cur.execute(autoscheduler_queries.tableau_job_load_to_history)
                sf_conn.commit()
                logging.info("Step 2: Success!")

                # fetch rows inserted
                logging.info("Step 3: Checking to see if the data was inserted successfully.")
                # job_history_row_count = sf_cur.execute(autoscheduler_queries.job_history_idempotency_check).fetchone()[1]
                # if job_history_row_count < 400:
                #     logging.warning(
                #         "The number of rows inserted into TABLEAU_JOB_HISTORY was less than 400. This is likely an error, attempting to retry the load."
                #     )
                #     # delete entry for the day
                #     sf_cur.execute(autoscheduler_queries.delete_tableau_job_history)
                #     sf_conn.commit()
                #     logging.info("Deleted the entry for the day in TABLEAU_JOB_HISTORY")
                #     # retry the load
                #     time_module.sleep(300)
                #     logging.info("Retrying the load into TABLEAU_JOB_HISTORY")
                #     sf_cur.execute(autoscheduler_queries.tableau_job_load_to_history)
                #     sf_conn.commit()

                #     job_history_row_count = sf_cur.execute(autoscheduler_queries.job_history_idempotency_check).fetchone()[1]
                #     if job_history_row_count < 400:
                #         logging.warning(
                #             "The number of rows inserted into TABLEAU_JOB_HISTORY was still less than 150. This is likely an error, please investigate."
                #         )
                #         msg = f":yellow_alert: The number of rows inserted into TABLEAU_JOB_HISTORY was less than 150.Please investigate."
                #         send_slack_message(msg)
                #     else:
                #         logging.info("Step 3: Success!")
                #         print(
                #             "Successfully loaded the Tableau Refresh Queue to Snowflake [EDW.AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY] at " +
                #             datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                #         )
                is_anomaly, inserted_rows, msg = check_load_anomaly()
                info_msg = f":info: We have *{str(inserted_rows)}* Dashboards for refresh today"
                send_slack_message(info_msg)
                if is_anomaly:
                    send_slack_message(f":yellow_circle: {msg} - *Attempting to retry the load.*")
                    # re-attempt the load
                    logging.warning(f"{msg} Retrying the load into TABLEAU_JOB_HISTORY")
                    # delete entry for the day
                    send_slack_message(f":loading: *Retrying the load into TABLEAU_JOB_HISTORY*")
                    sf_cur.execute(autoscheduler_queries.delete_tableau_job_history)
                    sf_conn.commit()
                    logging.info("Deleted the entry for the day in TABLEAU_JOB_HISTORY")
                    # retry the load
                    logging.info("Sleeping for 5 minutes before retrying the load.")
                    time_module.sleep(300)
                    logging.info("Retrying the load into TABLEAU_JOB_HISTORY")
                    sf_cur.execute(autoscheduler_queries.tableau_job_load_to_history)
                    sf_conn.commit()
                    is_anomaly, inserted_rows, msg = check_load_anomaly()
                    if is_anomaly:
                        warning_msg = f":warning: {msg} AUTO_SCHEDULER is proceeding with the next steps."
                        send_slack_message(warning_msg)
                    else:
                        msg = f":white_check_mark:*Successfully loaded into TABLEAU_JOB_HISTORY, without anomalies.*"
                        logging.info("Step 3: Success!")
                        print(
                            "Successfully loaded the Tableau Refresh Queue to Snowflake [EDW.AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY] at " +
                            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                        )
                else:
                    logging.info("Step 3: Success!")
                    print(
                        "Successfully loaded the Tableau Refresh Queue to Snowflake [EDW.AUTOSCHEDULER_CLOUD.TABLEAU_JOB_HISTORY] at " +
                        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                    )

        except Exception as e:
            process_id = 'tableau_job_flow'
            process_status = 'ERROR'
            process_start_time = datetime.utcnow()
            process_end_time = datetime.utcnow()
            message = f"Error occurred in loading the Tableau Job Flow load data to Snowflake: {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("Error occurred in loading the Tableau Postgres data to Snowflake: {}".format(str(e)))

        try:
            # prepare DATABASE_READINESS_CHECK for the day's run
            logging.info("DATABASE_READINESS_CHECK update")
            logging.info(
                "Now that the Tableau job queue has been loaded, I'm going to update the DATABASE_READINESS_CHECK table for today's date as well"
            )
            logging.info("Running idempotency check for DATABASE_READINESS_CHECK")
            db_ready_count = sf_cur.execute(autoscheduler_queries.db_ready_date_check, {
                'log_entry_date_utc': log_entry_date_utc
            }).fetchone()[0]
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
                sf_cur.execute(autoscheduler_queries.db_ready_insert_new_date, {'log_entry_date_utc': log_entry_date_utc})
                logging.info("DATABASE_READINESS_CHECK table updated successfully!")
                print(
                    "Successfully prepped the DB check table [EDWANALYTICS.AUTOSCHEDULER.DATABASE_READINESS_CHECK] for the day at " +
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                )
        except Exception as e:
            process_id = 'tableau_job_flow'
            process_status = 'ERROR'
            process_start_time = datetime.utcnow()
            process_end_time = datetime.utcnow()
            message = f"Error occurred in updating DATABASE_READINESS_CHECK: {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("Error occurred in updating DATABASE_READINESS_CHECK: {}".format(str(e)))

        try:
            # prepare TABLEAU_JOB_ACTIVE_QUEUE for the day's run
            logging.info("TABLEAU_JOB_ACTIVE_QUEUE update")
            logging.info(
                "Now that TABLEAU_JOB_HISTORY and DATABASE_READINESS_CHECK are done, I'm going to see if I need to clear the TABLEAU_JOB_ACTIVE_QUEUE table for today as well"
            )
            active_q_count = sf_cur.execute(autoscheduler_queries.active_q_idempotency_check, {
                'log_entry_date_utc': log_entry_date_utc
            }).fetchone()[0]
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
            process_id = 'tableau_job_flow'
            process_status = 'ERROR'
            process_start_time = datetime.utcnow()
            process_end_time = datetime.utcnow()
            message = f"Error occurred in truncating TABLEAU_JOB_ACTIVE_QUEUE: {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("Error occurred in truncating TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


# Check if Vertica databases data are ready
def is_vertica_ready(db, **kwargs):
    # random sleep between 0 and 300 seconds to avoid all tasks running at the same time
    time_module.sleep(300)
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
                send_slack_message(db_delayed_message)
                has_alerted_provdb_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_provdb_2hr_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's been 2 hours since the previous alert and the data for {cluster_name} is still not fully loaded according to _ProvDb_. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_provdb_2hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_provdb_4hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the data for {cluster_name} is still not fully loaded according to _ProvDb_. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_provdb_4hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_provdb_6hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the data for {cluster_name} is still not fully loaded according to _ProvDb_. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_provdb_6hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_provdb_8hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the data for {cluster_name} is still not fully loaded according to _ProvDb_. Please check #dev-vertica. *This is the final alert I will send about the {cluster_name} ProvDB check* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
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
                send_slack_message(db_delayed_message)
                has_alerted_vertica_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow(
            ) < (db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_vertica_2hr_delay and not has_alerted_provdb_2hr_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's 2 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables in {cluster_name} are still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_vertica_2hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow(
            ) < (db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_vertica_4hr_delay and not has_alerted_provdb_4hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables in {cluster_name} are still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_vertica_4hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow(
            ) < (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_vertica_6hr_delay and not has_alerted_provdb_6hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables in {cluster_name} are still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_vertica_6hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_vertica_8hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the BI Daily Agg tables for {cluster_name} are still not ready. Please check #dev-vertica. *This is the final alert I will send about BI Daily Agg Tables in {cluster_name}* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
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
                send_slack_message(db_delayed_message)
                has_alerted_attribution_split_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_attribution_split_2hr_delay:
                db_delayed_message = f":warning: *{cluster_name} DELAY ALERT* - It's 2 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution  data for {cluster_name} is still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_attribution_split_2hr_delay = True
                sleep_time = 600
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_attribution_split_4hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_attribution_split_4hr_delay = True
                sleep_time = 600
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_attribution_split_6hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is still not ready. Please check #dev-vertica. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_attribution_split_6hr_delay = True
                sleep_time = 600
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_attribution_split_8hr_delay:
                db_delayed_message = f":alert: *{cluster_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and the Attribution data for {cluster_name} is still not ready. Please check #dev-vertica. *This is the final alert I will send about the {cluster_name} Attribution Split data* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
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
        send_slack_message(db_success_message)

        # xcom to push truth checks in subsequent tasks
        max_date_plus_one = max_date + timedelta(days=1)
        kwargs['ti'].xcom_push(key='db_max_date', value=max_date_plus_one)
        logging.info(f"xcom_push [db_max_date]: {max_date_plus_one}")
        kwargs['ti'].xcom_push(key='database_ready_time', value=database_ready_time)
        logging.info(f"xcom_push [database_ready_time]: {database_ready_time}")


# Check if EDW databases are ready
def is_edw_ready(db, **kwargs):
    #time_module.sleep(60)
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
                send_slack_message(db_delayed_message)
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                has_alerted_edw_delay = True
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=2)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=4)) and not has_alerted_edw_2hr_delay:
                db_delayed_message = f":warning: *{db_name} DELAY ALERT* - It's 2 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_edw_2hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=4)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=6)) and not has_alerted_edw_4hr_delay:
                db_delayed_message = f":alert: *{db_name} DELAY ALERT* - We are now 4 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. Please ask in #techops-edw. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_edw_4hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=6)) and datetime.utcnow() < (
                    db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_edw_6hr_delay:
                db_delayed_message = f":alert: *{db_name} DELAY ALERT* - We are now 6 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. Please ask in #techops-edw. I will continue checking for now."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
                has_alerted_edw_6hr_delay = True
                logging.info(f"Sleeping {round(sleep_time/60)} minutes before checking again.")
                sleep_time = 600
                time_module.sleep(sleep_time)
            elif datetime.utcnow() >= (db_delayed_tolerance_time + timedelta(hours=8)) and not has_alerted_edw_8hr_delay:
                db_delayed_message = f":alert: *{db_name} DELAY ALERT* - We are now 8 hours past the delay tolerance time of {db_delayed_tolerance_time.strftime('%H:%M UTC')} and {db_name} data is still not ready. Please ask in #techops-edw. *This is the final alert I will send about {db_name}* but I will continue checking. Please visit the Airflow UI to stop this task if we don't expect the data to be ready anytime soon."
                logging.warning(db_delayed_message)
                send_slack_message(db_delayed_message)
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
        send_slack_message(db_success_message)

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
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
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
            process_id = 'db_ready'
            process_status = 'ERROR'
            process_start_time = datetime.utcnow()
            process_end_time = datetime.utcnow()
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, str(e))
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
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
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


# Flag Tableau tasks that are ready to run on Tableau Cloud in TABLEAU_JOB_HISTORY
def tableau_job_is_ready(**kwargs):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_cur = sf_conn.cursor()
            logging.info("Updating TABLEAU_JOB_HISTORY with whatever can be marked IS_READY_TO_GO")
            sf_cur.execute(autoscheduler_queries.tableau_job_is_ready_update)
            sf_conn.commit()
            logging.info("TABLEAU_JOB_HISTORY has been updated with IS_READY_TO_GO flags.")
        except Exception as e:
            process_id = 'tableau_job_is_ready'
            process_status = 'ERROR'
            process_start_time = datetime.utcnow()
            process_end_time = datetime.utcnow()
            message = f"Error occurred while updating TABLEAU_JOB_HISTORY: {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("Error occurred while updating TABLEAU_JOB_HISTORY: {}".format(str(e)))


# Function to load tasks into TABLEAU_JOB_ACTIVE_QUEUE so BI AutoScheduler can pick them up
def tableau_job_active_queue(**kwargs):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    # Sleep for a random interval between 1 and 300 seconds to deter possible race conditions created by multiple tasks running at the same time
    sleep_interval = random.randint(1, 10)
    logging.info(f"Sleeping for {sleep_interval} seconds before executing the query.")
    time_module.sleep(sleep_interval)

    # Get the current timestamp with seconds and fractional seconds
    current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    logging.info(f"log_entry_time_utc: {current_timestamp}")
    query_param_log_entry_time_utc = {'log_entry_time_utc': current_timestamp}

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_cur = sf_conn.cursor()
            logging.info("Loading the TABLEAU_JOB_ACTIVE_QUEUE with jobs that can be queued for refresh.")
            sf_cur.execute(autoscheduler_queries.tableau_job_active_queue_insert, query_param_log_entry_time_utc)
            rows_inserted = sf_cur.rowcount
            sf_conn.commit()
            logging.info(f"Inserted {rows_inserted} rows into TABLEAU_JOB_ACTIVE_QUEUE.")
        except Exception as e:
            logging.exception(str(e))
            raise Exception("Error occurred while updating TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))
        try:
            logging.info("Auditing all rows that were just inserted into TABLEAU_JOB_ACTIVE_QUEUE")
            audit_result = sf_cur.execute(autoscheduler_queries.tableau_job_active_queue_audit_insert,
                                          query_param_log_entry_time_utc).fetchall()
            if not audit_result:
                logging.warning("No rows were inserted into TABLEAU_JOB_ACTIVE_QUEUE. This is unexpected, please investigate.")
            duplicates_found = False
            for row in audit_result:
                if row[7]:  # Check if AUDIT_INSERT is True
                    logging.info(f"Inserted row: {row}")
                if row[8] > 1:  # Check if RECORD_COUNT is greater than 1
                    duplicates_found = True
            if duplicates_found:
                logging.warning(
                    "Duplicates found in TABLEAU_JOB_ACTIVE_QUEUE!!! This was likely caused by race condition. Initiating cleanup."
                )
                for row in audit_result:
                    if row[8] > 1:
                        logging.info(f"Deleting duplicate row: {row}")
                logging.info("Running deletion...")
                sf_cur.execute(autoscheduler_queries.tableau_job_delete_duplicates)
                deleted_rows = sf_cur.rowcount
                sf_conn.commit()
                logging.info(f"Success! Deleted {deleted_rows} duplicate rows from TABLEAU_JOB_ACTIVE_QUEUE.")
        except Exception as e:
            process_id = 'tableau_job_active_queue'
            process_status = 'ERROR'
            process_start_time = datetime.utcnow()
            process_end_time = datetime.utcnow()
            message = f"Error occurred while auditing TABLEAU_JOB_ACTIVE_QUEUE: {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("Error occurred while auditing TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


#########################
# Warp Fast Pass Lane   #
#########################


def get_fast_passed_dashboards():
    try:
        process_start_time = datetime.utcnow()
        log_prefix = '[warp_fast_pass lane - stats_collector]'
        #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection("autoscheduler_edw_snowflake_dev")
        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection("cloud_autoscheduler_edw_snowflake_prod")
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            cursor = sf_conn.cursor()
            fast_passed_dash = cursor.execute(autoscheduler_queries.warp_dash_fast_passed).fetchone()[0]
            logging.info(f'{log_prefix} Number of dashboards that have been fast-passed: {fast_passed_dash}')
        return fast_passed_dash
    except Exception as e:
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_id = 'get_fast_passed_dashboards'
            process_status = 'ERROR'
            process_end_time = datetime.utcnow()
            message = f"Error occurred in get_fast_passed_dashboards(): {str(e)}"
            write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
        logging.exception(f"Error occurred in get_fast_passed_dashboards(): {str(e)}")
        return 0


def warp_fast_pass(**kwargs):
    # check if tableau job history operator is complete
    try:
        process_start_time = datetime.utcnow()
        log_prefix = '[warp_fast_pass lane - poller]'
        slack_msg = ':checkered_flag: *Warp Fast Pass Lane* :checkered_flag:  - Starting the warp fast pass lane'
        send_slack_message(slack_msg)
        luid_name_mapper = {}

        #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection("autoscheduler_edw_snowflake_dev")

        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection("cloud_autoscheduler_edw_snowflake_prod")
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
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
            logging.info(f"Number of dashboards to fast-pass: {dash_count}")
            #dash_count = 14
        fast_passed = get_fast_passed_dashboards()

        while (fast_passed < dash_count) and (datetime.utcnow().time() < time(23, 30)):
            try:
                logging.info("Checking warp dash ready for refesh....")
                with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
                    cursor = sf_conn.cursor()
                    res = cursor.execute(autoscheduler_queries.warp_mark_ready).fetchall()
                    if len(res) > 0:
                        logging.info(f"{log_prefix} - Marked {len(res)} warp dashboards as ready for refresh.")
                        # include a race car at the end of the message
                        msg = f":checkered_flag: *Warp Fast Pass Lane* :checkered_flag: - Marked these warp dashboards as ready for refresh "
                        send_slack_message(msg)
                        for row in res:
                            msg = f"Dashboard {luid_name_mapper[row[0]]} :racing_car: \n "
                            send_slack_message(msg)
                # call function to pass to active queue
                tableau_job_active_queue()
                time_module.sleep(15)
                fast_passed = get_fast_passed_dashboards()

            except Exception as e:
                logging.exception(f"Error occurred in warp_fast_pass() loop: {str(e)}")
                continue
        logging.info(f"{log_prefix} Warp Fast Pass Lane complete")
        end_msg = ":checkered_flag: *Warp Fast Pass Lane* :checkered_flag: - Warp Fast Pass Lane complete"
        send_slack_message(end_msg)
    except Exception as e:
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_end_time = datetime.utcnow()
            process_id = 'warp_fast_pass'
            process_status = 'ERROR'
            message = f"Error occurred in warp_fast_pass(): {str(e)}"
            write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
        logging.exception(f"Error occurred in warp_fast_pass(): {str(e)}")


##########################
# BI AutoScheduler Stack #
##########################


##########################
# BI AutoScheduler Stack #
##########################
def autsch_art():
    art = r"""

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
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            process_start_time = datetime.utcnow()
            logging.debug("Querying for the result of all the jobs ran on Tableau Server from AUTOSCHEDULER.TABLEAU_JOB_HISTORY")
            cursor = sf_conn.cursor()
            cursor.execute(autoscheduler_queries.autoscheduler_result)
            autoscheduler_result = cursor.fetchone()

            if autoscheduler_result is None:
                raise Exception("No results found for autoscheduler_result query")

            # Extract values from the result tuple
            log_entry_date_utc, total_job_count, successful_job_count, failed_job_count, cancelled_job_count, skipped_job_count = autoscheduler_result

            result_summary = {
                "log_entry_date_utc": log_entry_date_utc,
                "total_job_count": total_job_count,
                "successful_job_count": successful_job_count,
                "failed_job_count": failed_job_count,
                "cancelled_job_count": cancelled_job_count,
                "skipped_job_count": skipped_job_count
            }

        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'autoscheduler_result_fetch'
            process_status = 'ERROR'
            message = f"Error occurred in autoscheduler_result_fetch(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("autoscheduler_result_fetch(): Error occurred: {}".format(str(e)))

    return result_summary


# Retrieve how many jobs have been executed today to feed the while loop in the case of task retries
def executed_jobs_today():
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            process_start_time = datetime.utcnow()
            # Retrieving all jobs that have been executed today
            logging.debug("Querying for all jobs that have been executed today from AUTOSCHEDULER.TABLEAU_JOB_HISTORY")
            result = sf_conn.cursor().execute(autoscheduler_queries.executed_jobs_today).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'executed_jobs_today'
            process_status = 'ERROR'
            message = f"Error occurred in executed_jobs_today(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception(
                "executed_jobs_today(): Error occurred in retrieving the total job count from TABLEAU_JOB_HISTORY in Snowflake: {}"
                .format(str(e))
            )


def retrieve_in_progress_jobs(object_luid=None):
    log_prefix = "[JOB RETRIEVER]"

    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    process_start_time = datetime.utcnow()
    if object_luid:
        # currently, nothing is using this function with an object_luid, so this is just a placeholder for now
        query_params = {'object_luid': object_luid}
        in_progress_jobs_query = autoscheduler_queries.single_in_progress_job
        logging.info(f"{log_prefix}: Retrieving JOB_ID from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake for OBJECT_LUID = {object_luid}")
    else:
        in_progress_jobs_query = autoscheduler_queries.all_in_progress_jobs
        logging.info(f"{log_prefix}: Retrieving in-progress jobs from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake.")

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            retrieve_jobs = sf_conn.cursor().execute(in_progress_jobs_query)
            in_progress_jobs = []
            for row in retrieve_jobs:
                jobs_dict = {'LOG_ENTRY_DATE_UTC': row[0], 'OBJECT_LUID': row[1], 'OBJECT_NAME': row[2], 'JOB_ID': row[3]}
                in_progress_jobs.append(jobs_dict)
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'retrieve_in_progress_jobs'
            process_status = 'ERROR'
            message = f"Error occurred in retrieve_in_progress_jobs(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.error(str(e))
            raise Exception(f"{log_prefix}: Error occurred in retrieving jobs from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake: {str(e)}")

    return in_progress_jobs


# Retrieve a task from TABLEAU_JOB_ACTIVE_QUEUE for BI AutoScheduler to run
def job_fetcher():
    global stop_time
    log_prefix = '[JOB FETCHER]'

    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        process_start_time = datetime.utcnow()
        while True:
            try:
                current_time = datetime.utcnow().time()
                if current_time >= stop_time:
                    logging.warning(f"{log_prefix}: Current time is past stop time. Stopping BI AutoScheduler.")
                    return -1  # used to be `break`` but it would hang here and wait for all futures to finish before exiting the loop
                # Retrieve a single task to pass into autsch so we can run the refresh
                logging.info("Querying for a new job from TABLEAU_JOB_ACTIVE_QUEUE...")
                retrieve_task = sf_conn.cursor().execute(autoscheduler_queries.fetch_a_job)

                new_job = []
                for row in retrieve_task:
                    new_job_dict = {
                        'LOG_ENTRY_DATE_UTC': row[0],
                        'OBJECT_TYPE': row[1],
                        'OBJECT_LUID': row[2],
                        'OBJECT_NAME': row[3],
                        'QUEUE_POSITION': row[4]
                    }
                    new_job.append(new_job_dict)

                if new_job:
                    return new_job  # Exit the loop and function if a job is found

                # If no tasks found, wait for 10 minutes before checking again
                logging.warning(f"{log_prefix}: No job available! Waiting 1 minute before checking again.")
                time_module.sleep(60)
                logging.info(f"{log_prefix}: Waited 1 minutes, attempting to fetch a job again.")

            except Exception as e:
                process_end_time = datetime.utcnow()
                process_id = 'job_fetcher'
                process_status = 'ERROR'
                message = f"Error occurred in job_fetcher(): {str(e)}"
                write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
                logging.error(str(e))
                raise Exception(f"{log_prefix}: Error occurred in fetching a job from TABLEAU_JOB_ACTIVE_QUEUE in Snowflake: ", str(e))


# Flag a retrieved task as "hot" in TABLEAU_JOB_ACTIVE_QUEUE
def active_job_update(job_pickup_timestamp, gate_elapsed_time_in_seconds, object_luid):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    process_start_time = datetime.utcnow()
    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        query_params = {
            'job_pickup_time_utc': job_pickup_timestamp,
            'gate_elapsed_time_in_seconds': gate_elapsed_time_in_seconds,
            'object_luid': object_luid
        }
        try:
            sf_conn.cursor().execute(autoscheduler_queries.active_job_update, query_params)
            sf_conn.commit()
            logging.info(f"active_job_update(): successful for OBJECT_LUID = {object_luid}")
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'active_job_update'
            process_status = 'ERROR'
            message = f"Error occurred in active_job_update(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception(
                f"active_job_update(): Error occurred updating JOB_PICKUP_TIME_UTC for {object_luid} in TABLEAU_JOB_ACTIVE_QUEUE: ", str(e)
            )


# Log the Job ID returned from Tableau Server into TABLEAU_JOB_HISTORY and increase ATTEMPT_COUNT by 1
# job_id_update(job_id, object_luid, active_job, job_pickup_timestamp, gate_elapsed_time_in_seconds)
def job_id_update(job_id, object_luid, active_job, job_pickup_timestamp, gate_elapsed_time_in_seconds):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    process_start_time = datetime.utcnow()
    # Parameters for the query
    job_and_object_ids = {
        'job_id': job_id,
        'object_luid': object_luid,
        'job_pickup_time_utc': job_pickup_timestamp,
        'gate_elapsed_time_in_seconds': gate_elapsed_time_in_seconds
    }

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.job_id_full_update, job_and_object_ids)
            sf_conn.commit()
            logging.info(f"job_id_update(): succesful for {active_job}")
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'job_id_update'
            process_status = 'ERROR'
            message = f"Error occurred in job_id_update(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception("job_id_update(): Error occurred updating JOB_ID in TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


def job_error_update(object_luid, active_job, job_pickup_timestamp, gate_elapsed_time_in_second, job_result_message):
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    # Parameters for the query
    process_start_time = datetime.utcnow()
    job_and_object_ids = {
        'object_luid': object_luid,
        'job_pickup_time_utc': job_pickup_timestamp,
        'gate_elapsed_time_in_seconds': gate_elapsed_time_in_second,
        'job_result_message': job_result_message
    }
    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.job_id_full_error_update, job_and_object_ids)
            sf_conn.commit()
            logging.info(f"Error Update for {active_job} successfull")
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'job_error_update'
            process_status = 'Failed'
            message = f"Error occurred in job_error_update(): {str(e)}"
            logging.exception(str(e))
            raise Exception("job_id_update(): Error occurred updating JOB_ID in TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e)))


# Log the Job Start Time so we can calculate the elapsed time in Snowflake
def job_start_time_update(job_id, job_created_time_utc, job_start_time_utc, active_job):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    process_start_time = datetime.utcnow()
    # Parameters for the query
    job_times = {'job_id': job_id, 'job_created_time_utc': job_created_time_utc, 'job_start_time_utc': job_start_time_utc}

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.job_start_time_update, job_times)
            sf_conn.commit()
            logging.info(f"job_start_time_update(): succesful for {active_job}")
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'job_start_time_update'
            process_status = 'ERROR'
            message = f"Error occurred in job_start_time_update(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception(
                "job_start_time_update(): Error occurred updating JOB_START_TIME_UTC in TABLEAU_JOB_ACTIVE_QUEUE: {}".format(str(e))
            )


# Log the result of a job into TABLEAU_JOB_ACTIVE_QUEUE
def log_job_result(job_result_dict):
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    active_job = job_result_dict['active_job']
    process_start_time = datetime.utcnow()
    # Parameters for the query
    job_result_query_params = {
        'job_created_at': job_result_dict['job_created_at'],
        'job_started_at': job_result_dict['job_started_at'],
        'job_completed_at': job_result_dict['job_completed_at'],
        'job_result_code': job_result_dict['job_result_code'],
        'job_result': job_result_dict['job_result'],
        "job_result_message": job_result_dict['job_result_message'],
        'object_luid': job_result_dict['object_luid']
    }

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.update_job_with_result, job_result_query_params)
            sf_conn.commit()
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'log_job_result'
            process_status = 'ERROR'
            message = f"Error occurred in log_job_result(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception(f"log_job_result(): Error occurred in trying to log job result to Snowflake: {str(e)}")

    logging.info(f"log_job_result(): succesful for Job {active_job}")


# Update TABLEAU_JOB_HISTORY with the results of all jobs that were executed today
def job_history_update():
    # fetch DB connection info
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    process_start_time = datetime.utcnow()
    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            sf_conn.cursor().execute(autoscheduler_queries.update_job_history_with_results)
            sf_conn.commit()
            logging.info(f"job_history_update(): successful!")
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'job_history_update'
            process_status = 'ERROR'
            message = f"Error occurred in job_history_update(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception(
                "job_history_update(): Error occurred updating TABLEAU_JOB_HISTORY with all job results for the day: {}".format(str(e))
            )


def snowflake_daily_wrap_up(**kwargs):
    log_prefix = "[DAILY WRAP UP]"
    logging.info(f"{log_prefix}: Starting Snowflake daily wrap-up, this is a 3 step process.")
    logging.info(f"{log_prefix}: Going to retrieve the date for the day we are wrapping up.")
    execution_date = autoscheduler_result_fetch()['log_entry_date_utc']
    logging.info(f"{log_prefix}: Execution Date - {execution_date}")
    logging.info(f"{log_prefix}: Good to go! Let's start the wrap-up process.")
    logging.info(f"{log_prefix}: Step 1 - Calling in the Job Resolver to see if there are any jobs that need to be resolved.")
    initalize_tableau_server()
    load_hist_job_run_time()
    job_resolver(execution_date)
    logging.info(f"{log_prefix}: Step 1 - Complete!")
    logging.info(f"{log_prefix}: Step 2 - Updating TABLEAU_JOB_HISTORY with the results of all jobs that were executed today.")
    job_history_update()
    logging.info(f"{log_prefix}: Step 2 -  Complete!")
    logging.info(f"{log_prefix}: Step 3 -  Updating DATABASE_READINESS_CHECK with the time that all data became ready today.")
    all_db_ready(**kwargs)
    logging.info(f"{log_prefix}: Step 3 -  Complete!")
    logging.info(f"{log_prefix}: All Steps Complete! Logging off for the day.")


#########################
# Tableau API Functions #
#########################

# Global scope for Tableau API functions
alerted_api_down = False
alerted_api_restored = False
api_status_lock = threading.Lock()
stop_time = time(23, 30)  # Stop BI AutoScheduler if we cross 23:00 UTC
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


def initalize_tableau_server():
    global sm
    tableau_server_conn_info = get_db_connection('tableau_cloud')
    # {'server': tableau_cloud_server, 'site': site_id, 'token_name': token_name, 'token_secret': token_secret}
    sm = ServerManager(
        token_name=tableau_server_conn_info['token_name'],
        token_secret=tableau_server_conn_info['token_secret'],
        site_content_url=tableau_server_conn_info['site'],
        server_endpoint=tableau_server_conn_info['server'],
        debounce_time=300
    )
    return


def load_hist_job_run_time():
    global hist_job_run_time
    log_prefix = "[HIST JOB RUN TIME LOADER]"
    logging.info(f"{log_prefix}: Loading historical job run times from Snowflake.")
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')

    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        try:
            process_start_time = datetime.utcnow()
            cursor = sf_conn.cursor()
            cursor.execute(autoscheduler_queries.fetch_avg_run_time)
            hist_job_run_time = cursor.fetchall()
            hist_job_run_time = {row[0]: float(row[1]) if row[1] is not None else 0.0 for row in hist_job_run_time}
            logging.info(f"{log_prefix}: Historical job run times loaded successfully.")
            print(hist_job_run_time)
        except Exception as e:
            process_end_time = datetime.utcnow()
            process_id = 'load_hist_job_run_time'
            process_status = 'ERROR'
            message = f"Error occurred in load_hist_job_run_time(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
            logging.exception(str(e))
            raise Exception(f"{log_prefix}: Error occurred in loading historical job run times from Snowflake: {str(e)}")


# Decorator to handle errors for all Tableau API functions
def tableau_server_error_handling(func):
    global alerted_api_down, alerted_api_restored, api_status_lock, sm, stop_time
    log_prefix = "[DECORATOR]"
    MAX_LOGIN_RETRIES = 8
    RETRY_BACKOFF = [180] * 5 + [240, 360, 480, 600]  # 2 minute pause for the first 5 retries, then 4, 6, 8, 10 minutes

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        retries = 0
        while retries < MAX_LOGIN_RETRIES:
            current_time = datetime.utcnow().time()
            if current_time >= stop_time:
                logging.warning(f"{log_prefix}: Current time is past stop time. Stopping BI AutoScheduler.")
                return
            logging.info(
                f"{log_prefix} starting attempt {retries + 1} of {MAX_LOGIN_RETRIES} for {func.__name__} called by thread: {threading.current_thread().name} with args: {args} and kwargs: {kwargs}"
            )
            server = sm.fetch_server()
            try:
                return func(server, *args, **kwargs)
            except TSC.NotSignedInError:
                error_msg = f"{log_prefix}: I'm not signed in to Tableau Server! Going to authenticate now."
                logging.warning(error_msg)
                #_SERVER_CACHE.pop("server", None)
                sm.refresh_server()
            except (OSError, ConnectionError) as e:
                # Handle these exceptions and attempt re-authentication
                if "Connection aborted" in str(e):
                    error_msg = f"{log_prefix}: Connection aborted! This could be due to a network issue. Attempting to re-authenticate."
                    logging.warning(error_msg)
                    #_SERVER_CACHE.pop("server", None)
                    #sm.refresh_server()
            except Exception as e:
                if "Missing site ID. You must sign in first." in str(e):
                    error_msg = f"{log_prefix}: Authentication required due to missing site ID. Attempting to re-authenticate."
                    logging.warning(error_msg)
                    #_SERVER_CACHE.pop("server", None)
                    sm.refresh_server()
                elif "Invalid authentication credentials were provided" in str(e):
                    error_msg = f"{log_prefix}: Invalid authentication credentials provided. Attempting to re-authenticate."
                    logging.warning(error_msg)
                    sm.refresh_server()
                # Special handling for API version error or API 502 error (Tableau API is completely dark)
                elif "not available in API" in str(e) or "Could not get version info from server" in str(
                        e) or "Error status code: 502" in str(e):
                    logging.warning(f"{log_prefix}: API is down or version mismatch. Retrying in 10 minutes.")
                    with api_status_lock:
                        if not alerted_api_down:
                            api_down_slack_alert = (
                                f"Tableau API is down! I encountered the following error: {e}. I'll keep trying to reach the server indefinitely, but please check on the server if this was not expected."
                            )
                            send_slack_message(api_down_slack_alert)
                            alerted_api_down = True
                    while True:  # Try to authenticate every 10 minutes until successful
                        time_module.sleep(600)  # Sleep for 10 minutes
                        try:
                            server = sm.fetch_server()
                            if server is not None:
                                logging.info(f"{log_prefix}: Successfully authenticated after API was down.")
                                with api_status_lock:
                                    if not alerted_api_restored:
                                        api_restored_message = "Tableau API is back up! I was able to successfully authenticate. Continuing with normal operations."
                                        send_slack_message(api_restored_message)
                                        alerted_api_restored = True
                                break  # Exit the infinite loop if authentication is successful
                        except Exception as retry_exception:
                            logging.warning(
                                f"{log_prefix}: Failed to authenticate after API was down. Error: {retry_exception}. Retrying in 10 minutes."
                            )
                # For any other unexpected exceptions, raise them immediately without retrying
                else:
                    logging.error(f"{log_prefix}: Unexpected error: {e}")
                    return

            #logging.warning(f"{error_msg}. Trying attempt {retries + 1} of {MAX_LOGIN_RETRIES}.")

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
        logging.error(
            f"Max retries reached ({MAX_LOGIN_RETRIES}) for {func.__name__} called by {threading.current_thread().name} Exiting function"
        )
        return

    return wrapper


# Centralized function to make Jobitem Class from TSC more readable and usable
def tableau_job_result_dict(
    thread_id=None,
    thread_name=None,
    thread_start_time=None,
    object_luid=None,
    object_name=None,
    object_type=None,
    active_job=None,
    job_pickup_timestamp=None,
    gate_elapsed_time_pretty=None,
    job_result=None,
    return_format='dict'
):
    # Create a dictionary for the Tableau Job Result
    FINISH_CODE_MAPPING = {0: "Success", 1: "Failure", 2: "Cancelled", -1: "In Progress"}

    if job_result.finish_code in (1, 2):
        # For failed or cancelled jobs, extract the error message from the notes
        if isinstance(job_result.notes, list):
            entire_message = ' '.join(job_result.notes)
        else:
            entire_message = job_result.notes
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
    else:
        job_result_message = None

    job_result_dict = {
        "thread_id": thread_id,
        "thread_name": thread_name,
        "future_time": time_module.time() - thread_start_time if thread_start_time else None,
        "object_luid": object_luid,
        "object_name": object_name,
        "object_type": object_type,
        "active_job": active_job,
        "job_pickup_timestamp": job_pickup_timestamp,
        "gate_elapsed_time": gate_elapsed_time_pretty,
        "job_id": job_result.id if job_result else None,
        "job_type": job_result.type if job_result else None,
        "job_created_at": job_result.created_at if job_result else None,
        "job_started_at": job_result.started_at if job_result else None,
        "job_completed_at": job_result.completed_at if job_result else None,
        "job_duration": (job_result.completed_at - job_result.started_at).total_seconds() if job_result.completed_at else None,
        "job_result_code": job_result.finish_code if job_result else None,
        "job_result": FINISH_CODE_MAPPING.get(job_result.finish_code, "Unknown") if job_result else None,
        "job_result_message": job_result_message
    }
    formatted_future_time = f"{job_result_dict.get('future_time'):.2f}" if job_result_dict.get('future_time') is not None else None
    formatted_job_duration = f"{job_result_dict.get('job_duration'):.2f}" if job_result_dict.get('job_duration') is not None else None
    job_result_pretty = f"""
    +---------------------------------------------+
    | Future ID: {job_result_dict.get('thread_id', 'None')}
    | Future Name: {job_result_dict.get('thread_name', 'None')}
    | Futured Time: {formatted_future_time}
    | Object ID: {job_result_dict.get('object_luid', 'None')}
    | Object Name: {job_result_dict.get('object_name', 'None')}
    | Object Type: {job_result_dict.get('object_type', 'None')}
    | Active Job: {job_result_dict.get('active_job', 'None')}
    | Job ID: {job_result_dict.get('job_id', 'None')}
    | Job Type: {job_result_dict.get('job_type', 'None')}
    | Job Pickup Time: {job_result_dict.get('job_pickup_timestamp', 'None')}
    | Gate Elapsed Time: {job_result_dict.get('gate_elapsed_time', 'None')}
    | Job Created At: {job_result_dict.get('job_created_at', 'None')}
    | Job Started At: {job_result_dict.get('job_started_at', 'None')}
    | Job Completed At: {job_result_dict.get('job_completed_at', 'None')}
    | Job Duration: {formatted_job_duration}
    | Job Result Code: {job_result_dict.get('job_result_code', 'None')}
    | Job Result: {job_result_dict.get('job_result', 'None')}
    | Job Result Message: {job_result_dict.get('job_result_message', 'None')}
    +---------------------------------------------+
    """

    if return_format == 'dict':
        return job_result_dict
    elif return_format == 'pretty':
        return job_result_pretty
    elif return_format == 'both':
        return job_result_dict, job_result_pretty


@tableau_server_error_handling
def job_resolver(server, execution_date):
    global thread_start_times, thread_job_info
    log_prefix = "[JOB RESOLVER]"

    logging.info(f"{log_prefix}: Checking to see if we left any in-progress jobs hanging from any earlier run(s).")
    in_progress_jobs = retrieve_in_progress_jobs()

    if not in_progress_jobs:
        logging.info(f"{log_prefix}: No jobs in need of resolving! We're good to go.")
        return

    logging.info(f"{log_prefix}: Found {len(in_progress_jobs)} job(s) left hanging. Checking Tableau Server for their status.")

    # Making the watcher aware of our presence
    thread_name = threading.current_thread().name
    thread_start_times[thread_name] = time_module.time()
    thread_job_info[thread_name] = {'object_name': 'Job Resolver', 'object_luid': 'Job Resolver'}

    # Lists to hold job results
    jobs_on_server_finished = []
    jobs_on_server_failed = []
    jobs_on_server_in_progress = []

    #  {'LOG_ENTRY_DATE_UTC': row[0], 'OBJECT_LUID': row[1], 'OBJECT_NAME': row[2], 'JOB_ID': row[3]}

    for jobs in in_progress_jobs:
        try:
            job_id = jobs['JOB_ID']
            object_luid = jobs['OBJECT_LUID']
            object_name = jobs['OBJECT_NAME']
            active_job = f"Job - [{object_name} ({object_luid})]"
            job = server.jobs.get_by_id(job_id)
            logging.info(f"{log_prefix} job details {job}")
            job_result_dict = tableau_job_result_dict(
                object_luid=object_luid, object_name=object_name, job_result=job, active_job=active_job
            )

            # Categorize the job based on its finish code
            if job_result_dict['job_result_code'] == -1:
                jobs_on_server_in_progress.append(job_result_dict)
            elif job_result_dict['job_result_code'] in (1, 2):
                jobs_on_server_failed.append(job_result_dict)
            elif job_result_dict['job_result_code'] == None:
                logging.error(f"{log_prefix}: Job ({job_id}) returned a None finish code. This is unexpected! Please investigate.")
            else:
                jobs_on_server_finished.append(job_result_dict)

        except Exception as e:
            logging.error(f"{log_prefix}: Error retrieving status for Job ({job_id}): {str(e)}")
            continue

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
            for i, job in enumerate(job_list):
                try:
                    job_id = job['job_id']
                    active_job = job['active_job']
                    updated_job_result = job_phantom(job_id, active_job, execution_date)
                    # Create a new job dictionary with the updated results
                    updated_job_dict = tableau_job_result_dict(
                        object_name=job['object_name'], object_luid=job['object_luid'], job_result=updated_job_result
                    )
                    # Replace the old job dictionary with the new one
                    job_list[i] = updated_job_dict
                    job_result_code, job_started_at, job_completed_at = updated_job_result.finish_code, updated_job_result.started_at, updated_job_result.completed_at
                    if job_result_code == 0 and ((job_completed_at - job_started_at) < timedelta(minutes=2)):
                        logging.info(
                            f"{log_prefix}: Job {active_job} completed in less than 2 minutes. Sending to delayer to verify it's true run time"
                        )
                        delay_data = job_delayer(updated_job_dict)
                        if delay_data is None:
                            updated_job_dict['job_result'] = "Success-Bridge"
                            updated_job_dict['job_result_message'] = "Unable to delay due to reaching stop time or error"
                        else:
                            updated_job_dict['job_result'] = delay_data['JOB_RESULT']
                            updated_job_dict['job_result_message'] = delay_data['JOB_RESULT_MESSAGE']
                    log_job_result(updated_job_dict)
                except Exception as e:
                    logging.error(f"{log_prefix}: Unknown Error while waiting for Job ({job_id}): {str(e)}")
                    continue

    logging.info(f"{log_prefix}: Job Resolver has finished!")
    future_truncator(thread_name)


# Check how many backgrounders are currently in use on Tableau Server
@tableau_server_error_handling
def get_active_backgrounders_count(server):
    try:
        process_start_time = datetime.utcnow()
        req = TSC.RequestOptions()
        req.filter.add(TSC.Filter("status", TSC.RequestOptions.Operator.Equals, 'InProgress'))
        count = len(list(TSC.Pager(server.jobs, request_opts=req)))
        return count
    except Exception as e:
        # cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
        # with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
        #     process_end_time = datetime.utcnow()
        #     process_id = 'get_active_backgrounders_count'
        #     process_status = 'ERROR'
        #     message = f"Error occurred in get_active_backgrounders_count(): {str(e)}"
        #     write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
        # logging.error(f"[BACKGROUNDER COUNT]: Error occurred while checking backgrounders: {str(e)}")
        raise e


# Function to gate autsch until backgrounder bandwidth is optimal
def tableau_task_gate():
    # Consistent logging prefix
    log_prefix = "[TASK GATE]"
    global stop_time
    backgrounder_count = get_active_backgrounders_count()

    logging.info(f"{log_prefix}: Backgrounders in use: {backgrounder_count}")
    #backgrounder_count = None
    while backgrounder_count > 10:
        current_time = datetime.now(timezone.utc)
        if current_time.time() >= stop_time:
            logging.warning(f"{log_prefix}: Current time is past stop time. Stopping Task Gating")
            return -1
        logging.warning(f'{log_prefix}: All backgrounders currently in use! Waiting 30 seconds.')
        time_module.sleep(30)
        backgrounder_count = get_active_backgrounders_count()
        logging.info(f"{log_prefix}: Backgrounders in use: {backgrounder_count}")


# Function to watch a job on Tableau Server until it completes
JOB_PHANTOM_STOP_TIME_LOGGED = False


@tableau_server_error_handling
def job_phantom(server, job_id, active_job, execution_date):
    global JOB_PHANTOM_STOP_TIME_LOGGED
    # Consistent logging prefix
    log_prefix = "[JOB PHANTOM]"

    # Create a datetime object for the stop time which would be 00:40 UTC the next day
    JOB_WAIT_STOP_TIME = datetime.combine(execution_date + timedelta(days=1), time(0, 30))
    if not JOB_PHANTOM_STOP_TIME_LOGGED:
        logging.info(f"a phantom appears...")
        logging.info(f"{log_prefix}: Job Wait Stop Time for today is {JOB_WAIT_STOP_TIME}.")
        JOB_PHANTOM_STOP_TIME_LOGGED = True

    logging.info(f"{log_prefix}: Shadowing Job {job_id} for {active_job}")
    job = server.jobs.get_by_id(job_id)

    if job.completed_at is not None:
        logging.info(f"{log_prefix}: Job {job_id} for {active_job} is already complete. Reporting back to the runner.")
        return job
    elif job.completed_at is None:
        time_module.sleep(5)
        job_start_time_called = False
        while job.completed_at is None:
            try:
                job = server.jobs.get_by_id(job_id)
                if job.started_at is None:
                    job_created_at = job.created_at
                    logging.info(f"{log_prefix}: Job {job_id} for {active_job} has not started! Job was created at {job_created_at}.")
                    logging.info(f"{log_prefix}: Waiting 30 seconds before checking again.")
                    time_module.sleep(30)
                else:
                    if not job_start_time_called:
                        job_created_at = job.created_at
                        job_started_at = job.started_at
                        job_start_time_update(job_id, job_created_at, job_started_at, active_job)
                        job_start_time_called = True

                    job_start_time = job.started_at
                    current_time = datetime.now(timezone.utc)
                    progress_check = current_time - job_start_time
                    progress_minutes, progress_seconds = divmod(progress_check.total_seconds(), 60)
                    progress_duration = f"{int(progress_minutes):02d}:{int(progress_seconds):02d}"
                    logging.info(f"{log_prefix}: Job {job_id} for {active_job} in progress. Duration: {progress_duration} Minute(s).")
                    time_module.sleep(30)

                if datetime.utcnow() >= JOB_WAIT_STOP_TIME:
                    logging.warning(f"{log_prefix}: Job {job_id} for {active_job} has crossed the stop time. Attempting to cancel the job.")
                    try:
                        server.jobs.cancel(job_id)
                        logging.warning(
                            f"{log_prefix}: Job {job_id} for {active_job} has been succesfully cancelled. Waiting 10 seconds before retrieving the final job details and reporting back to the runner."
                        )
                        time_module.sleep(10)
                        job = server.jobs.get_by_id(job_id)
                    except TSC.ServerResponseError:
                        logging.error(f"{log_prefix}: Failed to cancel Job {job_id} for {active_job}.")
                    break
            except Exception as e:
                logging.error(f"{log_prefix}: Unknown Error while waiting for Job ({job_id}): {str(e)}")
                logging.exception(e)
                raise e

        job_start_time = job.started_at
        job_complete_time = job.completed_at
        final_job_duration = job_complete_time - job_start_time
        total_job_minutes, total_job_seconds = divmod(final_job_duration.total_seconds(), 60)
        total_job_duration = f"{int(total_job_minutes):02d}:{int(total_job_seconds):02d}"
        logging.info(
            f"{log_prefix}: Job {job_id} for {active_job} complete! Total duration: {total_job_duration}. Reporting back to the runner."
        )

    return job


##########################################
# Multi-Threaded Orchestration Functions #
##########################################

# Global scope for use in all futures dependent functions
active_futures = []  # track active futures
thread_start_times = {}
thread_job_info = {}
MAX_FUTURES = 10  # futures == threads == concurrent tableau jobs

# Todo : update the number of max_futures


def future_truncator(thread_name):
    if thread_name in thread_start_times:
        del thread_start_times[thread_name]
    if thread_name in thread_job_info:
        del thread_job_info[thread_name]


# Function to monitor active futures
def future_watcher():
    global active_futures, thread_start_times, thread_job_info

    while True:
        # Remove completed futures from the list
        active_futures = [f for f in active_futures if not f.done()]

        # Gather info for all active tasks into a list of dictionaries
        futures_list = []
        for thread_number, (thread_name, start_time) in enumerate(thread_start_times.items(), start=1):
            # Calculate how long the thread has been running
            elapsed_time = time_module.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            formatted_elapsed_time = f"{minutes}:{seconds:02}"

            # Get the associated task info
            futures_info = thread_job_info.get(thread_name, {})

            # Add this task's info to the list
            futures_list.append({
                'thread_number': thread_number,
                'thread_name': thread_name,
                'elapsed_time': formatted_elapsed_time,
                'object_name': futures_info.get('object_name', 'Unknown'),
                'object_luid': futures_info.get('object_luid', 'Unknown')
            })

        # Print the table of task info

        active_thread_count = len(active_futures)
        print(f"Active Futures: {active_thread_count}")

        print_futures(futures_list)

        # Print the number of active threads

        time_module.sleep(60)  # Log every 1 minute


def print_futures(futures_list):
    # Define a format string with columns of a fixed width
    format_string = "{:<6} {:<15} {:<50} {:<30} {:<20}"

    # Print the header
    print(format_string.format("Future", "Future ID", "Object Name", "Object LUID", "Elapsed Time (min)"))
    print("-" * 125)  # Print a line of dashes as a separator

    # Print each row of task info
    for task_info in futures_list:
        print(
            format_string.format(
                task_info['thread_number'], task_info['thread_name'], task_info['object_name'], task_info['object_luid'],
                str(task_info['elapsed_time'])
            )
        )


# Function to start the thread that monitors the futures if it's not already active
def wake_the_future_watcher():
    if not hasattr(wake_the_future_watcher, "thread_activity_thread") or not wake_the_future_watcher.thread_activity_thread.is_alive():
        logging.info("Future Watcher is not awake! The bell tolls for thee...")
        wake_the_future_watcher.thread_activity_thread = threading.Thread(target=future_watcher)
        wake_the_future_watcher.thread_activity_thread.daemon = True
        wake_the_future_watcher.thread_activity_thread.start()
    else:
        logging.info("the future is being watched...")


@tableau_server_error_handling
def tableau_future_runner(
    server, active_job, object_luid, object_name, object_type, job_pickup_timestamp, gate_elapsed_time_pretty, execution_date,
    gate_elapsed_time_in_seconds
):
    global thread_start_times, thread_job_info

    # Start the thread that monitors the active futures
    wake_the_future_watcher()
    # Future Scope
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    thread_start_times[thread_name] = time_module.time()
    thread_start_time = time_module.time()
    thread_job_info[thread_name] = {'object_name': object_name, 'object_luid': object_luid}
    log_prefix = f"[FUTURE RUNNER] Future [{thread_name} ({thread_id})]"
    logging.info(f"{log_prefix}: Received {active_job}. Sending to Tableau for refresh.")

    try:
        # if object_luid == '0e0a8f65-b813-482e-bfa1-05d1d5c25b92':
        #     raise TSC.NotSignedInError("This is a test error to simulate a Tableau API error.")
        # Refresh the object based on its type
        if object_type.lower() == "datasource":
            datasource = server.datasources.get_by_id(object_luid)
            job = server.datasources.refresh(datasource)
        elif object_type.lower() == "workbook":
            workbook = server.workbooks.get_by_id(object_luid)
            job = server.workbooks.refresh(workbook)
        elif object_type.lower() == "flows":
            flow = server.flows.get_by_id(object_luid)
            job = server.flows.refresh(flow)
        else:
            logging.error(f"{log_prefix}: Invalid object type '{object_type}'. Expected 'datasource' or 'workbook' or 'flows'.")
            return

        job_id = job.id

        if job_id:
            logging.info(
                f"{log_prefix}: Job ID for {active_job} is {job_id}. Logging JOB_ID to Snowflake then waiting 10 seconds before invoking job phantom..."
            )
            job_id_update(job_id, object_luid, active_job, job_pickup_timestamp, gate_elapsed_time_in_seconds)
            time_module.sleep(10)
            job_result = job_phantom(job_id, active_job, execution_date)
            logging.info(f"{log_prefix}: Job Result received for {active_job}.")
            job_dict, job_result_pretty = tableau_job_result_dict(
                thread_id,
                thread_name,
                thread_start_time,
                object_luid,
                object_name,
                object_type,
                active_job,
                job_pickup_timestamp,
                gate_elapsed_time_pretty,
                job_result,
                return_format='both'
            )
            job_result_code, job_started_at, job_completed_at = job_result.finish_code, job_result.started_at, job_result.completed_at
            if job_result_code == 0 and ((job_completed_at - job_started_at) < timedelta(minutes=2)):
                logging.info(
                    f"{log_prefix}: Job {active_job} completed in less than 2 minutes. Sending to delayer to verify it's true run time"
                )
                delay_data = job_delayer(job_dict)
                if delay_data is None:
                    job_dict['job_result'] = "Success-Bridge"
                    job_dict['job_result_message'] = "Unable to delay due to reaching stop time or error"
                else:
                    job_dict['job_result'] = delay_data['JOB_RESULT']
                    job_dict['job_result_message'] = delay_data['JOB_RESULT_MESSAGE']
        else:
            logging.error(
                f"{log_prefix}: Job ID for {active_job} returned None!! This is very unexpected, please investigate on Tableau side."
            )
            return None

        logging.info(job_result_pretty)
        logging.info(f"{log_prefix}: Logging the results of the Job {job_id} for {active_job} to Snowflake.")
        log_job_result(job_dict)
        logging.info(f"{log_prefix}: Future Complete. Returning to the present for a new job.")

    except Exception as e:
        job_error_update(object_luid, active_job, job_pickup_timestamp, gate_elapsed_time_in_seconds, job_result_message=str(e))
        logging.error(f"{log_prefix}: Error encountered while future running for {active_job}")
        logging.exception(e)
        raise e

    finally:
        future_truncator(thread_name)


def time_watcher(func):
    global stop_time
    log_prefix = "[TIME WATCHER DECORATOR]"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        current_time = datetime.utcnow().time()
        try:
            if current_time >= stop_time:
                logging.warning(f"{log_prefix}: It's past stop time. Stopping the function.")
                return
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"{log_prefix}: Error occurred in function: {str(e)}")
            return

    return wrapper


@time_watcher
def job_delayer(job_dict):
    global hist_job_run_time
    log_prefix = "[JOB DELAYER]"
    try:
        object_luid, object_name, job_started_at, job_completed_at = job_dict['object_luid'], job_dict['object_name'], job_dict[
            'job_started_at'], job_dict['job_completed_at']
        logging.info(f"{log_prefix}: Job {object_name} is being sent to the delayer to verify it's true run time.")
        if object_name in hist_job_run_time:
            logging.info(f"{log_prefix}: Job {object_name} has a historical run time of {hist_job_run_time[object_name]} minutes.")
            current_run_time = (job_completed_at - job_started_at).total_seconds()
            hist_run_time = hist_job_run_time[object_name] * 60 if hist_job_run_time[object_name] is not None else 0
            if current_run_time < hist_run_time:

                logging.info(
                    f"{log_prefix}: Job {object_name} has run for less than the historical run time of {hist_run_time} seconds. Delaying the job."
                )
                # delay the job for min(30*60, hist_run_time - current_run_time) seconds
                delay_time = min(15 * 60, (hist_run_time - current_run_time))
                res = delay_simulator(delay_time, object_name)
                return {
                    "JOB_RESULT":
                    "Success-Bridge",
                    "JOB_RESULT_MESSAGE":
                    f"Delayed for {delay_time} seconds based on historical run time of {hist_run_time} seconds"
                    if res else "Unable to delay due to reaching stop time or error"
                }
            else:
                logging.info(f"{log_prefix}: Job {object_name} has run for longer than the historical run time. Not delaying the job.")
                return {
                    "JOB_RESULT": "Success",
                    "JOB_RESULT_MESSAGE": f"Not delayed. Run time greater than or equal to historical run time of {hist_run_time} seconds"
                }
        else:
            logging.info(f"{log_prefix}: Job {object_name} has no historical run time. Waiting out 5 minutes, just for safety.")
            delay_simulator(300, object_name)
            return {
                "JOB_RESULT": "Success-Bridge",
                "JOB_RESULT_MESSAGE": "Historical run time not found. Waited out 5 minutes just for safety"
            }
    except Exception as e:
        logging.error(f"{log_prefix}: Error occurred in function: {str(e)}")
        return


def delay_simulator(delay_time, object_name):
    log_prefix = "[DELAY FUNCTION]"
    try:
        logging.info(f"{log_prefix}: Delaying the function for {delay_time} seconds for {object_name}.")
        # incremental delays with checking for stop time every 5 minutes
        while delay_time > 0:
            current_time = datetime.utcnow().time()
            if current_time >= stop_time:
                logging.warning(f"{log_prefix}: It's past stop time. Stopping the delay function.")
                return False
            if (delay_time - 300) < 0:
                time_module.sleep(delay_time)
                delay_time = 0
            else:
                time_module.sleep(300)
                delay_time -= 300
        logging.info(f"{log_prefix}: Delay complete for {object_name}")
        return True
    except Exception as e:
        logging.error(f"{log_prefix}: Error occurred in function: {str(e)}")
        return False


##############################
# BI AutoScheduler Vena Cava #
##############################

pq = queue.PriorityQueue()
seen = set()


@time_watcher
def put_object_in_queue(object):
    log_prefix = "[BI Autoscheduler PQ Handler - OBJECT PUT]"
    global pq
    process_start_time = datetime.utcnow()
    try:
        flg = False
        pq.put_nowait(object)
        logging.info(f"{log_prefix}: Object added to the queue: {object}")
        flg = True
    except queue.Full:
        # since this is an inifinite length queue, we should never hit this
        logging.error(f"{log_prefix}: The priority queue is full! This object will not be added to the queue.")
    except Exception as e:
        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_end_time = datetime.utcnow()
            process_id = 'put_object_in_queue'
            process_status = 'ERROR'
            message = f"Error occurred in put_object_in_queue(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
        logging.error(f"{log_prefix}: Error occurred adding task to the queue: {str(e)}")
    finally:
        return flg


@time_watcher
def get_object_from_queue():
    log_prefix = "[BI Autoscheduler PQ Handler - OBJECT GET]"
    global pq
    process_start_time = datetime.utcnow()
    try:
        task = None
        task = pq.get_nowait()
        logging.info(f"{log_prefix}: Task retrieved from the queue: {task}")
        return task
    except queue.Empty:
        logging.info(f"{log_prefix}: The priority queue is empty! No tasks to retrieve.")
    except Exception as e:
        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_end_time = datetime.utcnow()
            process_id = 'get_object_from_queue'
            process_status = 'ERROR'
            message = f"Error occurred in get_object_from_queue(): {str(e)}"
            write_to_execution_logs(sf_conn, process_name, process_id, process_status, process_start_time, process_end_time, message)
        logging.error(f"{log_prefix}: Error occurred retrieving task from the queue: {str(e)}")
    finally:
        return task


@time_watcher
def populator_object_fetch():  # Possible return value: [(queue_position, task_luid), ...] || [] || None
    global pq
    global seen
    log_prefix = "[BI Autoscheduler Populator - OBJECT FETCHER]"
    #autoscheduler_edw_snowflake_dev_conn_info = get_db_connection('autoscheduler_edw_snowflake_dev')
    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
    try:
        process_start_time = datetime.utcnow()
        res = []
        # Fetch all the tasks that are ready to be executed
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            sf_cursor = sf_conn.cursor()
            sf_cursor.execute(autoscheduler_queries.populator_object_fetch)
            objects = sf_cursor.fetchall()
            logging.info(f"{log_prefix}: Fetched {len(objects)} objects from Snowflake.")
            for obj in objects:
                obj_dict = {
                    "LOG_ENTRY_DATE_UTC": obj[0],
                    "OBJECT_TYPE": obj[1],
                    "OBJECT_LUID": obj[2],
                    "OBJECT_NAME": obj[3],
                    "QUEUE_POSITION": obj[4]
                }
                if obj_dict['OBJECT_LUID'] not in seen:
                    res.append((obj_dict['QUEUE_POSITION'], obj_dict))
        return res
    except Exception as e:
        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_end_time = datetime.utcnow()
            process_id = 'populator_object_fetch'
            process_status = 'ERROR'
            message = f"Error occurred in populator_object_fetch(): {str(e)}"
            write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
        logging.error(f"{log_prefix}: Error occurred fetching tasks from Snowflake: {str(e)}")
        return []


def populator():
    global pq
    global seen
    global stop_time

    log_prefix = "[BI Autoscheduler Populator]"

    # populate seen, with all the tasks that have been executed today
    try:
        process_start_time = datetime.utcnow()
        seen.update(executed_jobs_today())
        logging.info(f"{log_prefix}: Seen set updated with {len(seen)} tasks.")
        total_job_count = autoscheduler_result_fetch()['total_job_count']
        while len(seen) < total_job_count:
            try:
                current_time = datetime.utcnow().time()
                if current_time >= stop_time:
                    logging.warning(f"{log_prefix}: We are past stop time, shutting down populator")
                    break
                objects_for_refresh = populator_object_fetch()
                if objects_for_refresh:
                    for obj in objects_for_refresh:
                        if put_object_in_queue(obj):
                            seen.add(obj[1]['OBJECT_LUID'])
                    logging.info(f"{log_prefix}: Populator has populated the queue with {len(objects_for_refresh)} objects.")
                current_time = datetime.utcnow().time()
                if current_time >= stop_time:
                    logging.warning(f"{log_prefix}: We are past stop time, shutting down populator")
                    break
                if len(seen) >= total_job_count:
                    logging.info(f"{log_prefix}: Populator has finished populating the queue with {len(seen)} objects.")
                    break
                logging.info(f"{log_prefix}: Populator sleeping for 15 minutes...")
                time_module.sleep(900)
            except Exception as e:
                logging.error(f"{log_prefix}: Error occurred in populator: {str(e)}")
                continue

        logging.info(f"{log_prefix}: Populator has finished populating the queue with {len(seen)} objects.")
        logging.info(f"{log_prefix}:Populator exiting...")
    except Exception as e:
        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_end_time = datetime.utcnow()
            process_id = 'populator'
            process_status = 'ERROR'
            message = f"Error occurred in populator(): {str(e)}"
            write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)

        logging.error(f"{log_prefix}: Error occurred in populator: {str(e)}")
        slack_message = f":police_light: BI AutoScheduler Populator has encountered an error and has stopped. Please investigate. :police_light:"
        send_slack_message(slack_message)


def bi_autoscheduler(**kwargs):
    global active_futures
    log_prefix = "[BI AUTOSCHEDULER]"
    process_start_time = datetime.utcnow()
    process_id = 'BI AutoScheduler'
    autsch_art()
    logging.info(f"{log_prefix}: BI AutoScheduler booting up!")
    initalize_tableau_server()
    load_hist_job_run_time()
    time_module.sleep(30)
    # Retrieve what the total job count is for the day and create the stop time for the day
    logging.info(f"{log_prefix}: Retrieving daily metadata from Snowflake.")
    daily_metadata = autoscheduler_result_fetch()
    execution_date = daily_metadata['log_entry_date_utc']
    TOTAL_JOB_COUNT_FOR_DAY = daily_metadata['total_job_count']
    JOB_SUBMIT_STOP_TIME = datetime.combine(execution_date, time(23, 30))
    # the amount of jobs we need to run today
    logging.info(f"{log_prefix}: Total Job Count for today is {TOTAL_JOB_COUNT_FOR_DAY}")
    # the timestamp when we stop submitting jobs for today
    logging.info(f"{log_prefix}: Job Submit Stop Time for today is {JOB_SUBMIT_STOP_TIME}")

    halfway_point = TOTAL_JOB_COUNT_FOR_DAY // 2
    # Initialize executed_jobs with tasks we've already sent to Tableau Server today
    executed_jobs = set()

    logging.info(f"{log_prefix}: Starting the BI AutoScheduler Vena Cava Background Threads..")
    background_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    populator_future = background_executor.submit(populator)
    populator_future.add_done_callback(
        lambda x: logging.info(f"{log_prefix}: Populator has finished with status: {x.result()} and exited due to {x.exception()}.")
    )
    active_futures.append(populator_future)

    if not executed_jobs:
        logging.info(f"{log_prefix}: executed_jobs list created. Checking Snowflake to see if we've already ran today.")
        executed_jobs.update(executed_jobs_today())
        logging.info(f"{log_prefix}: Total Executed Jobs = {len(executed_jobs)}")
    try:
        if len(executed_jobs) > 0 and len(executed_jobs) < TOTAL_JOB_COUNT_FOR_DAY:
            logging.info(f"{log_prefix}: This is a rerun! Invoking the Job Resolver to check on the jobs we ran earlier today.")
            job_resolver_future = background_executor.submit(job_resolver, execution_date)
            active_futures.append(job_resolver_future)
    except Exception as e:
        cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
        with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
            process_end_time = datetime.utcnow()
            process_status = 'ERROR'
            message = f"Error occurred in bi_autoscheduler(): {str(e)}"
            write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
        logging.error(f"Error starting resolver: {e}")
    wake_the_future_watcher()
    logging.info(f"{log_prefix}: Starting the BI AutoScheduler Vena Cava...")
    background_executor.shutdown(wait=False)

    time_module.sleep(30)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FUTURES) as executor:
        while len(executed_jobs) < TOTAL_JOB_COUNT_FOR_DAY:
            # Check the current time against the stop_time
            current_time = datetime.utcnow()
            if current_time >= JOB_SUBMIT_STOP_TIME:
                current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                logging.warning(
                    f"{log_prefix}: Current time is {current_timestamp} which is past stop time of {JOB_SUBMIT_STOP_TIME}. Stopping BI AutoScheduler Vena Cava."
                )
                return
            if len(executed_jobs) == halfway_point:
                current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                logging.info(f"{log_prefix}: Halfway Point reached!- {len(executed_jobs)} of {TOTAL_JOB_COUNT_FOR_DAY} Tasks Exectued")
                send_slack_message(
                    f"We have reached the halfway point at {current_timestamp} UTC! {len(executed_jobs)} of {TOTAL_JOB_COUNT_FOR_DAY} tasks have been executed. If you'd like to see my progress, please visit the Snowflake workbook in the _Canvas_ of this channel :thumbsup:"
                )
                halfway_point = -1  # Only send the halfway point alert once
            if len(active_futures) < MAX_FUTURES:
                logging.info(f"{log_prefix}: Progress Check-In - {len(executed_jobs)} of {TOTAL_JOB_COUNT_FOR_DAY} Tasks Exectued")

                try:
                    logging.info(f"{log_prefix}: waiting at the gate...")
                    gate_entry_time = time_module.time()
                    tableau_task_gate()
                    gate_elapsed_time = time_module.time() - gate_entry_time
                    minutes = int(gate_elapsed_time // 60)
                    seconds = int(gate_elapsed_time % 60)
                    gate_elapsed_time_in_seconds = int(gate_elapsed_time)
                    gate_elapsed_time_pretty = f"{minutes}:{seconds:02}"
                    logging.info(
                        f"{log_prefix}: Backgrounder(s) available! Opening the gate, Gate Elapsed Time: {gate_elapsed_time_pretty} minute(s)"
                    )
                except Exception as e:
                    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
                    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
                        process_end_time = datetime.utcnow()
                        process_status = 'ERROR'
                        message = f"Error occurred in tableau_task_gate(): {str(e)}"
                        write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
                    logging.error(f"{log_prefix}: Error occurred in waiting at the gate: {str(e)}")
                    err = f'Error occurred in waiting at the gate: {str(e)}'
                    send_slack_message(err)
                    return
                # Retrieve a task to run
                try:
                    # job_to_run = job_fetcher()
                    # if job_to_run == -1:
                    #     logging.warning(f"{log_prefix}: Stop Time breached. Stopping BI AutoScheduler Vena Cava.")
                    #     break
                    # job_pickup_timestamp = datetime.utcnow()
                    # object_luid = job_to_run[0]['OBJECT_LUID']
                    # object_name = job_to_run[0]['OBJECT_NAME']
                    # queue_position = job_to_run[0]['QUEUE_POSITION']
                    # object_type = job_to_run[0]['OBJECT_TYPE']
                    # active_job = f"[{object_name} ({object_luid})]"

                    job_to_run = get_object_from_queue()
                    if job_to_run is None:
                        logging.info(f"{log_prefix}: No objects to refresh. Waiting 2 minutes before checking again.")
                        time_module.sleep(120)
                        continue

                    job_pickup_timestamp = datetime.utcnow()
                    object_luid = job_to_run[1]['OBJECT_LUID']
                    object_name = job_to_run[1]['OBJECT_NAME']
                    queue_position = job_to_run[0]
                    object_type = job_to_run[1]['OBJECT_TYPE']
                    active_job = f"[{object_name} ({object_luid})]"

                    if object_luid in executed_jobs:
                        logging.warning(f"{log_prefix}: {active_job} was already executed. Marking as hot and querying for a new task.")
                        #active_job_update(job_pickup_timestamp, gate_elapsed_time_in_seconds, object_luid)
                        continue

                    logging.info(f"{log_prefix}: {active_job} has been retrieved from the queue. Queue Position: {queue_position}")
                except Exception as e:
                    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
                    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
                        process_end_time = datetime.utcnow()
                        process_status = 'ERROR'
                        message = f"Error occurred in job_fetcher(): {str(e)}"
                        write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)
                    logging.error(f"{log_prefix}: Error occurred in fetching a job to run: {str(e)}")
                    continue

                # Wait for backgrounder bandwidth to be available

                # Submit the task to the executor
                try:
                    logging.info(f"{log_prefix}: Submitting {active_job} to the future runners!")
                    future = executor.submit(
                        tableau_future_runner, active_job, object_luid, object_name, object_type, job_pickup_timestamp,
                        gate_elapsed_time_pretty, execution_date, gate_elapsed_time_in_seconds
                    )
                    logging.info(f"{log_prefix}: Future Debug: {future}.")
                    active_futures.append(future)
                    # Mark the job as active
                    #active_job_update(job_pickup_timestamp, gate_elapsed_time_in_seconds, object_luid)
                    #logging.info(f"{log_prefix}: {active_job} marked as active in Snowflake.")
                    # Add it to list of executed jobs
                    executed_jobs.add(object_luid)
                    logging.info(f"{log_prefix}: {active_job} added to the list of executed jobs.")
                    #time_module.sleep(30)  # 30 sec delay before submitting the next task

                except Exception as e:
                    cloud_autoscheduler_edw_snowflake_prod_conn_info = get_db_connection('cloud_autoscheduler_edw_snowflake_prod')
                    with snowflake.connector.connect(**cloud_autoscheduler_edw_snowflake_prod_conn_info) as sf_conn:
                        process_end_time = datetime.utcnow()
                        process_status = 'ERROR'
                        message = f"Error occurred in tableau_future_runner(): {str(e)}"
                        write_to_execution_logs(process_name, process_id, process_status, process_start_time, process_end_time, message)

                    logging.error(f"{log_prefix}: Error occurred in submitting {active_job} to orchestrator: {str(e)}")
                    continue
            else:
                logging.warning(f"{log_prefix}: Reached maximum number of active futures. Waiting 2 minutes before checking again.")
                time_module.sleep(120)  # 2 min delay before checking the number of active futures again

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
"""
# delayer tasks
delay_in_minutes_task = PythonOperator(
    task_id='3_min_delay',
    python_callable=delayer,
    op_kwargs={'delay_minutes': 3},
    provide_context=True,
    retries=5,
    retry_delay=timedelta(minutes=10),
    dag=dag.airflow_dag,
)
delay_until_timestamp = PythonOperator(
    task_id='delay_until_15_UTC',
    python_callable=delayer,
    op_kwargs={'target_time': time(hour=15)},
    retries=5,
    retry_delay=timedelta(minutes=10),
    provide_context=True,
    dag=dag.airflow_dag,
)
"""
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
    dag=dag.airflow_dag,
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
    execution_timeout=timedelta(hours=23),
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

#todo -> point all db conn to prod, update max_futures to 7
