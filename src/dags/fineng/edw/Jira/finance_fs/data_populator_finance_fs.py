from datetime import date, timedelta, datetime, time, timezone
import logging
import random
import pandas as pd
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
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from ttd.ttdslack import dag_post_to_slack_callback, get_slack_client
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.base import TtdDag
import logging
from dags.fineng.edw.Jira.finance_fs.issues_finance_fs import populate_finance_fs

alarm_slack_channel = '#bizng-edw-oncall'  #'#bizng-edw-oncall'

default_args = {
    "owner": "FINENG",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 3),
    "catchup": False,
}

dag = TtdDag(
    dag_id="MAIN-EDW_Jira_Finance_FS_To_EDWStaging",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=timedelta(hours=23),
    max_active_runs=1,
    tags=["fineng", "edw", "jira", "finance_fs"],
    on_failure_callback=
    dag_post_to_slack_callback(dag_name="Jira finance_fs", slack_channel=alarm_slack_channel, step_name="parent dagrun"),
    run_only_latest=True,
)

adag = dag.airflow_dag


def fetch_secrets_and_connections(conn_name):
    if conn_name == "jira_username":
        return Variable.get("jira_username")
    elif conn_name == "jira_password":
        return Variable.get("jira_password")
    elif conn_name == "edw_snowflake_dev":
        conn = BaseHook.get_connection("edw_snowflake_dev")
        return {
            "user": conn.login,
            "password": conn.password,
            "account": conn.extra_dejson.get("account"),
            "warehouse": conn.extra_dejson.get("warehouse"),
            "role": conn.extra_dejson.get("role"),
            "database": conn.schema,
            "schema": conn.extra_dejson.get("schema"),
            "autocommit": True,
        }
    elif conn_name == "edw_snowflake_prod":
        conn = BaseHook.get_connection("edw_snowflake_prod")
        return {
            "user": conn.login,
            "password": conn.password,
            "account": conn.extra_dejson.get("account"),
            "warehouse": conn.extra_dejson.get("warehouse"),
            "role": conn.extra_dejson.get("role"),
            "database": conn.schema,
            "schema": conn.extra_dejson.get("schema"),
            "autocommit": True,
        }


def issues(**kwargs):
    dag_run = kwargs.get("dag_run")
    logging.info(dag_run)
    dag_run = kwargs.get("dag_run").conf.get('default_args', {})

    username = fetch_secrets_and_connections("jira_username")
    password = fetch_secrets_and_connections("jira_password")
    snowflake_conn = fetch_secrets_and_connections("edw_snowflake_prod")
    logging.info("Starting to populate issues ")
    populate_finance_fs(snowflake_conn, username, password)
    logging.info("Finished populating issues")
    return


LoadIssues = OpTask(op=PythonOperator(
    task_id="loadIssues",
    python_callable=issues,
    dag=adag,
))

dag >> LoadIssues
