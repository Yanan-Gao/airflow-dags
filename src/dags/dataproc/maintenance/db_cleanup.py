# Fork of the GoogleCloudPlatform sample DAG
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/HEAD/composer/workflows/airflow_db_cleanup.py

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
import logging

from airflow.utils import timezone
from airflow import settings
from airflow.jobs.job import Job
from airflow.models import (
    DAG,
    DagModel,
    DagRun,
    Log,
    SlaMiss,
    TaskInstance,
    XCom,
    TaskReschedule,
    TaskFail,
    ImportError,
)
from airflow.operators.python import PythonOperator

import dateutil.parser
from sqlalchemy import and_, func, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 183  # 6 months
PRINT_QUERIES = False
EXECUTE_DELETION = True

DATABASE_OBJECTS = [
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": True,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id,
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column": DagModel.last_parsed_time,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": TaskReschedule,
        "age_check_column": TaskReschedule.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": TaskFail,
        "age_check_column": TaskFail.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": ImportError,
        "age_check_column": ImportError.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
        "do_not_delete_by_dag_id": True,
    },
    {
        "airflow_db_model": Job,
        "age_check_column": Job.latest_heartbeat,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
]

default_args = {
    "owner": "DATAPROC",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(hours=1),
}

dag_name = "airflow-db-cleaning-job"

dag = DAG(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="47 12 * * *",
    start_date=datetime(2023, 6, 5),
    tags=["DATAPROC", "Maintenance"],
    catchup=False,
)


def calculate_max_db_entry_date(**context):
    max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_date = timezone.utcnow() + timedelta(-max_db_entry_age_in_days)

    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))

    logging.info("Setting max_execution_date to XCom for Downstream Processes")
    context["ti"].xcom_push(key="max_date", value=max_date.isoformat())


def build_query(
    session,
    airflow_db_model,
    age_check_column,
    max_date,
    keep_last,
    keep_last_filters=None,
    keep_last_group_by=None,
):
    query = session.query(airflow_db_model).options(load_only(age_check_column))

    logging.info("INITIAL QUERY : " + str(query))

    if not keep_last:
        query = query.filter(age_check_column <= max_date, )
    else:
        subquery = session.query(func.max(DagRun.execution_date))
        # workaround for MySQL "table specified twice" issue
        # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
        if keep_last_filters is not None:
            for entry in keep_last_filters:
                subquery = subquery.filter(entry)

            logging.info("SUB QUERY [keep_last_filters]: " + str(subquery))

        if keep_last_group_by is not None:
            subquery = subquery.group_by(keep_last_group_by)
            logging.info("SUB QUERY [keep_last_group_by]: " + str(subquery))

        subquery = subquery.from_self()

        query = query.filter(and_(age_check_column.notin_(subquery)), and_(age_check_column <= max_date))

    return query


def print_query(query, airflow_db_model, age_check_column):
    entries_to_delete = query.all()

    logging.info("Query: " + str(query))
    logging.info("Process will be Deleting the following " + str(airflow_db_model.__name__) + "(s):")
    for entry in entries_to_delete:
        date = str(entry.__dict__[str(age_check_column).split(".")[1]])
        logging.info("\tEntry: " + str(entry) + ", Date: " + date)

    logging.info("Process will be Deleting " + str(len(entries_to_delete)) + " " + str(airflow_db_model.__name__) + "(s)")


def cleanup_for_db_object(**context):
    session = settings.Session()

    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(task_ids=calculate_max_db_entry_date_task.task_id, key="max_date")
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    airflow_db_model = context["params"].get("airflow_db_model")
    state = context["params"].get("state")
    age_check_column = context["params"].get("age_check_column")
    keep_last = context["params"].get("keep_last")
    keep_last_filters = context["params"].get("keep_last_filters")
    keep_last_group_by = context["params"].get("keep_last_group_by")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(EXECUTE_DELETION))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("state:                    " + str(state))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("keep_last:                " + str(keep_last))
    logging.info("keep_last_filters:        " + str(keep_last_filters))
    logging.info("keep_last_group_by:       " + str(keep_last_group_by))

    logging.info("")

    logging.info("Running Cleanup Process...")

    try:
        if context["params"].get("do_not_delete_by_dag_id"):
            query = build_query(
                session,
                airflow_db_model,
                age_check_column,
                max_date,
                keep_last,
                keep_last_filters,
                keep_last_group_by,
            )
            if PRINT_QUERIES:
                print_query(query, airflow_db_model, age_check_column)
            if EXECUTE_DELETION:
                logging.info("Performing Delete...")
                query.delete(synchronize_session=False)
            session.commit()
        else:
            dags = session.query(airflow_db_model.dag_id).distinct()
            session.commit()

            list_dags = [str(list(dag)[0]) for dag in dags] + [None]
            for dag in list_dags:
                query = build_query(
                    session,
                    airflow_db_model,
                    age_check_column,
                    max_date,
                    keep_last,
                    keep_last_filters,
                    keep_last_group_by,
                )
                query = query.filter(airflow_db_model.dag_id == dag)
                if PRINT_QUERIES:
                    print_query(query, airflow_db_model, age_check_column)
                if EXECUTE_DELETION:
                    logging.info("Performing Delete...")
                    query.delete(synchronize_session=False)
                session.commit()

        if not EXECUTE_DELETION:
            logging.warning("You've opted to skip deleting the db entries. "
                            "Set EXECUTE_DELETION to True to delete entries!!!")

        logging.info("Finished Running Cleanup Process")

    except ProgrammingError as e:
        logging.error(e)
        logging.error(str(airflow_db_model) + " is not present in the metadata. "
                      "Skipping...")

    finally:
        session.close()


def cleanup_sessions():
    session = settings.Session()

    try:
        logging.info("Deleting sessions...")
        before = len(session.execute(text("SELECT * FROM session WHERE expiry < now()::timestamp(0);")).mappings().all())
        session.execute(text("DELETE FROM session WHERE expiry < now()::timestamp(0);"))
        after = len(session.execute(text("SELECT * FROM session WHERE expiry < now()::timestamp(0);")).mappings().all())
        logging.info("Deleted {} expired sessions.".format(before - after))
    except Exception as e:
        logging.error(e)

    session.commit()
    session.close()


def analyze_db():
    session = settings.Session()
    session.execute("ANALYZE")
    session.commit()
    session.close()


analyze_op = PythonOperator(
    task_id="analyze_query",
    python_callable=analyze_db,
    provide_context=True,
    dag=dag,
)

cleanup_session_op = PythonOperator(
    task_id="cleanup_sessions",
    python_callable=cleanup_sessions,
    provide_context=True,
    dag=dag,
)

cleanup_session_op.set_downstream(analyze_op)

calculate_max_db_entry_date_task = PythonOperator(
    task_id="calculate_max_db_entry_date",
    python_callable=calculate_max_db_entry_date,
    provide_context=True,
    dag=dag,
)

for db_object in DATABASE_OBJECTS:
    cleanup_op = PythonOperator(
        task_id="cleanup_" + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_for_db_object,
        params=db_object,
        provide_context=True,
        dag=dag,
    )

    calculate_max_db_entry_date_task.set_downstream(cleanup_op)
    cleanup_op.set_downstream(analyze_op)
