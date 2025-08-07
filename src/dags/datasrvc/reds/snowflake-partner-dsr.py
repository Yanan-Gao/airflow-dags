from datetime import datetime, timedelta
from enum import Enum
import logging
from typing import Dict, Set

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from prometheus_client.metrics import Gauge

from dags.datasrvc.utils.common import is_prod
from ttd.el_dorado.v2.base import TtdDag
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdslack import dag_post_to_slack_callback
from ttd.ttdprometheus import get_or_register_gauge, push_all
from ttd.tasks.op import OpTask

__doc__ = """
This Airflow process involves loading the partner DSR from S3 and scrubbing the sensitive data,
including IDs and PII, from REDS data in Snowflake across multiple regions.
"""

logging_only = False

dag_name = "snowflake_partner_dsr_for_REDS"
execution_interval = timedelta(days=1)

snowflake_conn_id_default = "snowflake"
snowflake_warehouse_default = "PARTNER_DSR_WH"
snowflake_database_default = "THETRADEDESK"
snowflake_reds_schema_default = "REDS"
snowflake_reds_backup_schema_default = "REDS_DSR_BACKUP"

alarm_slack_datasrvc_channel = "#scrum-data-services-alarms"
max_dsr_request_seq_id_xcom_key = "max_dsr_request_seq_id"

_ensure_flow_task_id_format_string = "ensure_flow_{}"
_update_request_task_id_format_string = 'update_request_{}'
_cleanup_dsr_data_task_id_string_format = "cleanup_dsr_data_{}"
_ensure_dsr_schema_and_column_task_id_string_format = "{}_ensure_dsr_schema_and_column"
_calc_push_execution_stats_task_id_string_format = "calc_push_execution_stats_{}"
_region_table_task_id_string_format = "{}_process_request_{}"
_load_dsr_task_id_string_format = "load_request_into_snowflake_{}"

default_args = {
    "owner": "datasrvc",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

primary_database_name = "THETRADEDESK"
primary_database_param = {
    "database": primary_database_name,
}

testing = not is_prod()


def choose(prod, test):
    return test if testing else prod


env_prefix = choose(prod='prod', test='test')

dag = TtdDag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval=execution_interval,
    tags=["DATASRVC", "REDS", "DSR"],
    on_failure_callback=dag_post_to_slack_callback(
        dag_name=dag_name,
        step_name="parent dagrun",
        slack_channel=alarm_slack_datasrvc_channel,
    ),
    max_active_runs=2,
    start_date=datetime(2025, 6, 1),
    run_only_latest=False
)

adag = dag.airflow_dag

connection_params = [
    # {
    # "snowflake_conn_id" :  snowflake_conn_id_default,
    # "database": snowflake_database_default,
    # "reds_schema": snowflake_reds_schema_default,
    # },
    {
        "database": "REDS_AWS_AP_SOUTHEAST_2",
    },
    {
        "database": "REDS_AWS_EU_WEST_1",
    },
    {
        "database": "REDS_AWS_US_EAST_1",
    },
    {
        "database": "REDS_AZURE_EASTUS2",
    },
    {
        "database": "REDS_AZURE_WESTUS2",
    },
    {
        "database": "REDS_GCP_US_CENTRAL1",
    },
    primary_database_param
]

reds_tables = [
    {
        "table_name": "BIDFEEDBACK",
        "table_abbr": "bf"
    },
    {
        "table_name": "CONVERSIONTRACKER",
        "table_abbr": "con"
    },
    {
        "table_name": "CLICKTRACKER",
        "table_abbr": "click"
    },
    {
        "table_name": "VIDEOEVENT",
        "table_abbr": "v"
    },
]

dsr_config = {
    "reds_tables": reds_tables,
    "partner_dsr_request_scrubbing_table": {
        "table_name": "partner_dsr_request_scrubbing",
        "table_abbr": "dsr"
    },
    "dsr_scrubbed_datetime_column_name": "DSRSCRUBBEDDATE",
    "dsr_scrubbed_flagging_column_name": "DSRFLAGGING",
    "is_flag_rows_only": True,
    "partner_dsr_requests_s3_bucket": f"s3://ttd-data-subject-requests/env={env_prefix}/partnerdsr/enriched/",
    "dsr_request_data_retention_days": 30,
    "scrubbed_data_backup_retention_days": 14,
    "partner_dsr_stage": f"partner_dsr_stage_{env_prefix}",
}


class FlowStep(Enum):
    Prep = 1
    Column = 2
    Load = 3
    Process = 4
    BranchCheck = 5
    Finalize = 6
    CleanUp = 7
    Stats = 8


def get_snowflake_connection(connection_param, op_name, run_id):
    conn_id = connection_param.get("snowflake_conn_id", snowflake_conn_id_default)
    warehouse = connection_param.get("warehouse", snowflake_warehouse_default)
    database = connection_param.get("database", snowflake_database_default)
    reds_schema = connection_param.get("reds_schema", snowflake_reds_schema_default)
    tag = f"DSR;{op_name.name};{run_id}"

    sf_hook = SnowflakeHook(snowflake_conn_id=conn_id, warehouse=warehouse, database=database, schema=reds_schema)
    conn = sf_hook.get_conn()
    execute_sql_statements(conn, [f"ALTER SESSION SET QUERY_TAG = '{tag}'"])

    return conn


def execute_sql_statements(connection, sql_statements):
    with connection.cursor() as cursor:
        try:
            for sql_statement in sql_statements:
                logging.info(f"Executing SQL statement: \n\ndb={connection.database}\n{sql_statement}\n")
                if not logging_only:
                    cursor.execute(sql_statement)
        except Exception as e:
            logging.info(f"\nAn error occurred: {e}")
            raise


def ensure_dsr_scrubbed_column(connection, table_schema, table_name, column_name, data_type):
    sql_statement = (
        f"SELECT EXISTS (\n"
        f"    SELECT 1\n"
        f"    FROM INFORMATION_SCHEMA.COLUMNS\n"
        f"    WHERE TABLE_SCHEMA = '{table_schema}'\n"
        f"    AND TABLE_NAME = '{table_name}'\n"
        f"    AND COLUMN_NAME = '{column_name}'\n"
        f") AS ColumnExists;\n"
    )

    column_exists = execute_sql_statement_with_return(connection, sql_statement)
    logging.info(f"\ncolumn_exists from db: {column_exists}")

    if len(column_exists) > 0 and len(column_exists[0]) > 0:
        logging.info(f"The {table_name}.{table_schema}.{column_name} already exists.")
    else:
        sql_statement = (f"ALTER TABLE {table_schema}.{table_name}\n"
                         f"ADD COLUMN {column_name} {data_type};\n")

        execute_sql_statements(connection, [sql_statement])


def ensure_partner_dsr_request_flow(connection_param, dsr_config, **kwargs):
    set_connection_param_default_values(connection_param)

    logging.info(f"ensuring partner dsr request flow for {connection_param['database']}.")

    connection = get_snowflake_connection(connection_param, FlowStep.Prep, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)

    s3_bucket_url = dsr_config.get("partner_dsr_requests_s3_bucket")
    partner_dsr_stage = dsr_config.get("partner_dsr_stage")
    sql_statements = [
        (
            f"CREATE TABLE IF NOT EXISTS {schema_name}.partner_dsr_request (\n"
            f"    RequestId VARCHAR(36),\n"
            f"    RequestTimeStamp VARCHAR(36),\n"
            f"    AdvertiserId VARCHAR(36),\n"
            f"    PartnerId VARCHAR(36),\n"
            f"    TenantId VARCHAR(36),\n"
            f"    UserIdGuid  VARCHAR(50),\n"
            f"    UserId VARCHAR(50),\n"
            f"    UserIdType VARCHAR(50)\n"
            f");\n"
        ),
        (f"CREATE SEQUENCE IF NOT EXISTS {schema_name}.partner_dsr_sequence\n"
         f"  START WITH 1\n"
         f"  INCREMENT BY 1;\n"),
        (
            f"CREATE TABLE IF NOT EXISTS {schema_name}.partner_dsr_request_scrubbing (\n"
            f"    id INT DEFAULT {schema_name}.partner_dsr_sequence.NEXTVAL,\n"
            f"    RequestId VARCHAR(36),\n"
            f"    RequestTimeStamp VARCHAR(36),\n"
            f"    AdvertiserId VARCHAR(36),\n"
            f"    PartnerId VARCHAR(36),\n"
            f"    TenantId VARCHAR(36),\n"
            f"    UserIdGuid  VARCHAR(50),\n"
            f"    UserId VARCHAR(50),\n"
            f"    UserIdType VARCHAR(50),\n"
            f"    ProcessedTimeStamp TIMESTAMP_NTZ\n"
            f");\n"
        ),
        # (
        #     # This command requires the AccountAdmin role to run, included here for documentation purposes
        #     # The  STORAGE INTEGRATION was already created via UI
        #     f"CREATE STORAGE INTEGRATION IF NOT EXISTS S3_PARTNER_DSR_READ_INTEGRATION\n"
        #     f"TYPE = EXTERNAL_STAGE\n"
        #     f"STORAGE_PROVIDER = 'S3'\n"
        #     f"ENABLED = TRUE\n"
        #     f"STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::003576902480:role/snowflake_partner_dsr_read'\n"
        #     f"STORAGE_ALLOWED_LOCATIONS = ('{s3_bucket_url}')\n"
        #     f"STORAGE_AWS_EXTERNAL_ID = 'TRADEDESK_SFCRole=2_LFB4JCeEt6p4rGcm4bcQ+EZMxEI=';\n"
        # ),
        (
            f"CREATE FILE FORMAT IF NOT EXISTS {schema_name}.partner_dsr_file_format\n"
            f"   TYPE = 'CSV'\n"
            f"   FIELD_DELIMITER = ','\n"
            f"   SKIP_HEADER = 1;\n"
        ),
        (
            f"CREATE STAGE IF NOT EXISTS {schema_name}.{partner_dsr_stage}\n"
            f"    URL = '{s3_bucket_url}'\n"
            f"    FILE_FORMAT = {schema_name}.partner_dsr_file_format\n"
            f"    STORAGE_INTEGRATION = S3_PARTNER_DSR_READ_INTEGRATION;\n"
        )
    ]

    execute_sql_statements(connection, sql_statements)


def copy_partner_dsr_to_stage(connection, schema_name, pattern) -> None:
    partner_dsr_stage = dsr_config.get("partner_dsr_stage")
    sql_statement = (
        f"COPY INTO {schema_name}.partner_dsr_request\n"
        f"FROM @{schema_name}.{partner_dsr_stage}\n"
        f"PATTERN='{pattern}'\n"
        f"ON_ERROR = 'skip_file';\n"
    )

    execute_sql_statements(connection, [sql_statement])


def load_partner_dsr_request_to_scrubbing(connection, schema_name) -> None:
    sql_statement = (
        f"INSERT INTO {schema_name}.partner_dsr_request_scrubbing (\n"
        f"    RequestId, RequestTimeStamp, AdvertiserId, PartnerId, TenantId,\n"
        f"    UserIdGuid, UserId, UserIdType, ProcessedTimeStamp)\n"
        f"SELECT\n"
        f"    latest_rows.RequestId,\n"
        f"    latest_rows.RequestTimeStamp,\n"
        f"    latest_rows.AdvertiserId,\n"
        f"    latest_rows.PartnerId,\n"
        f"    latest_rows.TenantId,\n"
        f"    latest_rows.UserIdGuid,\n"
        f"    latest_rows.UserId, \n"
        f"    latest_rows.UserIdType,\n"
        f"    NULL AS ProcessedTimeStamp\n"
        f"FROM {schema_name}.partner_dsr_request latest_rows\n"
        f"LEFT JOIN {schema_name}.partner_dsr_request_scrubbing AS existing_rows\n"
        f"ON latest_rows.RequestId = existing_rows.RequestId\n"
        f"AND latest_rows.REQUESTTIMESTAMP = existing_rows.REQUESTTIMESTAMP\n"
        f"AND latest_rows.PartnerId = existing_rows.PartnerId\n"
        f"AND latest_rows.UserIdGuid IS NOT DISTINCT FROM existing_rows.UserIdGuid\n"
        f"AND latest_rows.UserId IS NOT DISTINCT FROM existing_rows.UserId\n"
        f"AND latest_rows.UserIdType = existing_rows.UserIdType\n"
        f"WHERE existing_rows.RequestId IS NULL;\n"
    )

    execute_sql_statements(connection, [sql_statement])


def load_partner_dsr_request_into_snowflake_primary_db(connection_param, **kwargs):
    logging.info(f"loading partner dsr from s3 to snowflake table for {connection_param['database']}.")

    connection = get_snowflake_connection(connection_param, FlowStep.Load, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)

    copy_partner_dsr_to_stage(connection, schema_name, ".*advertiser_data_subject_requests.csv")
    load_partner_dsr_request_to_scrubbing(connection, schema_name)

    max_dsr_request_seq_id = get_max_dsr_request_seq_id(connection, schema_name)

    kwargs["ti"].xcom_push(key=max_dsr_request_seq_id_xcom_key, value=max_dsr_request_seq_id)


def ensure_dsr_schema_and_column(connection_param, dsr_config, **kwargs):
    connection = get_snowflake_connection(connection_param, FlowStep.Column, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)
    backup_schema_name = connection_param.get("reds_backup_schema", snowflake_reds_backup_schema_default)
    dsr_scrubbed_datetime_column_name = dsr_config["dsr_scrubbed_datetime_column_name"]
    dsr_scrubbed_flagging_column_name = dsr_config["dsr_scrubbed_flagging_column_name"]

    ensure_reds_backup_schema(connection, backup_schema_name)

    reds_tables = dsr_config["reds_tables"]
    for reds_table in reds_tables:
        table_name = reds_table["table_name"]
        ensure_dsr_scrubbed_column(connection, schema_name, table_name, dsr_scrubbed_datetime_column_name, "TIMESTAMP_NTZ")
        ensure_dsr_scrubbed_column(connection, schema_name, table_name, dsr_scrubbed_flagging_column_name, " VARCHAR(100)")


def ensure_reds_backup_schema(connection, schema_name):
    sql_statements = [(f"CREATE SCHEMA IF NOT EXISTS {schema_name};\n")]
    execute_sql_statements(connection, sql_statements)


def execute_sql_statement_with_return(connection, sql_statement):
    result = []
    with connection.cursor() as cursor:
        try:
            logging.info(f"Executing SQL statement: \n\ndb={connection.database}\n{sql_statement}\n")
            if not logging_only:
                cursor.execute(sql_statement)
                result = cursor.fetchall()
        except Exception as e:
            logging.info(f"An error occurred: {e}")
            raise

    return result


def get_max_dsr_request_seq_id(connection, schema_name):
    sql_statement = (f"SELECT max(ID)\n"
                     f"FROM {schema_name}.partner_dsr_request_scrubbing;\n")

    max_dsr_request_seq_id = execute_sql_statement_with_return(connection, sql_statement)
    logging.info(f"\nmax_dsr_request_seq_id from db: {max_dsr_request_seq_id}\n")

    if len(max_dsr_request_seq_id) == 0:
        logging.info('Nothing returned for max_dsr_request_seq_id')
        return 1
    max_dsr_request_seq_id = max_dsr_request_seq_id[0][0]
    max_dsr_request_seq_id = 1 if max_dsr_request_seq_id is None else max_dsr_request_seq_id
    logging.info(f"\nmax_dsr_request_seq_id for downstream: {max_dsr_request_seq_id}\n")

    return max_dsr_request_seq_id


def get_last_scrub_date(connection, schema_name):
    sql_statement = (f"SELECT max(StartDate)\n"
                     f"FROM {schema_name}.DataScrubbingLog\n"
                     f"WHERE LogAction = 'DataScrub';\n")

    last_scrub_date = execute_sql_statement_with_return(connection, sql_statement)
    logging.info(f"\nlast_scrub_date from db: {last_scrub_date}\n")

    if last_scrub_date and last_scrub_date[0] is not None and last_scrub_date[0][0] is not None:
        last_scrub_date = last_scrub_date[0][0]
    else:
        logging.info('Nothing returned for last_scrub_date')
        return datetime(2023, 1, 1)

    last_scrub_date = datetime(2023, 1, 1) if last_scrub_date is None else last_scrub_date
    logging.info(f"\nlast_scrub_date: {last_scrub_date}\n")

    return last_scrub_date


def retrieve_table_column_names(connection, schema_name, table_name) -> Set[str]:
    sql_statement = (
        f"SELECT column_name\n"
        f"FROM information_schema.columns\n"
        f"WHERE table_schema ILIKE '{schema_name}'\n"
        f"AND table_name ILIKE '{table_name}'\n"
        f"ORDER BY ordinal_position;\n"
    )

    rows = execute_sql_statement_with_return(connection, sql_statement)
    logging.info(f"retrieve_table_column_names, rows from db: \n{rows}\n")

    column_set = {row[0] for row in rows}
    logging.info(f"retrieve_table_column_names, set of rows: \n{column_set}\n")

    return column_set


def generate_id_match_filter_sql(table_columns_to_identify_records, table_abbr, dsr_table_abbr):
    dsr_table_id_column_name = "USERID"
    table_columns_to_identify_records_clauses = [
        f"   {table_abbr}.{field} = {dsr_table_abbr}.{dsr_table_id_column_name}" for field in table_columns_to_identify_records
    ]
    result = " OR \n".join(sorted(table_columns_to_identify_records_clauses))

    return result


def generate_scrub_flagged_row_filter_sql():
    dsr_scrubbed_flagging_column_name = dsr_config["dsr_scrubbed_flagging_column_name"]
    to_be_scrubbed_row_filter = (f" {dsr_scrubbed_flagging_column_name} is NOT NULL\n"
                                 f"AND DSRSCRUBBEDDATE IS NULL")

    return to_be_scrubbed_row_filter


def get_all_fields_to_identify_records() -> Dict[str, str]:
    id_fields: Dict[str, str] = {
        # Supported ID Types from https://partner.thetradedesk.com/v3/portal/data/doc/post-data-advertiser-firstparty
        "TDID": "''",
        "DAID": "''",
        "OTHERID": "''",
        "UID2": "''",
        "UID2Token": "''",
        "EUID": "''",
        "EUIDToken": "''",
        "IDL": "''",
        "ID5": "''",
        "netID": "''",
        "FirstId": "''",

        # addtional fields from log forma:
        "COMBINEDIDENTIFIER": "''",
        "DEVICEADVERTISINGID": "''",
        "IdentityLinkID": "''",
        "ORIGINALID": "''",
        "ORIGINATINGID": "''",
    }

    return id_fields


def get_additional_pii_fields() -> Dict[str, str]:
    # https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?pageId=231183996
    scrubbing_functions: Dict[str, str] = {
        "AttributedEventTDID": "''",
        "BrowserTDID": "''",
        "CoreID": "''",
        "DatId": "''",
        "DeviceId": "''",
        "DeviceAdvertisingId": "''",
        "EUID": "''",
        "HashedIpAsUiid": "''",
        "HawkId": "''",
        "HHID": "''",
        "IdentityLinkID": "''",
        "IDL": "''",
        "IPAddress": "''",
        "Latitude": "0",
        "Longitude": "0",
        "OriginalId": "''",
        "OriginatingId": "''",
        "OtherId": "''",
        "PersonId": "''",
        "TDID": "''",
        "UID2": "''",
        "Zip": "''",
    }

    return scrubbing_functions


def generate_update_fields_with_variant(base_fields_dict):
    fields_with_variant = {
        f"{key}{suffix}".upper(): value.upper()
        for key, value in base_fields_dict.items()
        for suffix in ["", "Internal", "External"]
    }

    return fields_with_variant


def generate_filter_fields_with_variant(base_fields: Dict[str, str]) -> Set[str]:
    fields_with_variant: Set[str] = {f"{field}{suffix}".upper() for field in base_fields for suffix in ["", "Internal", "External"]}

    return fields_with_variant


def get_table_pii_columns_with_scrubbing(column_names):
    all_fields_to_identify_records = get_all_fields_to_identify_records()
    additional_pii_fields = get_additional_pii_fields()
    all_pii_fields = {**all_fields_to_identify_records, **additional_pii_fields}
    fields_with_variant = generate_update_fields_with_variant(all_pii_fields)

    table_columns_with_scrubbing = {key: fields_with_variant[key] for key in column_names if key in fields_with_variant}

    return table_columns_with_scrubbing


def get_table_columns_to_identify_records(table_column_names):
    all_fields_to_identify_records = get_all_fields_to_identify_records()
    fields_to_identify_records_with_variant = generate_filter_fields_with_variant(all_fields_to_identify_records)

    table_columns_to_identify_records = table_column_names.intersection(fields_to_identify_records_with_variant)

    return table_columns_to_identify_records


def generate_dsr_flagging_sql(schema_name, table_name, table_abbr, set_pii_columns_with_scrubbing, last_scrub_date, id_match_filter_sql):
    set_id_fields_sql = ", \n   ".join(f"{key} = {value}" for key, value in sorted(set_pii_columns_with_scrubbing.items()))
    all_user_ids_sql = (
        f"    SELECT ADVERTISERID, UserId, ProcessedTimeStamp\n"
        f"    FROM {primary_database_name}.{schema_name}.PARTNER_DSR_REQUEST_SCRUBBING\n"
        f"    WHERE UserId IS NOT NULL\n"
        f"    AND PROCESSEDTIMESTAMP IS NULL\n"
        f"\n"
        f"    UNION DISTINCT\n"
        f"\n"
        f"    SELECT ADVERTISERID, UserIdGuid, ProcessedTimeStamp\n"
        f"    FROM {primary_database_name}.{schema_name}.PARTNER_DSR_REQUEST_SCRUBBING\n"
        f"    WHERE UserIdGuid IS NOT NULL\n"
        f"    AND PROCESSEDTIMESTAMP IS NULL\n"
    )

    last_scrub_date = f"{last_scrub_date.strftime('%Y-%m-%d %H:%M:%S')}"
    dsr_flagging_sql = (
        f"UPDATE {schema_name}.{table_name} {table_abbr}\n"
        f"SET {set_id_fields_sql}\n"
        f"FROM (\n{all_user_ids_sql}) dsr\n"
        f"WHERE {table_abbr}.LOGENTRYTIME > '{last_scrub_date}'\n"
        f"AND {table_abbr}.ADVERTISERID = dsr.ADVERTISERID\n"
        f"AND (\n"
        f"{id_match_filter_sql}\n"
        f");\n"
    )

    return dsr_flagging_sql


def generate_dsr_scrubbing_sql(schema_name, table_name, table_abbr, set_pii_columns_with_scrubbing, last_scrub_date, id_match_filter_sql):
    set_id_fields_sql = ", \n   ".join(f"{key} = {value}" for key, value in sorted(set_pii_columns_with_scrubbing.items()))

    dsr_scrubbing_sql = (
        f"UPDATE {schema_name}.{table_name} {table_abbr}\n"
        f"SET {set_id_fields_sql}\n"
        f"WHERE "
        f"{id_match_filter_sql};\n"
    )

    return dsr_scrubbing_sql


def generate_create_backup_table_sql(schema_name, backup_schema, table_name, run_id):
    backup_table_full_name = f"{backup_schema}.\"{table_name}_DSR_BACKUP_{run_id}\""
    flagged_row_filter_sql = generate_scrub_flagged_row_filter_sql()

    create_backup_table_sql = (
        f"CREATE TABLE IF NOT EXISTS {backup_table_full_name} AS\n"
        f"SELECT *\n"
        f"FROM {schema_name}.{table_name}\n"
        f"WHERE "
        f"{flagged_row_filter_sql};\n"
    )

    return create_backup_table_sql


def generate_dsr_flagging_and_scrubbing_statement(connection, dsr_config, schema_name, reds_table, last_scrub_date, run_id):
    dsr_table_abbr = dsr_config["partner_dsr_request_scrubbing_table"]["table_abbr"]
    dsr_flagging_sql = ""
    dsr_scrubbing_sql = ""

    table_name = reds_table["table_name"]
    table_abbr = reds_table["table_abbr"]

    table_column_names = retrieve_table_column_names(connection, schema_name, table_name)

    table_columns_to_identify_records = get_table_columns_to_identify_records(table_column_names)
    if not table_columns_to_identify_records:
        logging.warning("No table columns for identifying records.")
    else:
        id_match_filter_sql = generate_id_match_filter_sql(table_columns_to_identify_records, table_abbr, dsr_table_abbr)

        set_pii_columns_with_scrubbing = {dsr_config["dsr_scrubbed_flagging_column_name"]: f"'{run_id}'"}

        dsr_flagging_sql = generate_dsr_flagging_sql(
            schema_name, table_name, table_abbr, set_pii_columns_with_scrubbing, last_scrub_date, id_match_filter_sql
        )
        logging.info(f"dsr_flagging_sql: \n{dsr_flagging_sql}\n")

        flagged_row_filter_sql = generate_scrub_flagged_row_filter_sql()

        set_pii_columns_with_scrubbing = {dsr_config["dsr_scrubbed_datetime_column_name"]: "CURRENT_TIMESTAMP()"}

        table_pii_columns_with_scrubbing = get_table_pii_columns_with_scrubbing(table_column_names)
        set_pii_columns_with_scrubbing.update(table_pii_columns_with_scrubbing)

        dsr_scrubbing_sql = generate_dsr_scrubbing_sql(
            schema_name, table_name, table_abbr, set_pii_columns_with_scrubbing, last_scrub_date, flagged_row_filter_sql
        )

        logging.info(f"dsr_scrubbing_sql: \n{dsr_scrubbing_sql}\n")

    return dsr_flagging_sql, dsr_scrubbing_sql


def process_partner_dsr_request_for_region(connection_param, dsr_config, reds_table, **kwargs):
    logging.info(f"processing partner dsr request flow for {connection_param['database']}_for {reds_table}.")

    run_id = kwargs['run_id']
    is_flag_rows_only = dsr_config["is_flag_rows_only"]

    connection = get_snowflake_connection(connection_param, FlowStep.Process, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)
    backup_schema_name = connection_param.get("reds_backup_schema", snowflake_reds_backup_schema_default)

    last_scrub_date = get_last_scrub_date(connection, schema_name)
    logging.info(f"last_scrub_date: {last_scrub_date}")

    dsr_flagging_sql, dsr_scrubbing_sql = generate_dsr_flagging_and_scrubbing_statement(
        connection, dsr_config, schema_name, reds_table, last_scrub_date, run_id
    )

    # Always flag rows for DSR as long as there are PII columns available.
    if dsr_flagging_sql:
        execute_sql_statements(connection, [dsr_flagging_sql])

        table_name = reds_table["table_name"]
        backup_table_sql = generate_create_backup_table_sql(schema_name, backup_schema_name, table_name, run_id)
        execute_sql_statements(connection, [backup_table_sql])

    # Only scrub the data if it is not in flag-only mode and there are PII columns available.
    if not is_flag_rows_only and dsr_scrubbing_sql:
        execute_sql_statements(connection, [dsr_scrubbing_sql])


def update_request(connection_param, load_dsr_task_id, **kwargs):
    max_dsr_request_seq_id = kwargs["ti"].xcom_pull(task_ids=load_dsr_task_id, key=max_dsr_request_seq_id_xcom_key)
    logging.info(f"\nmax_dsr_request_seq_id from upstream: {max_dsr_request_seq_id}\n")

    logging.info(f"updating scrubbed dsr request table for {connection_param['database']} till max seq {max_dsr_request_seq_id}.")

    connection = get_snowflake_connection(connection_param, FlowStep.Finalize, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)

    logging.info("Updating scrubbed dsr requests....\n")
    update_scrubbed_dsr_request(connection, max_dsr_request_seq_id, schema_name)


def drop_dsr_backup_tables(connection, backup_schema_name, scrubbed_data_backup_retention_days):
    # -- Dynamically identifies and drops backup tables beyond the retention from a specified schema.
    # -- It uses a cursor to fetch eligible tables, sets their data retention to zero, and then drops each table.
    drop_dsr_backup_table_sql = (
        f"DECLARE\n"
        f"  table_full_name STRING;\n"
        f"  sql_command STRING;\n"
        f"  logging STRING DEFAULT '';\n"
        f"  num_of_tables_dropped INT DEFAULT 0;\n"
        f"  \n"
        f"  tables_cursor CURSOR FOR\n"
        f"    SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME\n"
        f"    FROM INFORMATION_SCHEMA.TABLES\n"
        f"    WHERE TABLE_SCHEMA = '{backup_schema_name}'\n"
        f"      AND TABLE_TYPE = 'BASE TABLE'\n"
        f"      AND TABLE_NAME LIKE '%_DSR_BACKUP_%'\n"
        f"      AND CREATED <= DATEADD(Days, -{scrubbed_data_backup_retention_days}, CURRENT_TIMESTAMP());\n"
        f"BEGIN\n"
        f"  FOR rec IN tables_cursor DO\n"
        f"    table_full_name := rec.TABLE_CATALOG || '\".\"' || rec.TABLE_SCHEMA || '\".\"' || rec.TABLE_NAME;\n"
        f"    sql_command := 'ALTER TABLE \"' || table_full_name || '\" SET DATA_RETENTION_TIME_IN_DAYS = 0;';\n"
        f"    EXECUTE IMMEDIATE sql_command;\n"
        f"    \n"
        f"    sql_command := 'DROP TABLE \"' || table_full_name || '\";';\n"
        f"    EXECUTE IMMEDIATE sql_command;\n"
        f"    \n"
        f"    logging := logging || num_of_tables_dropped || ':' ||table_full_name || CHR(10);\n"
        f"    num_of_tables_dropped := num_of_tables_dropped + 1;\n"
        f"  END FOR;\n"
        f"  \n"
        f"  RETURN 'num_of_tables_dropped:' || num_of_tables_dropped || CHR(10) || logging;\n"
        f"END;\n"
    )

    result = execute_sql_statement_with_return(connection, drop_dsr_backup_table_sql)
    logging.info(f"\n{result}\n")


def delete_dsr_old_request_data(connection, schema_name, dsr_request_data_retention_days):
    # Delete all rows from table, partner_dsr_reques.
    # The metadata about which files have been loaded is not changed.
    # This means that the same files will not be reloaded automatically.
    #
    # Note: 'TRUNCATE TABLE REDS.partner_dsr_request;' does remove data and metadata.
    sql_statements = [(f"DELETE FROM {schema_name}.partner_dsr_request;"),
                      (
                          f"DELETE FROM {schema_name}.partner_dsr_request_scrubbing\n"
                          f"WHERE PROCESSEDTIMESTAMP <= DATEADD(Days, -{dsr_request_data_retention_days}, CURRENT_TIMESTAMP());\n"
                      )]
    execute_sql_statements(connection, sql_statements)


def cleanup_dsr_data(connection_param, **kwargs):
    logging.info(f"Cleanup dsr request data  for {connection_param['database']}.")

    connection = get_snowflake_connection(connection_param, FlowStep.CleanUp, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)
    backup_schema_name = connection_param.get("reds_backup_schema", snowflake_reds_backup_schema_default)

    logging.info("Deleting dsr old request data....\n")
    dsr_request_data_retention_days = dsr_config.get("dsr_request_data_retention_days")
    delete_dsr_old_request_data(connection, schema_name, dsr_request_data_retention_days)

    logging.info("Dropping dsr backup tables....\n")
    scrubbed_data_backup_retention_days = dsr_config.get("scrubbed_data_backup_retention_days")
    drop_dsr_backup_tables(connection, backup_schema_name, scrubbed_data_backup_retention_days)


def set_connection_param_default_values(connection_param):
    connection_param.setdefault("snowflake_conn_id", snowflake_conn_id_default)
    connection_param.setdefault("warehouse", snowflake_warehouse_default)
    connection_param.setdefault("database", snowflake_database_default)
    connection_param.setdefault("reds_schema", snowflake_reds_schema_default)
    connection_param.setdefault("reds_backup_schema", snowflake_reds_backup_schema_default)


def branch_condition_check(connection_param, **kwargs):
    logging.info(f"branch condition check for {connection_param['database']}.")

    connection = get_snowflake_connection(connection_param, FlowStep.BranchCheck, kwargs['run_id'])
    schema_name = connection_param.get("reds_schema", snowflake_reds_schema_default)

    sql_statement = (f"SELECT COUNT(1)\n"
                     f"FROM {schema_name}.partner_dsr_request_scrubbing\n"
                     f"WHERE PROCESSEDTIMESTAMP IS NULL;\n")

    num_of_requests_not_processed = execute_sql_statement_with_return(connection, sql_statement)
    logging.info(f"\nnum_of_requests_not_processed from db: {num_of_requests_not_processed}\n")

    if len(num_of_requests_not_processed) != 0:
        num_of_requests_not_processed = num_of_requests_not_processed[0][0]
        logging.info(f"\nnum_of_requests_not_processed for downstream: {num_of_requests_not_processed}\n")
    else:
        num_of_requests_not_processed = 0

    push_requests_not_processed_metrics(num_of_requests_not_processed)

    cleanup_task_id = _cleanup_dsr_data_task_id_string_format.format(primary_database_param['database'])
    db_task_list = [
        _ensure_dsr_schema_and_column_task_id_string_format.format(connection_param['database']) for connection_param in connection_params
    ]
    logging.info(f"db_task_list: {db_task_list}\n")
    logging.info(f"cleanup_task_id: {cleanup_task_id}\n")

    if num_of_requests_not_processed <= 0:
        logging.info(f"\nBranching: Will jump to cleanup: {cleanup_task_id}\n")
        return cleanup_task_id
    else:
        logging.info(f"\nBranching: Will continue the operation for each db: {db_task_list}\n")
        return db_task_list


def push_requests_not_processed_metrics(num_of_requests_not_processed):
    num_of_requests_not_processed_gauge: Gauge = get_or_register_gauge(
        job=dag_name,
        name="datasrvc_snowflake_partner_dsr_requests_not_processed",
        description="The count of partner dsr requests not processed",
        labels=[]
    )

    num_of_requests_not_processed_gauge.set(num_of_requests_not_processed)
    push_all(dag_name, grouping_key={"env": TtdEnvFactory.get_from_system().execution_env})


def calc_execution_stats(connection, run_id):
    one_million = 1000 * 1000
    sql_statement = (
        f"WITH data AS (\n"
        f"  SELECT\n"
        f"    database_name,\n"
        f"    SPLIT_PART(QUERY_TAG, ';', 2) AS OP_STEP,\n"
        f"    SPLIT_PART(QUERY_TAG, ';', 3) AS RUN_ID,\n"
        f"    SUM(EXECUTION_TIME) AS EXECUTION_TIME,\n"
        f"    SUM(TOTAL_ELAPSED_TIME) AS TOTAL_ELAPSED_TIME,\n"
        f"    (SUM(CREDITS_USED_Cloud_Services * {one_million})::NUMERIC) AS CREDITS_USED_CLOUD_SERVICES,\n"
        f"    Sum(\n"
        f"      CASE\n"
        f"         WHEN Split_part(query_tag, ';', 2) = 'Process'\n"
        f"         AND      query_type <> 'UPDATE' THEN 0\n"
        f"         ELSE rows_produced\n"
        f"      END ) AS rows_produced\n"
        f"  FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(\n"
        f"    WAREHOUSE_NAME => 'PARTNER_DSR_WH'\n"
        f"  ))\n"
        f"  WHERE  SPLIT_PART(QUERY_TAG, ';', 3)  = '{run_id}'\n"
        f"  GROUP BY 1, 2, 3\n"
        f")\n"
        f"SELECT\n"
        f"  snowflake_partner_dsr_query_execution_stats\n"
        f"      AS \"snowflake_partner_dsr_query_execution_stats\",\n"
        f"  database_name,\n"
        f"  op_step,\n"
        f"  measurement_name,\n"
        f"  run_id,\n"
        f"FROM data\n"
        f"UNPIVOT(\n"
        f"  snowflake_partner_dsr_query_execution_stats\n"
        f"      FOR measurement_name IN\n"
        f"          (EXECUTION_TIME, TOTAL_ELAPSED_TIME, CREDITS_USED_CLOUD_SERVICES, ROWS_PRODUCED)\n"
        f");\n"
    )

    rows = execute_sql_statement_with_return(connection, sql_statement)
    logging.info(f"snowflake_partner_dsr_query_execution_stats, rows from db: \n{rows}\n")

    return rows


def update_push_execution_stats(rows):
    snowflake_partner_dsr_query_execution_stats_try: Gauge = get_or_register_gauge(
        job=dag_name,
        name="snowflake_partner_dsr_query_execution_stats_try",
        description="snowflake_partner_dsr_query_execution_stats_try",
        labels=["database_name", "op_step", "measurement_name", "run_id"]
    )
    for row in rows:
        snowflake_partner_dsr_query_execution_stats_try.labels(
            database_name=row[1],
            op_step=row[2],
            measurement_name=row[3],
            run_id=row[4],
        ).inc(row[0])
    push_all(dag_name, grouping_key={"env": TtdEnvFactory.get_from_system().execution_env})


def calc_push_execution_stats(connection_param, **kwargs):
    logging.info(f"calculate and push run state for {connection_param['database']}.")

    run_id = kwargs['run_id']
    connection = get_snowflake_connection(connection_param, FlowStep.Stats, run_id)

    rows = calc_execution_stats(connection, run_id)
    if rows:
        update_push_execution_stats(rows)


# TODO: remove the following line after the primary_database_param is enabled in connection_params.
# set_connection_param_default_values(primary_database_param)

for connection_param in connection_params:
    set_connection_param_default_values(connection_param)

ensure_partner_dsr_request_flow_task_id = _ensure_flow_task_id_format_string.format(primary_database_param['database'])
ensure_partner_dsr_request_flow_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id=ensure_partner_dsr_request_flow_task_id,
        python_callable=ensure_partner_dsr_request_flow,
        op_args=[primary_database_param, dsr_config],
        provide_context=True,
    )
)

load_dsr_task_id = _load_dsr_task_id_string_format.format(primary_database_param['database'])
load_partner_dsr_request_into_snowflake_primary_db_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id=load_dsr_task_id,
        python_callable=load_partner_dsr_request_into_snowflake_primary_db,
        op_args=[primary_database_param],
        provide_context=True,
    )
)
dag >> ensure_partner_dsr_request_flow_task >> load_partner_dsr_request_into_snowflake_primary_db_task

update_request_task_id = _update_request_task_id_format_string.format(primary_database_param['database'])
update_request_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id=update_request_task_id,
        python_callable=update_request,
        op_args=[primary_database_param, load_dsr_task_id],
        provide_context=True,
    )
)


def update_scrubbed_dsr_request(connection, max_dsr_request_seq_id, schema_name):
    sql_statement = (
        f"UPDATE {schema_name}.partner_dsr_request_scrubbing\n"
        f"SET ProcessedTimeStamp = CURRENT_TIMESTAMP()\n"
        f"WHERE ProcessedTimeStamp IS NULL\n"
        f"AND ID <= {max_dsr_request_seq_id};\n"
    )
    execute_sql_statements(connection, [sql_statement])


# The Cleanup task should be run if at least one of the upstream tasks succeeds
cleanup_task_id = _cleanup_dsr_data_task_id_string_format.format(primary_database_param['database'])
cleanup_dsr_data_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id=cleanup_task_id,
        python_callable=cleanup_dsr_data,
        op_args=[primary_database_param],
        provide_context=True,
        trigger_rule='none_failed',
    )
)

calc_push_execution_stats_task_id = _calc_push_execution_stats_task_id_string_format.format(primary_database_param['database'])
calc_push_execution_stats_task = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id=calc_push_execution_stats_task_id,
        python_callable=calc_push_execution_stats,
        op_args=[primary_database_param],
        provide_context=True,
    )
)

update_request_task >> cleanup_dsr_data_task >> calc_push_execution_stats_task
_branch_condition_check_task_id_string_format = "branch_condition_check_"
branch_condition_check_task_id = _branch_condition_check_task_id_string_format.format(primary_database_param['database'])
branch_condition_check_task = OpTask(
    op=BranchPythonOperator(
        dag=adag,
        task_id=branch_condition_check_task_id,
        python_callable=branch_condition_check,
        op_args=[primary_database_param],
        provide_context=True,
    )
)

load_partner_dsr_request_into_snowflake_primary_db_task >> branch_condition_check_task
branch_condition_check_task >> cleanup_dsr_data_task

for connection_param in connection_params:
    ensure_dsr_task_id = _ensure_dsr_schema_and_column_task_id_string_format.format(connection_param['database'])
    ensure_dsr_schema_and_column_task = OpTask(
        op=PythonOperator(
            dag=adag,
            task_id=ensure_dsr_task_id,
            python_callable=ensure_dsr_schema_and_column,
            op_args=[connection_param, dsr_config],
            provide_context=True,
        )
    )
    branch_condition_check_task >> ensure_dsr_schema_and_column_task

    for reds_table in reds_tables:
        region_table_task_id = _region_table_task_id_string_format.format(connection_param['database'], reds_table['table_name'])
        region_table_task = OpTask(
            op=PythonOperator(
                dag=adag,
                task_id=region_table_task_id,
                python_callable=process_partner_dsr_request_for_region,
                op_args=[connection_param, dsr_config, reds_table],
                provide_context=True,
            )
        )
        ensure_dsr_schema_and_column_task >> region_table_task
        region_table_task >> update_request_task
