import logging
from enum import Enum
from textwrap import dedent
from typing import List

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from dags.pdg.data_subject_request.config.vertica_config import cleanse_key

# https://vault.adsrvr.org/ui/vault/secrets/secret/kv/SCRUM-PDG%2FAirflow%2FConnections%2Fttd-lwdb-dsr-connection/details?namespace=team-secrets&version=1
LWDB_CONNECTION_ID = 'ttd-lwdb-dsr-connection'


# Must match adplatform/TTD/Common/Logging/TTD.Common.Logging.Structured/LogProcessing/LogFileTaskStatus.cs
class LogFileTaskStatus(Enum):
    DefaultNone = 0
    NotReady = 1
    Ready = 2
    Processing = 3
    Failed = 4
    Completed = 5
    Ignored = 6
    Retry = 7
    Repeat = 8


class LogExTask(Enum):
    VerticaScrubGenerator = ('dbo.fn_Enum_Task_VerticaScrubGenerator()', lambda key: key)
    VerticaScrub = ('dbo.fn_Enum_Task_VerticaScrub()', lambda key: f'{cleanse_key(key)}/%')


def monitor_logex_task(**kwargs):
    table_to_s3_keys_mapping = kwargs['ti'].xcom_pull(key='table_to_s3_keys')
    table_name = kwargs['templates_dict']['table_name']
    logex_task: LogExTask = kwargs['templates_dict']['logex_task']

    if table_name not in table_to_s3_keys_mapping.keys():
        logging.info("No scrub tasks generated for table " + table_name)
        return True

    logging.info("Connecting to LWDB")
    connection = BaseHook.get_connection(LWDB_CONNECTION_ID)
    hook = connection.get_hook()
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()

    log_file_task_id, key_transformer = logex_task.value
    s3_keys_transformed = list(map(key_transformer, table_to_s3_keys_mapping[table_name]))

    outcomes: List[bool] = []
    for s3_key in s3_keys_transformed:
        log_file_task_status_id_sql = dedent(
            f"""
            SELECT LFT.LogFileId, LF.S3RawLogKey, LFT.LogFileTaskStatusId
            FROM dbo.LogFileTask as LFT
            INNER JOIN dbo.LogFile as LF
            ON LFT.LogFileId = LF.LogFileId
            WHERE LF.S3RawLogKey LIKE '{s3_key}' AND LFT.TaskId = {log_file_task_id}
            """
        )
        logging.info(f'{logex_task}, executing query="{log_file_task_status_id_sql}"')
        cursor.execute(log_file_task_status_id_sql)

        for (log_file_id, s3_key, lft_status_id) in cursor:
            outcomes.append(map_log_file_status_to_outcome(logex_task, table_name, log_file_id, s3_key, LogFileTaskStatus(lft_status_id)))

    logging.info("Returning outcomes")
    return bool(outcomes) and all(outcomes)


def map_log_file_status_to_outcome(logex_task: LogExTask, table_name: str, log_file_id: str, s3_log_key: str, status: LogFileTaskStatus):
    if status is LogFileTaskStatus.Completed:
        logging.info(f"{logex_task.name}: status=Completed, table={table_name}, log_file_id={log_file_id}, s3_log_key={s3_log_key}")
        return True
    if status in (LogFileTaskStatus.Failed, LogFileTaskStatus.Ignored):
        logging.error(f"{logex_task.name}: status={status.name}, table={table_name}, log_file_id={log_file_id}, s3_log_key={s3_log_key}")
        raise AirflowException(
            f"{logex_task.name}: `{status.name}` for table {table_name}, log_file_id={log_file_id}, s3_log_key={s3_log_key}"
        )

    logging.info(
        f"{logex_task.name} in-progress: status={status.name} for table {table_name}, log_file_id={log_file_id}, s3_log_key={s3_log_key}"
    )
    return False
