from datetime import datetime, timezone

import airflow
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.interop.logworkflow_callables import LogFileBatchToVerticaLoadCallable, open_external_gate_for_log_type, check_task_enabled
from ttd.ttdenv import TtdEnv, TtdEnvFactory
from ttd.tasks.op import OpTask
from ttd.tasks.chain import ChainOfTasks
import hmac
import struct
import logging
from enum import Enum
from typing import Optional


class VerticaLoadWithLogs(ChainOfTasks):
    """
    VerticaLoadWithLogs operator, to import data from S3 to Vertica with custom LogType.

    :param dag:                  Top level airflow dag
    :param subdag_name:          Identifies the operator
    :param s3_bucket:            s3://ttd_XXX
    :param s3_bucket_prefix:     Rest of path before date_partition
    :param gating_type_id:       Get real value from LogWorkflowDB: dbo.GatingType
    :param date_partition:       date=YYYYMMDD or date=YYYYMMDD/hour=HH
    :param log_type_id:          dbo.fn_Enum_LogType_XXX
    :param log_start_time:       LogStartTime in LWDB
    :param vertica_table_id:     dbo.fn_Enum_VerticaTable_XXX
    :param copy_column_list_sql: Semicolon-separate string of column names to copy
    :param vertica_table_copy_version_id: call ThreadSafeMD5.ToPlatformNeutralHash( <copyColumnListSQL> ) from
        https://gitlab.adsrvr.org/thetradedesk/ttd.common/-/blob/master/src/BCL/TTD.Common.BCL/Concurrent/ThreadSafeMD5.cs
    :param vertica_load_enabled:     Flag to disable VerticaLoad when testing rest of dag separately
    :param job_environment:          prod, prodTest, test
    :param logworkflow_sandbox_connection:
    :param logworkflow_connection:
    """

    def __init__(
        self,
        dag: airflow.DAG,
        subdag_name: str,
        s3_bucket: str,
        s3_bucket_prefix: str,
        gating_type_id: int,
        date_partition: str,
        log_type_id: int,
        log_start_time: str,
        vertica_table_id: int,
        copy_column_list_sql: str,
        vertica_table_copy_version_id: Optional[int] = None,
        vertica_load_enabled: bool = True,
        job_environment: TtdEnv = TtdEnvFactory.get_from_system(),
        logworkflow_sandbox_connection: str = 'sandbox-lwdb',
        logworkflow_connection: str = 'lwdb',
    ):
        if vertica_table_copy_version_id is None:
            vertica_table_copy_version_id = self.calculate_platform_neutral_hash(copy_column_list_sql)

        self.check_vertica_load_enabled_task = OpTask()
        self.create_logworkflow_entry_task = OpTask()
        self.open_external_gate_task = OpTask()

        self.check_vertica_load_enabled_task >> self.create_logworkflow_entry_task
        self.create_logworkflow_entry_task >> self.open_external_gate_task

        super().__init__(task_id=subdag_name, first=self.check_vertica_load_enabled_task, last=self.open_external_gate_task)

        is_vertica_load_enabled = ShortCircuitOperator(
            task_id=f"is_vertica_load_enabled_{subdag_name}",
            python_callable=check_task_enabled,
            op_kwargs={"task_enabled": vertica_load_enabled},
            dag=dag,
            trigger_rule="none_failed"
        )

        def get_vertica_load_object_list(
            log_start_time: str,
            s3_prefix: str,
            partition: str,
            log_type_id: int,
        ):
            hook = CloudStorageBuilder(CloudProviders.aws).build()

            s3_prefix_with_time = s3_prefix + partition

            keys = hook.list_keys(prefix=s3_prefix_with_time, bucket_name=s3_bucket)
            if keys is None or len(keys) == 0:
                raise Exception(
                    f"Expected non-zero number of files for VerticaLoad log type {log_type_id}, for prefix {s3_prefix_with_time}, log_start_time: {log_start_time}"
                )
            now_utc = datetime.now(timezone.utc)
            logfiletask_endtime = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
            # CloudServiceId 1 == AWS, DataDomain 1 == TTD_RestOfWorld
            object_list = [(log_type_id, f"{s3_prefix_with_time}", log_start_time, 1, 0, 1, 1, 0, logfiletask_endtime)]

            logging.info("Successfully obtained object list of logs")
            logging.info(object_list)
            return object_list

        vertica_load_task_create_logworkflow_entry = PythonOperator(
            dag=dag,
            python_callable=LogFileBatchToVerticaLoadCallable,
            op_kwargs={
                "database": "LogWorkflow",
                "mssql_conn_id": logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
                "get_object_list": get_vertica_load_object_list,
                "s3_prefix": s3_bucket_prefix,
                "partition": date_partition,
                "bucket_name": s3_bucket,
                "log_start_time": log_start_time,
                "log_type_id": log_type_id,
                "vertica_load_sproc_arguments": {
                    "verticaTableId": vertica_table_id,
                    "lineCount": 1,
                    "verticaTableCopyVersionId": vertica_table_copy_version_id,
                    "copyColumnListSQL": copy_column_list_sql
                }
            },
            task_id=f"vertica_load_{subdag_name}_create_logworkflow_entry",
        )

        vertica_load_task_ready_logworkflow_entry = PythonOperator(
            task_id=f"vertica_load_{subdag_name}_readylogworkflowentry",
            python_callable=open_external_gate_for_log_type,
            op_kwargs={
                'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
                'sproc_arguments': {
                    'gatingTypeId': gating_type_id,
                    'logTypeId': log_type_id,
                    'logStartTimeToReady': log_start_time,
                }
            },
            dag=dag,
        )

        self.check_vertica_load_enabled_task.set_op(op=is_vertica_load_enabled)
        self.create_logworkflow_entry_task.set_op(op=vertica_load_task_create_logworkflow_entry)
        self.open_external_gate_task.set_op(op=vertica_load_task_ready_logworkflow_entry)

    def calculate_platform_neutral_hash(self, copy_column_list_sql: str):
        adplatform_key = [2, 8, 30, 36, 48, 28, 32, 33, 34, 45]
        md5_key = bytearray(adplatform_key)
        hash = hmac.new(md5_key, copy_column_list_sql.encode('utf-8'), 'MD5').hexdigest()
        first_four_bytes = bytearray.fromhex(hash)[:4]
        vertica_table_copy_version_id = struct.unpack('l', first_four_bytes)
        return vertica_table_copy_version_id


class LogTypeFrequency(Enum):
    """
    LogType_Enum from Adplatform
    """
    HOURLY = 38
    DAILY = 39
    WEEKLY = 152


class VerticaImportFromCloud(ChainOfTasks):
    """
    VerticaImportFromCloud operators to import hourly or daily data, presets with log_type_frequency.
    :param dag:                      Top level airflow dag
    :param subdag_name:              Identifies the operator
    :param gating_type_id:           Get real value from LogWorkflowDB: dbo.GatingType
    :param log_type_id:              LogTypeId in LWDB
    :param log_start_time:           LogStartTime in LWDB
    :param log_type_frequency:       Hourly or Daily processing presets
    :param vertica_import_enabled:   Flag to disable VerticaLoad when testing rest of dag separately
    :param job_environment:          prod, prodTest, test
    :param logworkflow_sandbox_connection:
    :param logworkflow_connection:
    """

    def __init__(
        self,
        dag: airflow.DAG,
        subdag_name: str,
        gating_type_id: int,
        log_type_id: Optional[int] = None,
        log_start_time: Optional[str] = None,
        log_type_frequency: Optional[LogTypeFrequency] = None,
        vertica_import_enabled: bool = True,
        job_environment: TtdEnv = TtdEnvFactory.get_from_system(),
        logworkflow_sandbox_connection: str = 'sandbox-lwdb',
        logworkflow_connection: str = 'lwdb',
    ):
        if log_type_id and log_type_frequency:
            raise Exception("Cannot define both log_type_frequency and log_type_id! Conflict!")
        elif not log_type_id and not log_type_frequency:
            raise Exception("Missing log_type_id definition!")
        elif not log_start_time and not log_type_frequency:
            raise Exception("Missing log_start_time definition!")

        # Set hourly/daily common parameters
        if log_type_frequency is not None:
            log_type_id = log_type_frequency.value
            if log_type_frequency == LogTypeFrequency.HOURLY:
                log_start_time = '{{ logical_date.strftime(\"%Y-%m-%d %H:00:00\") }}'
            elif log_type_frequency == LogTypeFrequency.DAILY:
                log_start_time = '{{ logical_date.strftime(\"%Y-%m-%d 00:00:00\") }}'

        self.check_vertica_import_enabled_task = OpTask()
        self.open_external_gate_task = OpTask()

        super().__init__(task_id=subdag_name, tasks=[self.check_vertica_import_enabled_task, self.open_external_gate_task])

        is_vertica_import_enabled = ShortCircuitOperator(
            task_id=f"is_vertica_import_enabled_{subdag_name}",
            python_callable=check_task_enabled,
            op_kwargs={"task_enabled": vertica_import_enabled},
            dag=dag,
            trigger_rule="none_failed"
        )

        vertica_import_task_ready_logworkflow_entry = PythonOperator(
            task_id=f"vertica_import_{subdag_name}_ready_logworkflow_entry",
            python_callable=open_external_gate_for_log_type,
            op_kwargs={
                'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
                'sproc_arguments': {
                    'gatingTypeId': gating_type_id,
                    'logTypeId': log_type_id,
                    'logStartTimeToReady': log_start_time,
                }
            },
            dag=dag,
        )

        self.check_vertica_import_enabled_task.set_op(op=is_vertica_import_enabled)
        self.open_external_gate_task.set_op(op=vertica_import_task_ready_logworkflow_entry)
