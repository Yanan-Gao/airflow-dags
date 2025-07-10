import logging
from typing import Optional, Callable, List, Tuple, Any, Dict

import jinja2
import pymssql
import json
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def LogFileBatchToVerticaLoadCallable(
    database: str,
    mssql_conn_id: str,
    vertica_load_sproc_arguments: dict,
    get_object_list: Callable,
    autocommit=True,
    **kwargs,
):
    """
        Executes CreateOrCollectLogFileBatch -> CreateVerticaLoadLogFile for a set of input objects.
        Pass a function that returns the list of input objects to process as the value to get_object_list.
        The input object list should be a list of tuples with the following structure:
            (LogTypeId, S3RawLogKey, LogStartTime, IsCollected, HasLateData, CloudServiceId, DataDomainId, LogFileId)

        See demo-lwf-createorcollect.py / demo-lwf-test DAG for a working example.
    """

    # resolve object_list before doing any work
    object_list = get_object_list(**kwargs)

    # skipping hook.run() so we can have more control over our sql session
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema=database)
    conn = hook.get_conn()
    conn.autocommit(autocommit)
    cursor = conn.cursor(as_dict=True)

    log_file_id_map = _CreateOrCollectLogFileBatch(object_list=object_list, cursor=cursor, return_log_file_id_map=True)

    logging.info("translate map to S3RawLogKey -> LogFileId ex: {'test623': 50} for use in CreateVerticaLoad")
    assert log_file_id_map is not None
    s3_key_to_log_file_id_map = {item["S3RawLogKey"]: item["LogFileId"] for item in log_file_id_map}

    for s3_raw_log_key, log_file_id in s3_key_to_log_file_id_map.items():
        logging.info("VerticaLoad: ", s3_raw_log_key, log_file_id)
        _VerticaLoad(
            cursor=cursor,
            s3_raw_log_key=s3_raw_log_key,
            log_file_id=log_file_id,
            sproc_arguments=vertica_load_sproc_arguments,
        )


def LogFileBatchProcessCallable(
    database: str,
    mssql_conn_id: str,
    get_object_list: Callable,
    autocommit=True,
    **kwargs,
):
    """
        Executes LogFileBatchProcessCallable -> CreateVerticaLoadLogFile for a set of input objects.
        Pass a function that returns the list of input objects to process as the value to get_object_list.
        The input object list should be a list of tuples with the following structure:
            (LogTypeId, S3RawLogKey, LogStartTime, IsCollected, HasLateData, CloudServiceId, DataDomainId, LogFileId)
        Note we only create new log file record in the LogFile table by calling _CreateOrCollectLogFileBatch
    """

    # resolve object_list before doing any work
    object_list = get_object_list(**kwargs)

    # skipping hook.run() so we can have more control over our sql session
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema=database)
    conn = hook.get_conn()
    conn.autocommit(autocommit)
    cursor = conn.cursor(as_dict=True)

    _CreateOrCollectLogFileBatch(object_list=object_list, cursor=cursor, return_log_file_id_map=True)

    logging.info(f"Added new record in the Log File for log type id {kwargs['log_type_id']}")


def _CreateOrCollectLogFileBatch(cursor: pymssql.Cursor, object_list: List[Tuple], return_log_file_id_map=True) -> \
        Optional[dict]:
    log_file_batch_sproc_name = "prc_CreateOrCollectLogFileBatchV2"
    temp_table_name = "#temp_LogFileWorkList_with_LogFileTaskEndTime"

    create_temp_table = jinja2.Template(
        """CREATE TABLE {{ temp_table_name }} (
            LogTypeId int not null,
            S3RawLogKey nvarchar(1024) not null,
            LogStartTime datetime not null,
            IsCollected bit not null,
            HasLateData bit not null,
            CloudServiceId tinyint not null,
            DataDomainId tinyint not null,
            LogFileId bigint null,
            LogFileTaskEndTime datetime null
        );"""
    )

    insert_into_temp_table = jinja2.Template(
        """INSERT INTO {{ temp_table_name }}(LogTypeId, S3RawLogKey, LogStartTime, IsCollected, HasLateData, CloudServiceId, DataDomainId, LogFileId, LogFileTaskEndTime)
            VALUES
            {%- for object in object_list %}
                {{ object }}{% if not loop.last %},{% else %};{% endif %}
            {%- endfor -%}
        """
    )

    construct_log_file_id_map = jinja2.Template(
        """SELECT lf.LogFileId, lf.S3RawLogKey
            FROM {{ temp_table_name }}_Copy tmp
            INNER JOIN dbo.LogFile lf on lf.S3RawLogKey = tmp.S3RawLogKey
                AND lf.LogTypeId = tmp.LogTypeId
                AND lf.LogStartTime = tmp.LogStartTime
        """
    )

    logging.info("create, populate temporary table first for feeding into batch sproc")
    cursor.execute(create_temp_table.render(temp_table_name=temp_table_name))

    if return_log_file_id_map:
        cursor.execute(insert_into_temp_table.render(temp_table_name=temp_table_name, object_list=object_list))
        logging.info("make a copy of the work list table to use as an input to the returned log file id map")
        cursor.execute(f"select * into [{temp_table_name}_Copy] from [{temp_table_name}]")

    logging.info("run the batch sproc, which will pop and process items off of the work list table")
    cursor.execute(f"EXEC {log_file_batch_sproc_name}")

    if return_log_file_id_map:
        logging.info("create, save a map for returning LogFileIds for each inserted LogFile")
        cursor.execute(construct_log_file_id_map.render(temp_table_name=temp_table_name))
        log_file_id_map = cursor.fetchall()
        return log_file_id_map
    else:
        return None


def _VerticaLoad(cursor: pymssql.Cursor, s3_raw_log_key: str, log_file_id: str, sproc_arguments: dict) -> None:
    vertica_load_sproc_name = "prc_CreateVerticaLoadLogFile"

    vertica_load_execute_sproc = jinja2.Template(
        """ DECLARE @logFileId bigint, @verticaTableId int, @s3VerticaLoadLogKey nvarchar(1024);
        DECLARE @lineCount bigint, @verticaTableCopyVersionId bigint, @copyColumnListSQL nvarchar(max);

        SET @logFileId = {{ log_file_id }};
        SET @verticaTableId = {{ sproc_arguments['verticaTableId'] }};
        SET @s3VerticaLoadLogKey = '{{ s3_raw_log_key }}';
        SET @lineCount = {{ sproc_arguments['lineCount'] }};
        SET @verticaTableCopyVersionId = {{ sproc_arguments['verticaTableCopyVersionId'] }};
        SET @copyColumnListSQL = '{{ sproc_arguments['copyColumnListSQL'] }}';

        EXEC [{{ sproc_name }}] @logFileId, @verticaTableId, @s3VerticaLoadLogKey, @lineCount, @verticaTableCopyVersionId, @copyColumnListSQL
        """
    )

    cursor.execute(
        vertica_load_execute_sproc.render(
            s3_raw_log_key=s3_raw_log_key,
            log_file_id=log_file_id,
            sproc_arguments=sproc_arguments,
            sproc_name=vertica_load_sproc_name,
        )
    )


def _TimeGateOpen(mssql_conn_id: str, sproc_arguments: dict):
    # skipping hook.run() so we can have more control over our sql session
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor(as_dict=False)
    execute_sproc = jinja2.Template(
        """
        DECLARE @timeGateName varchar(100), @windowStartTime datetime2, @variantId int;
        SET NOCOUNT ON
        SET @timeGateName = '{{ sproc_arguments['timeGateName'] }}';
        SET @windowStartTime = CAST ( '{{ sproc_arguments['windowStartTime'] }}' as datetime2);
        SET @variantId = '{{ sproc_arguments['variantId'] }}';
        EXEC prc_OpenTimeGateExternal @timeGateName, null, @windowStartTime, @variantId
        """
    ).render(sproc_arguments=sproc_arguments)
    logging.info(execute_sproc)
    try:
        cursor.execute(execute_sproc)
    except Exception:
        logging.warn(
            "failed on prc_OpenTimeGate %s, %s, %d", sproc_arguments['timeGateName'], sproc_arguments['windowStartTime'],
            sproc_arguments['variantId']
        )
        raise ValueError('sproc call failed')


def _TimeGateStatus(mssql_conn_id: str, sproc_arguments: dict):
    # skipping hook.run() so we can have more control over our sql session
    print(sproc_arguments['timeGateName'])
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(False)
    cursor = conn.cursor(as_dict=False)
    execute_sproc = jinja2.Template(
        """
        DECLARE @timeGateName varchar(100), @windowStartTime datetime2, @variantId int, @gateStatus int;
        SET NOCOUNT ON
        SET @timeGateName = '{{ sproc_arguments['timeGateName'] }}';
        SET @windowStartTime = CAST ( '{{ sproc_arguments['windowStartTime'] }}' as datetime2);
        SET @variantId = '{{ sproc_arguments['variantId'] }}';
        EXEC @gateStatus = prc_CheckTimeGateStatus null, @timeGateName, @windowStartTime, @variantId
        SELECT @gateStatus as gateStatus
        """
    ).render(sproc_arguments=sproc_arguments)
    logging.info(execute_sproc)
    try:
        cursor.execute(execute_sproc)
        gateStatus = cursor.fetchone()[0]
        logging.info(gateStatus)
        if gateStatus != 1:
            raise ValueError('Gate is not open')
    except Exception:
        logging.warn(
            "failed on prc_CheckBatchGateState %s, %s, %d", sproc_arguments['timeGateName'], sproc_arguments['windowStartTime'],
            sproc_arguments['variantId']
        )
        raise ValueError('sproc call failed')


def IsVerticaEnabled(mssql_conn_id: str, sproc_arguments: dict):
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    cursor = conn.cursor(as_dict=False)
    execute_sproc = jinja2.Template(
        """
        DECLARE @verticaClusterId varchar(64);
        SET NOCOUNT ON
        SET @verticaClusterId = '{{ sproc_arguments['verticaClusterId'] }}';
        SELECT IsEnabled as isEnabled FROM dbo.VerticaCluster WHERE VerticaClusterId = @verticaClusterId
        """
    ).render(sproc_arguments=sproc_arguments)
    logging.info(execute_sproc)
    try:
        cursor.execute(execute_sproc)
        isEnabled = cursor.fetchone()[0]
        logging.info(isEnabled)
        return False if isEnabled == 0 else True
    except Exception:
        logging.warn("failed on checking whether the vertica cluster is enabled for %s", sproc_arguments['verticaClusterId'])
        raise ValueError('sproc call failed')


def ExternalGateOpen(mssql_conn_id: str, sproc_arguments: dict, **kwargs):
    # skipping hook.run() so we can have more control over our sql session
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor(as_dict=False)
    execute_sproc = jinja2.Template(
        """
        DECLARE @gatingType int, @grain int, @dateTimeToOpen datetime2, @runData varchar(max);
        SET NOCOUNT ON
        SET @gatingType = '{{ sproc_arguments['gatingType'] }}';
        SET @grain = '{{ sproc_arguments['grain'] }}';
        SET @dateTimeToOpen = CAST ( '{{ sproc_arguments['dateTimeToOpen'] }}' as datetime2);
        SET @runData = {% if 'runData' in sproc_arguments %}'{{ sproc_arguments['runData'] }}'{% else %}NULL{% endif %};
        EXEC [WorkflowEngine].[prc_OpenExternalGate] @gatingType, @grain, @dateTimeToOpen, @runData = @runData
        """
    ).render(sproc_arguments=sproc_arguments)
    logging.info(execute_sproc)
    try:
        cursor.execute(execute_sproc)
    except Exception:
        logging.warn(
            "failed on prc_OpenExternalGate %s, %s, %d, %d", sproc_arguments['gatingType'], sproc_arguments['grain'],
            sproc_arguments['dateTimeToOpen'], sproc_arguments.get('runData', None)
        )
        raise ValueError('sproc call failed')


def open_external_gate_for_log_type(mssql_conn_id: str, sproc_arguments: Dict[str, Any]):
    # skipping hook.run() so we can have more control over our sql session
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor(as_dict=False)
    execute_sproc = jinja2.Template(
        """
        DECLARE @gatingTypeId int, @logTypeId int, @logStartTimeToReady datetime2;
        SET NOCOUNT ON
        SET @gatingTypeId = '{{ sproc_arguments['gatingTypeId'] }}';
        SET @logTypeId = '{{ sproc_arguments['logTypeId'] }}';
        SET @logStartTimeToReady = CAST ( '{{ sproc_arguments['logStartTimeToReady'] }}' as datetime2);
        EXEC [WorkflowEngine].[prc_OpenExternalGateForLogType] @gatingTypeId, @logTypeId, @logStartTimeToReady
        """
    ).render(sproc_arguments=sproc_arguments)
    logging.info(execute_sproc)
    try:
        cursor.execute(execute_sproc)
    except BaseException as err:
        logging.error(
            f'Failed on prc_OpenExternalGateForLogType {sproc_arguments["gatingTypeId"]}, {sproc_arguments["logTypeId"]}, {sproc_arguments["logStartTimeToReady"]}'
        )
        raise ValueError(f'Sproc call failed: {err}')


def check_task_enabled(task_enabled: bool) -> bool:
    logging.info(f"Task status: {task_enabled}")
    return task_enabled


def to_sql_string(val: str | None) -> str:
    # only for logging purposes, unsafe characters are not escaped
    return f"'{val}'" if val is not None else 'null'


def ExecuteOnDemandDataMove(mssql_conn_id: str, sproc_arguments: Dict[str, Any], **kwargs):
    # skipping hook.run() so we can have more control over our sql session
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor(as_dict=False)

    args = json.dumps(sproc_arguments['args']) if 'args' in sproc_arguments and sproc_arguments['args'] else None
    prefix = sproc_arguments['prefix'] if 'prefix' in sproc_arguments else None

    execute_sproc = """
        DECLARE @logfileId bigint, @taskId bigint, @prefix nvarchar(256), @args varchar(max);
        SET NOCOUNT ON
        SET @taskId = %d;
        SET @prefix = %s;
        SET @args = %s;
        EXEC dbo.prc_CreateDataMoveRequest @taskId = @taskId, @args = @args, @locationPrefix = @prefix, @logfileId = @logfileId output;
        SELECT @logfileId as logfileId;
    """
    logging.info(execute_sproc % (sproc_arguments['taskId'], to_sql_string(prefix), to_sql_string(args)))
    try:
        cursor.execute(execute_sproc, (sproc_arguments['taskId'], prefix, args))
        logfileId = cursor.fetchone()[0]
        logging.info('logfileId = %i', logfileId)
    except Exception:
        logging.warn("failed on prc_CreateDataMoveRequest %i, %s, %s", sproc_arguments['taskId'], sproc_arguments['prefix'], args)
        raise ValueError('sproc call failed')
