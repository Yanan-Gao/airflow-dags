import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.tasks.op import OpTask


class ConverterDataTypeId(Enum):
    TrackingTag = 2
    ImportedAdvertiserData = 8
    ImportedAdvertiserDataWithBaseBid = 9
    EcommerceCatalogList = 19
    CrmData = 20


class ConverterDataEventTypeId(Enum):
    Purchase = 2
    SubscribeToNewsletter = 13
    LoyaltyMembers = 14


@dataclass
class ConverterCategorizationSprocConfig:
    mssql_conn_id: str
    batch_size: int
    test_only: int
    delay: float


@dataclass
class ConverterCategorizationSprocTaskSpecificConfig:
    data_type_id: ConverterDataTypeId
    data_event_type_id: ConverterDataEventTypeId
    is_categorized_by_keywords: int


def get_purchase_task(data_type_id: ConverterDataTypeId, sproc_config: ConverterCategorizationSprocConfig, is_categorized_by_keywords):
    task_id = f'{data_type_id}_purchase_task' if is_categorized_by_keywords else f'{data_type_id}_purchase_categorized_by_offlinedataprovider_table_task'
    purchase_task = OpTask(
        op=PythonOperator(
            task_id=task_id,
            python_callable=exec_1pd_converter_categorization_sproc,
            op_kwargs={
                'sproc_config':
                sproc_config,
                'task_specific_config':
                ConverterCategorizationSprocTaskSpecificConfig(data_type_id, ConverterDataEventTypeId.Purchase, is_categorized_by_keywords)
            }
        )
    )
    return purchase_task


def get_data_type_id_task(data_type_id: ConverterDataTypeId, sproc_config: ConverterCategorizationSprocConfig):
    is_categorized_by_keywords = 1
    purchase_task = get_purchase_task(data_type_id, sproc_config, is_categorized_by_keywords=is_categorized_by_keywords)

    loyalty_members_task = OpTask(
        op=PythonOperator(
            task_id=f'{data_type_id}_loyalty_members_task',
            python_callable=exec_1pd_converter_categorization_sproc,
            op_kwargs={
                'sproc_config':
                sproc_config,
                'task_specific_config':
                ConverterCategorizationSprocTaskSpecificConfig(
                    data_type_id, ConverterDataEventTypeId.LoyaltyMembers, is_categorized_by_keywords
                )
            }
        )
    )

    subscribe_to_newsletter_task = OpTask(
        op=PythonOperator(
            task_id=f'{data_type_id}_subscribe_to_newsletter_task',
            python_callable=exec_1pd_converter_categorization_sproc,
            op_kwargs={
                'sproc_config':
                sproc_config,
                'task_specific_config':
                ConverterCategorizationSprocTaskSpecificConfig(
                    data_type_id, ConverterDataEventTypeId.SubscribeToNewsletter, is_categorized_by_keywords
                )
            }
        )
    )

    purchase_task >> loyalty_members_task >> subscribe_to_newsletter_task

    return purchase_task, subscribe_to_newsletter_task


def exec_1pd_converter_categorization_sproc(
    sproc_config: ConverterCategorizationSprocConfig, task_specific_config: ConverterCategorizationSprocTaskSpecificConfig
):
    sql_hook = MsSqlHook(mssql_conn_id=sproc_config.mssql_conn_id, schema='Provisioning')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    session_id = uuid.uuid4()
    get_1pd_for_converter_categorization_sproc = f"""
        EXEC dbo.prc_Get1PDForConverterCategorization
            @sessionID = '{session_id}',
            @dataTypeId = {task_specific_config.data_type_id.value},
            @dataEventTypeId = {task_specific_config.data_event_type_id.value},
            @isCategorizedByKeywords = {task_specific_config.is_categorized_by_keywords}
    """
    logging.info(f"Executing sproc:\n{get_1pd_for_converter_categorization_sproc}")
    cursor.execute(get_1pd_for_converter_categorization_sproc)
    process_targeting_data_count = cursor.fetchone()[0]
    logging.info(f"Count of targeting data to be categorized: {process_targeting_data_count}")

    current_offset = 0
    while current_offset < process_targeting_data_count:
        categorize_1pd_into_converters_sproc = f"""
            EXEC dbo.prc_Categorize1PDIntoConverters
                @sessionID = '{session_id}',
                @dataEventTypeId = {task_specific_config.data_event_type_id.value},
                @currentOffset = {current_offset},
                @batchSize = {sproc_config.batch_size},
                @testOnly = {sproc_config.test_only}
        """
        cursor.execute(categorize_1pd_into_converters_sproc)
        current_offset += sproc_config.batch_size
        time.sleep(sproc_config.delay)
        result = cursor.fetchone()
        logging.info(f"Result: {result}")
