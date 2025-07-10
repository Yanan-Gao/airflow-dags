import logging
from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from typing import List, Dict, Tuple

from ttd.ttdenv import TtdEnvFactory

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

TABLE_NAME = 'data-subject-requests'
PROCESSING_STATE_GSI = 'ProcessingStateIndex'
PROCESSING_STATE_ATTRIBUTE_NAME = 'processing_state'
PARTITION_KEY_ATTRIBUTE_NAME = 'pk'
SORT_KEY_ATTRIBUTE_NAME = 'sk'

# Partner DSR attributes #
PARTNER_DSR_TIMESTAMP = 'timestamp'
PARTNER_DSR_REQUEST_ID = 'requestId'
PARTNER_DSR_ADVERTISER_ID = 'advertiserId'
PARTNER_DSR_PARTNER_ID = 'partnerId'
PARTNER_DSR_TENANT_ID = 'tenantId'
PARTNER_DSR_USER_ID_GUID = 'userIdGuid'
PARTNER_DSR_USER_ID_RAW = 'rawUserId'
PARTNER_DSR_USER_ID_TYPE = 'userIdType'
PARTNER_DSR_DATA_PROVIDER_ID = 'dataProviderId'
PARTNER_DSR_REQUEST_DATA = 'partnerDsrRequestData'
PARTNER_DSR_CLOUD_PROCESSING_BATCH_ID = 'batchId'
PARTNER_DSR_MERCHANT_ID = 'merchantId'
# ------- #

# Partner DSR processing states #
# state transition as follows s3 processing -> ready for opt out -> opt out processing
# -> ready for cloud -> cloud_processing
PARTNER_DSR_PROCESSING_READY_FOR_OPT_OUT = 'ready_for_opt_out_processing'
PARTNER_DSR_PROCESSING_OPT_OUT = 'opt_out_processing'
PARTNER_DSR_PROCESSING_READY_FOR_CLOUD = 'ready_for_cloud_processing'
PARTNER_DSR_PROCESSING_CLOUD = 'cloud_processing'

EXPIRY_INTERVAL_DAYS = timedelta(days=90)


class DynamoDbClient:
    _table = None

    def __new__(cls):
        if cls._table is None:
            cls._table = boto3.resource('dynamodb', region_name='us-east-1').Table(TABLE_NAME)
        return cls._table


def build_sort_keys(uid2s, tdids):
    """
    Constructs sort keys out of uid2s and tdids.
    """
    sort_keys: List[str] = []
    for index, uid2 in enumerate(uid2s):
        sort_keys.append(f'{uid2}#{tdids[index]}')
    return sort_keys


def build_single_attribute_data(partition_key, sort_key, attribute_name: str, attribute_value):
    """
    Constructs a DynamoDB item that satisfies the data-subject-requests table
    """
    return {PARTITION_KEY_ATTRIBUTE_NAME: partition_key, SORT_KEY_ATTRIBUTE_NAME: sort_key, f'{attribute_name}': attribute_value}


def build_multiple_attribute_data(partition_key, sort_key, attribute_name_to_value: Dict[str, str]):
    """
    Constructs a DynamoDB item that satisfies the data-subject-requests table
    """
    item = {PARTITION_KEY_ATTRIBUTE_NAME: partition_key, SORT_KEY_ATTRIBUTE_NAME: sort_key}
    for attribute_name, attribute_value in attribute_name_to_value.items():
        item[attribute_name] = attribute_value
    return item


def write_batch(items, ignore_duplicate_key_exception=False):
    """
    Writes a batch of items to Amazon DynamoDB. Batches can contain keys from
    more than one table.  In addition, the batch writer will also automatically
    handle any unprocessed items and resend them as needed.

    param items: The items to put in the table. Each item must contain at least the keys required by the schema that
    was specified when the table was created.
    """
    duplicate_count = 0
    with DynamoDbClient().batch_writer() as writer:
        for item in items:
            try:
                writer.put_item(Item=item)
            except ClientError as err:
                if (ignore_duplicate_key_exception and err.response['Error']['Code'] == 'ValidationException'
                        and 'duplicates' in err.response['Error']['Message']):
                    duplicate_count += 1
                else:
                    logging.error(
                        f"Couldn't load data into table {TABLE_NAME}. "
                        f"Here's why: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
                    )
                    raise
    logging.info(f'Duplicate count of {duplicate_count} when trying to add items to dynamodb')


def put_item(item):
    item['expireAt'] = int((datetime.now() + EXPIRY_INTERVAL_DAYS).timestamp())
    try:
        DynamoDbClient().put_item(Item=item)
    except ClientError as err:
        logging.error(
            "Couldn't add item %s to table %s. Here's why: %s: %s", item, TABLE_NAME, err.response['Error']['Code'],
            err.response['Error']['Message']
        )
        raise


def update_item(item: Dict[str, str]):
    """
    @param item:  Needs to contain the following keys pk, sk, {attribute_names} associated with their values
    @return: None
    """
    partition_key = item.pop(PARTITION_KEY_ATTRIBUTE_NAME)
    sort_key = item.pop(SORT_KEY_ATTRIBUTE_NAME)
    update_item_attributes(partition_key, sort_key, item)


def update_items_attribute(partition_key, sort_key, attribute_name, attribute_value):
    update_item_attributes(partition_key, sort_key, {attribute_name: attribute_value})


def update_item_attributes(partition_key, sort_key, attributes):
    update_expressions = []
    expr_attribute_names = {}
    expr_attribute_values = {}

    for attribute_name, attribute_value in attributes.items():
        placeholder_name = f"#N_{attribute_name}"
        placeholder_value = f":V_{attribute_name}"

        update_expressions.append(f"{placeholder_name} = {placeholder_value}")
        expr_attribute_names[placeholder_name] = attribute_name
        expr_attribute_values[placeholder_value] = attribute_value

    update_expression = "SET " + ", ".join(update_expressions)

    try:
        logging.info(
            f"Updating {TABLE_NAME} \n"
            f"update_expression: {update_expression}\n"
            f"expression_attribute_names: {expr_attribute_names}\n"
            f"expression_attribute_values: {expr_attribute_values}"
        )
        DynamoDbClient().update_item(
            Key={
                PARTITION_KEY_ATTRIBUTE_NAME: partition_key,
                SORT_KEY_ATTRIBUTE_NAME: sort_key
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expr_attribute_names,
            ExpressionAttributeValues=expr_attribute_values,
            ReturnValues='NONE'
        )
    except ClientError as err:
        item = {PARTITION_KEY_ATTRIBUTE_NAME: partition_key, SORT_KEY_ATTRIBUTE_NAME: sort_key, **attributes}
        logging.error(
            "Couldn't update item %s to table %s. Here's why: %s: %s", item, TABLE_NAME, err.response['Error']['Code'],
            err.response['Error']['Message']
        )
        raise


def persist_dag_ended(end_state: str, partition_key: str, dag_run_id: str):
    attributes = {PROCESSING_STATE_ATTRIBUTE_NAME: end_state, 'stop_timestamp': datetime.utcnow().isoformat()}

    if is_prod:
        update_item_attributes(partition_key, dag_run_id, attributes)
    else:
        logging.info(f'Non prod environment. DAG "{end_state}".\n'
                     f'pk:{partition_key}, sk:{dag_run_id}, attributes:{attributes}')


def update_items_processing_state(items, status):
    for item in items:
        pk = item[PARTITION_KEY_ATTRIBUTE_NAME]
        sk = item[SORT_KEY_ATTRIBUTE_NAME]
        attributes = {
            PROCESSING_STATE_ATTRIBUTE_NAME: status,
        }
        if is_prod:
            update_item_attributes(pk, sk, attributes)
        else:
            logging.info(f'Non prod environment".\n'
                         f'pk:{pk}, sk:{sk}, attributes:{attributes}')


def query_dynamodb_items(partition_key: str,
                         last_evaluated_key=None,
                         projection_expression=None,
                         filter_conditions=None) -> Tuple[List[Dict[str, str]], str]:
    """
    Queries the data-subject-requests Dynamodb Table
    @param partition_key: partition key we are querying on
    @param last_evaluated_key: last evaluated key to start querying from
    @param projection_expression: attributes we want to return.  Format needs to be as such 'attribute1, attribute2, attribute3'
    @param filter_conditions: filter_conditions: a list of tuples - attribute names with their corresponding values we want to filter on.
        e.g. [('processing_state': 'failed')]
    @return: Returns a list of dynamodb items represented by a dictionary
    """
    items = []
    try:
        query_params = {
            "KeyConditionExpression": Key(PARTITION_KEY_ATTRIBUTE_NAME).eq(partition_key),
        }
        if filter_conditions:
            filter_expression = None
            for attribute, value in filter_conditions:
                condition = Attr(attribute).eq(value)
                if filter_expression is None:
                    filter_expression = condition
                else:
                    filter_expression &= condition
            query_params['FilterExpression'] = filter_expression
        if projection_expression:
            query_params['ProjectionExpression'] = projection_expression
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key
        response = DynamoDbClient().query(**query_params)
    except ClientError as err:
        logging.error(
            f"Couldn't query for pk {partition_key}. "
            f"Error: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
        )
        raise

    # Append items from the current response to the list of items
    items.extend(response['Items'])

    # Update last_evaluated_key for pagination
    last_evaluated_key = response.get('LastEvaluatedKey')

    return items, last_evaluated_key


def scan_dynamodb_items(filter_conditions=None) -> List[Dict[str, str]]:
    """
    Cans the data-subject-requests Dynamodb Table given the filters
    @param filter_conditions: a list of tuples - attribute names with their corresponding values we want to filter on.
        e.g. [('processing_state': 'failed')]
    @return: Returns a list of dynamodb items represented by a dictionary
    """
    items = []
    last_evaluated_key = None
    while True:
        try:
            scan_params = {}
            if last_evaluated_key:
                scan_params['ExclusiveStartKey'] = last_evaluated_key
            if filter_conditions:
                filter_expression = None
                for attribute, value in filter_conditions:
                    condition = Attr(attribute).eq(value)
                    if filter_expression is None:
                        filter_expression = condition
                    else:
                        filter_expression &= condition
                scan_params['FilterExpression'] = filter_expression
            response = DynamoDbClient().scan(**scan_params)
        except ClientError as err:
            logging.error(
                f"Couldn't scan for filter conditions {filter_conditions}. "
                f"Error: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
            )
            raise

        # Append items from the current response to the list of items
        items.extend(response['Items'])

        # Update last_evaluated_key for pagination
        last_evaluated_key = response.get('LastEvaluatedKey')

        # Check if there are more items to retrieve
        if not last_evaluated_key:
            break

    return items


def query_processing_state_gsi_dynamodb_items() -> List[Dict[str, str]]:
    items = []
    last_evaluated_key = None
    while True:
        try:
            query_params = {
                'IndexName': PROCESSING_STATE_GSI,
                'KeyConditionExpression': Key('processing_state').eq(PARTNER_DSR_PROCESSING_READY_FOR_CLOUD)
            }
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key
            response = DynamoDbClient().query(**query_params)
        except ClientError as err:
            logging.error(
                f"Couldn't query on {PROCESSING_STATE_GSI} GSI with Key equal to {PARTNER_DSR_PROCESSING_READY_FOR_CLOUD}. "
                f"Error: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
            )
            raise

        # Append items from the current response to the list of items
        items.extend(response['Items'])

        # Update last_evaluated_key for pagination
        last_evaluated_key = response.get('LastEvaluatedKey')

        # Check if there are more items to retrieve
        if not last_evaluated_key:
            break

    return items


def write_db_records(db_items):
    if is_prod:
        write_batch(db_items)
    else:
        logging.info(f'Items to be persisted in prod - Items:{db_items}')


# This is used so we can check the environment and the tests still pass
def do_update_item_attributes(partition_key, sort_key, attributes):
    if is_prod:
        update_item_attributes(partition_key, sort_key, attributes)
    else:
        logging.info(f'Item attributes to be updated in prod - (pk:{partition_key}, sk:{sort_key})={attributes}')
