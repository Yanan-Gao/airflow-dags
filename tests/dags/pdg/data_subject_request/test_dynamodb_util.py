import unittest
from unittest.mock import Mock

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from dags.pdg.data_subject_request.util import dsr_dynamodb_util


class TestDynamodbUtil(unittest.TestCase):

    def test_update_item_attributes(self):
        partition_key = 'partition_key'
        sort_key = 'sort_key'

        dsr_dynamodb_util.DynamoDbClient._table = Mock()

        # Call the function under test
        dsr_dynamodb_util.update_item_attributes(partition_key, sort_key, {'atr1': 'one', 'atr2': 2})

        # Assert that the DynamoDbClient.update_item method was called with the correct parameters
        dsr_dynamodb_util.DynamoDbClient().update_item.assert_called_once_with(
            Key={
                dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: partition_key,
                dsr_dynamodb_util.SORT_KEY_ATTRIBUTE_NAME: sort_key
            },
            UpdateExpression='SET #N_atr1 = :V_atr1, #N_atr2 = :V_atr2',
            ExpressionAttributeNames={
                '#N_atr1': 'atr1',
                '#N_atr2': 'atr2'
            },
            ExpressionAttributeValues={
                ':V_atr1': 'one',
                ':V_atr2': 2
            },
            ReturnValues='NONE'
        )

    def test_query_items_success(self):
        dsr_dynamodb_util.DynamoDbClient._table = Mock()
        partition_key = 'some_partition_key'
        expected_items = [{
            dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: 'pk1',
            'name': 'Item 1'
        }, {
            dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: 'pk2',
            'name': 'Item 2'
        }]
        dsr_dynamodb_util.DynamoDbClient().query.return_value = {'Items': expected_items}

        actual_items = dsr_dynamodb_util.query_dynamodb_items(partition_key)

        self.assertEqual(actual_items, (expected_items, None))
        dsr_dynamodb_util.DynamoDbClient().query.assert_called_once_with(
            KeyConditionExpression=Key(dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME).eq(partition_key)
        )

    def test_query_items_for_update_success(self):
        dsr_dynamodb_util.DynamoDbClient._table = Mock()
        partition_key = 'some_partition_key'
        expected_items = [{
            dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: 'pk1',
            'name': 'Item 1'
        }, {
            dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: 'pk2',
            'name': 'Item 2'
        }]
        dsr_dynamodb_util.DynamoDbClient().query.return_value = {'Items': expected_items}

        actual_response = dsr_dynamodb_util.query_dynamodb_items(
            partition_key, None, f'{dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME}, '
            f'{dsr_dynamodb_util.SORT_KEY_ATTRIBUTE_NAME}'
        )

        dsr_dynamodb_util.DynamoDbClient().query.assert_called_once_with(
            KeyConditionExpression=Key(dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME).eq(partition_key),
            ProjectionExpression=f'{dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME}, {dsr_dynamodb_util.SORT_KEY_ATTRIBUTE_NAME}'
        )
        self.assertEqual(actual_response, (expected_items, None))

    def test_query_items_returns_last_evaluated_key(self):
        dsr_dynamodb_util.DynamoDbClient._table = Mock()
        partition_key = 'some_partition_key'
        expected_items = [{'id': 1}, {'id': 2}, {'id': 3}]
        dsr_dynamodb_util.DynamoDbClient().query.side_effect = [
            {
                'Items': expected_items[:2],
                'LastEvaluatedKey': {
                    dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: partition_key
                }
            },
            {
                'Items': expected_items[2:]
            },
        ]

        actual_response = dsr_dynamodb_util.query_dynamodb_items(partition_key)

        self.assertEqual(actual_response, (expected_items[:2], {dsr_dynamodb_util.PARTITION_KEY_ATTRIBUTE_NAME: partition_key}))
        self.assertEqual(dsr_dynamodb_util.DynamoDbClient().query.call_count, 1)

    def test_query_items_client_error(self):
        dsr_dynamodb_util.DynamoDbClient._table = Mock()
        partition_key = 'some_partition_key'
        error_code = 'ResourceNotFoundException'
        error_message = 'Table not found'
        dsr_dynamodb_util.DynamoDbClient().query.side_effect = ClientError({'Error': {
            'Code': error_code,
            'Message': error_message
        }}, 'query')

        with self.assertRaises(ClientError) as cm:
            dsr_dynamodb_util.query_dynamodb_items(partition_key)

        self.assertEqual(cm.exception.response['Error']['Code'], error_code)
        self.assertEqual(cm.exception.response['Error']['Message'], error_message)
