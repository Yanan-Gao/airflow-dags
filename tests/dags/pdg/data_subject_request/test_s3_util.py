# TODO fix
# import gzip
# import unittest
# from unittest.mock import Mock, patch
#
# from botocore.exceptions import ClientError
#
# from src.dags.pdg.data_subject_request.util.s3_utils import \
#     retrieve_open_partner_dsr_requests_from_s3, \
#     get_all_object_keys, put_enriched_object, partner_dsr_enriched_prefix, enriched_csv_bucket, \
#     generate_dated_folder_path, dsr_notification_bucket, dsr_notification_prefix, put_dsr_notification
#
#
# class TestS3Util(unittest.TestCase):
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.S3Hook')
#     def test_get_all_object_keys(self, mock_s3_hook):
#         # Mocking S3 client
#         s3_client_mock = Mock()
#         mock_s3_hook.return_value.get_conn.return_value = s3_client_mock
#
#         # Mocking paginator
#         paginator_mock = Mock()
#         paginator_mock.paginate.return_value = [{"Contents": [{"Key": "prefix/object1.txt"}, {"Key": "prefix/object2.txt"}]}]
#         s3_client_mock.get_paginator.return_value = paginator_mock
#
#         # Call the function
#         keys = get_all_object_keys(s3_client_mock, 'prefix')
#
#         # Assertions
#         self.assertEqual(keys, ['prefix/object1.txt', 'prefix/object2.txt'])
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.S3Hook')
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.get_all_object_keys')
#     def test_retrieve_open_partner_dsr_requests_from_s3_happy_path(self, mock_get_all_object_keys, mock_s3_hook):
#         # Mocking S3 client
#         s3_client_mock = Mock()
#         mock_s3_hook.return_value.get_conn.return_value = s3_client_mock
#
#         # Mocking the return value of get_all_object_keys_recursively
#         mock_get_all_object_keys.return_value = ['prefix/object1.txt']
#
#         # Mocking response for get_object
#         response_mock = {'Body': Mock()}
#         response_mock['Body'].read.return_value = gzip.compress(
#             b'timestamp\trequestId\tpartnerDsrRequestData\tadvertiserId\tdataProviderId\tuserIdGuid\trawUserId\tuserIdType\n'
#         )
#
#         s3_client_mock.get_object.return_value = response_mock
#
#         # Call the function
#         parsed_values = retrieve_open_partner_dsr_requests_from_s3()
#
#         # Assertions
#         self.assertEqual(len(parsed_values), 1)  # expect one even though we query for 7 days because they are duplicates
#         expected_parsed_value = {
#             'timestamp': 'timestamp',
#             'requestId': 'requestId',
#             'partnerDsrRequestData': 'partnerDsrRequestData',
#             'advertiserId': 'advertiserId',
#             'dataProviderId': 'dataProviderId',
#             'userIdGuid': 'userIdGuid',
#             'rawUserId': 'rawUserId',
#             'userIdType': 'userIdType'
#         }
#         self.assertEqual(parsed_values[0], expected_parsed_value)
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.S3Hook')
#     def test_put_enriched_object(self, mock_s3_hook):
#         # Mock S3Hook
#         mock_s3_client = Mock()
#         mock_s3_hook.return_value.get_conn.return_value = mock_s3_client
#
#         folder_name = generate_dated_folder_path(partner_dsr_enriched_prefix)
#
#         # Call the function
#         expected_csv_content = 'mock_csv_content'.encode()
#         put_enriched_object(expected_csv_content)
#
#         # Assertions
#         mock_s3_client.put_object.assert_called_once_with(
#             Body=expected_csv_content, Bucket=enriched_csv_bucket, Key=f'{folder_name}/advertiser_data_subject_requests.csv'
#         )
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.S3Hook')
#     @patch('boto3.client')
#     @patch('logging.error')
#     def test_put_enriched_object_exception_handling(self, mock_logging, mock_boto3_client, mock_s3_hook):
#         # Mock objects
#         mock_s3_hook_instance = mock_s3_hook.return_value
#         mock_s3_client = mock_s3_hook_instance.get_conn.return_value
#         mock_sts_client = mock_boto3_client.return_value
#
#         # Set the return value for get_caller_identity
#         mock_sts_object = {'Arn': 'mock_arn'}
#         mock_sts_client.get_caller_identity.return_value = mock_sts_object
#
#         # Exception to raise
#         exception_message = "Simulated error"
#         mock_s3_client.put_object.side_effect = ClientError({'Error': {
#             'Code': 'MockErrorCode',
#             'Message': exception_message
#         }}, 'operation_name')
#
#         # CSV content
#         csv_content = b'Sample,CSV,content'
#
#         # Call the function
#         with self.assertRaises(ClientError) as context:
#             put_enriched_object(csv_content)
#
#         folder_name = generate_dated_folder_path(partner_dsr_enriched_prefix)
#         key = f'{folder_name}/advertiser_data_subject_requests.csv'
#
#         mock_s3_hook_instance.get_conn.assert_called_once()
#         mock_s3_client.put_object.assert_called_once_with(Body=csv_content, Bucket=enriched_csv_bucket, Key=key)
#         expected_logging_call = f"Caller Identity: {mock_sts_object}\n" \
#                                 f"Couldn't put enriched object CSV: Sample,CSV,content\n" \
#                                 f"data into bucket ttd-data-subject-requests with the following path {key}.\n" \
#                                 f"Error code: MockErrorCode and Error response: {exception_message}"
#         mock_logging.assert_called_once_with(expected_logging_call)
#         mock_sts_client.get_caller_identity.assert_called_once()
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.S3Hook')
#     def test_post_dsr_notification(self, mock_s3_hook):
#         # Mock S3Hook
#         mock_s3_client = Mock()
#         mock_s3_hook.return_value.get_conn.return_value = mock_s3_client
#
#         folder_name = generate_dated_folder_path(dsr_notification_prefix)
#         key = f'{folder_name}/data_subject_delete_request_ids.csv'
#
#         # Call the function
#         expected_csv_content = 'mock_csv_content'.encode()
#         put_dsr_notification(expected_csv_content)
#
#         # Assertions
#         mock_s3_client.put_object.assert_called_once_with(Body=expected_csv_content, Bucket=dsr_notification_bucket, Key=key)
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.S3Hook')
#     @patch('boto3.client')
#     @patch('logging.error')
#     def test_post_dsr_notification_writes_correctly(self, mock_logging, mock_boto3_client, mock_s3_hook):
#         # Mock objects
#         mock_s3_hook_instance = mock_s3_hook.return_value
#         mock_s3_client = mock_s3_hook_instance.get_conn.return_value
#         mock_sts_client = mock_boto3_client.return_value
#
#         # Set the return value for get_caller_identity
#         mock_sts_object = {'Arn': 'mock_arn'}
#         mock_sts_client.get_caller_identity.return_value = mock_sts_object
#
#         # Exception to raise
#         exception_message = "Simulated error"
#         mock_s3_client.put_object.side_effect = ClientError({'Error': {
#             'Code': 'MockErrorCode',
#             'Message': exception_message
#         }}, 'operation_name')
#
#         # CSV content
#         csv_content = b'Sample,CSV,content'
#
#         # Call the function
#         with self.assertRaises(ClientError) as context:
#             put_dsr_notification(csv_content)
#
#         folder_name = generate_dated_folder_path(dsr_notification_prefix)
#         key = f'{folder_name}/data_subject_delete_request_ids.csv'
#
#         mock_s3_hook_instance.get_conn.assert_called_once()
#         mock_s3_client.put_object.assert_called_once_with(Body=csv_content, Bucket=dsr_notification_bucket, Key=key)
#         expected_logging_call = f"Caller Identity: {mock_sts_object}\n" \
#                                 f"Couldn't put dsr notification object CSV: Sample,CSV,content\n" \
#                                 f"data into bucket ttd-data-subject-requests-notification with the following path {key}.\n" \
#                                 f"Error code: MockErrorCode and Error response: {exception_message}"
#         mock_logging.assert_called_once_with(expected_logging_call)
#         mock_sts_client.get_caller_identity.assert_called_once()
