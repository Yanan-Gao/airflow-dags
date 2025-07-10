# TODO fix
# import unittest
# from typing import List
# from unittest import mock
# from unittest.mock import Mock, patch, call
#
# from airflow import AirflowException
#
# from src.dags.pdg.data_subject_request.handler.partner_dsr_handler import \
#     process_open_partner_dsr_requests_for_opt_out, save_tenant_id_and_partner_id, write_to_s3_enriched, \
#     map_user_id_type_to_value, process_partner_dsr_requests_ready_for_cloud_processing
# from src.dags.pdg.data_subject_request.util.dsr_dynamodb_util import \
#     PARTITION_KEY_ATTRIBUTE_NAME, SORT_KEY_ATTRIBUTE_NAME, PARTNER_DSR_TIMESTAMP, PARTNER_DSR_REQUEST_ID, \
#     PARTNER_DSR_REQUEST_DATA, PARTNER_DSR_ADVERTISER_ID, PARTNER_DSR_DATA_PROVIDER_ID, PARTNER_DSR_USER_ID_GUID, \
#     PARTNER_DSR_USER_ID_RAW, PARTNER_DSR_USER_ID_TYPE, PARTNER_DSR_PROCESSING_READY_FOR_OPT_OUT
#
#
# class TestPartnerDsrHandler(unittest.TestCase):
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.retrieve_open_partner_dsr_requests_from_s3')
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.write_batch')
#     @patch('logging.info')
#     def test_process_open_partner_dsr_requests_for_opt_out_with_requests(
#         self, mock_logging_info, mock_write_batch, mock_get_objects_to_process
#     ):
#         # Mocking objects to process
#         mock_request = {
#             PARTNER_DSR_TIMESTAMP: 'timestamp',
#             PARTNER_DSR_REQUEST_ID: 'requestId',
#             PARTNER_DSR_REQUEST_DATA: 'partnerDsrRequestData',
#             PARTNER_DSR_ADVERTISER_ID: 'advertiserId',
#             PARTNER_DSR_DATA_PROVIDER_ID: 'dataProviderId',
#             PARTNER_DSR_USER_ID_GUID: 'tdid_guid',
#             PARTNER_DSR_USER_ID_RAW: 'raw_user_id',
#             PARTNER_DSR_USER_ID_TYPE: 'Tdid',
#         }
#         mock_get_objects_to_process.return_value = [mock_request]
#
#         # Mocking TaskInstance
#         mock_task_instance = Mock()
#         mock_task_instance.return_value = mock_task_instance
#
#         # Call the function
#         result = process_open_partner_dsr_requests_for_opt_out(mock_task_instance)
#
#         # Assertions
#         self.assertTrue(result)
#         mock_get_objects_to_process.assert_called_once()
#         mock_write_batch.assert_called_once()
#         # Asserting on arguments passed to mock_write_batch
#         expected_dynamodb_items = [{
#             PARTITION_KEY_ATTRIBUTE_NAME: mock.ANY,  # Any UUID string
#             SORT_KEY_ATTRIBUTE_NAME: f"{mock_request['advertiserId']}#{mock_request['dataProviderId']}#{mock_request['userIdGuid']}",
#             'processing_state': PARTNER_DSR_PROCESSING_READY_FOR_OPT_OUT,
#             **mock_request
#         }]
#         mock_write_batch.assert_called_once_with(expected_dynamodb_items, True)
#         expected_calls = [
#             call(key='request_id', value=mock.ANY)  # Any UUID string
#         ]
#         mock_task_instance.xcom_push.assert_has_calls(expected_calls)
#         mock_logging_info.assert_not_called()
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.retrieve_open_partner_dsr_requests_from_s3')
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.write_batch')
#     @patch('logging.info')
#     def test_process_open_partner_dsr_requests_for_opt_out_with_no_advertiser_id(
#         self, mock_logging_info, mock_write_batch, mock_get_objects_to_process
#     ):
#         # Mocking objects to process
#         mock_request = {
#             'timestamp': 'timestamp',
#             'requestId': 'requestId',
#             'partnerDsrRequestData': 'partnerDsrRequestData',
#             'advertiserId': '',
#             'dataProviderId': 'dataProviderId',
#             'userIdGuid': 'userIdGuid',
#             'rawUserId': 'rawUserId',
#             'userIdType': 'userIdType',
#         }
#         mock_get_objects_to_process.return_value = [mock_request]
#
#         # Mocking TaskInstance
#         mock_task_instance = Mock()
#         mock_task_instance.return_value = mock_task_instance
#
#         # Call the function
#         result = process_open_partner_dsr_requests_for_opt_out(mock_task_instance)
#
#         # Assertions
#         self.assertTrue(result)
#         mock_get_objects_to_process.assert_called_once()
#         mock_write_batch.assert_called_once()
#         # Asserting on arguments passed to mock_write_batch
#         expected_dynamodb_items = [{
#             PARTITION_KEY_ATTRIBUTE_NAME: mock.ANY,  # Any UUID string
#             SORT_KEY_ATTRIBUTE_NAME: f"{mock_request['dataProviderId']}#{mock_request['userIdGuid']}",
#             'processing_state': PARTNER_DSR_PROCESSING_READY_FOR_OPT_OUT,
#             **mock_request
#         }]
#         mock_write_batch.assert_called_once_with(expected_dynamodb_items, True)
#         expected_calls = [
#             call(key='request_id', value=mock.ANY),  # Any UUID string
#         ]
#         mock_task_instance.xcom_push.assert_has_calls(expected_calls)
#         mock_logging_info.assert_not_called()
#
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.retrieve_open_partner_dsr_requests_from_s3')
#     @patch('logging.info')
#     def test_process_open_partner_dsr_requests_for_opt_out_no_requests(self, mock_logging_info, mock_get_objects_to_process):
#         # Mocking no objects to process
#         mock_get_objects_to_process.return_value = []
#
#         # Mocking TaskInstance
#         mock_task_instance = Mock()
#         mock_task_instance.return_value = mock_task_instance
#
#         # Call the function
#         result = process_open_partner_dsr_requests_for_opt_out(mock_task_instance)
#
#         # Assertions
#         self.assertFalse(result)
#         mock_get_objects_to_process.assert_called_once()
#         mock_logging_info.assert_called_once_with('No requests to process.')
#
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.query_processing_state_gsi_dynamodb_items')
#     def test_process_partner_dsr_requests_ready_for_cloud_processing_with_requests(self, mock_query_dynamodb_items):
#         mock_items = [{'pk': 'pk1', 'sk': 'sk1'}, {'pk': 'pk2', 'sk': 'sk2'}]
#         mock_query_dynamodb_items.return_value = mock_items
#         mock_task_instance = Mock()
#
#         result = process_partner_dsr_requests_ready_for_cloud_processing(mock_task_instance)
#
#         mock_query_dynamodb_items.assert_called_once()
#
#         expected_calls = [
#             call(key='batch_request_id', value=mock.ANY),  # Any UUID string
#             call(key='partner_dsr_requests', value=mock_items),
#         ]
#         mock_task_instance.xcom_push.assert_has_calls(expected_calls)
#         self.assertTrue(result)
#
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.query_processing_state_gsi_dynamodb_items')
#     @patch('logging.info')
#     def test_process_partner_dsr_requests_ready_for_cloud_processing_no_requests(self, mock_logging_info, mock_query_dynamodb_items):
#         mock_items: List[str] = []
#         mock_query_dynamodb_items.return_value = mock_items
#         mock_task_instance = Mock()
#
#         result = process_partner_dsr_requests_ready_for_cloud_processing(mock_task_instance)
#
#         mock_query_dynamodb_items.assert_called_once()
#
#         mock_task_instance.xcom_push.assert_not_called()
#         mock_logging_info.assert_called_once_with('No requests to process.')
#         self.assertFalse(result)
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.query_tenant_id')
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.update_item')
#     def test_save_tenant_id_and_partner_id(self, mock_update_item, mock_query_tenant_id):
#         xcom_pull_result = [{
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk1',
#             'advertiserId': 'example_advertiser_id1',
#             'dataProviderId': 'example_data_provider_id1'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk2',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk2',
#             'advertiserId': 'example_advertiser_id2'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk3',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk3',
#             'dataProviderId': 'example_data_provider_id1'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk4',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk4',
#             'advertiserId': '',
#             'dataProviderId': 'example_data_provider_id2'
#         }]
#
#         # Mock the TaskInstance and its xcom_pull method
#         task_instance = Mock()
#         task_instance.xcom_pull.return_value = xcom_pull_result
#
#         # Mock the query result from sql_util.query_tenant_id
#         tenant_list = [{
#             'advertiserId': 'example_advertiser_id1',
#             'tenantId': 'example_tenant_id1',
#             'partnerId': 'example_partner_id'
#         }, {
#             'advertiserId': 'example_advertiser_id2',
#             'tenantId': 'example_tenant_id2'
#         }, {
#             'dataProviderId': 'example_data_provider_id1',
#             'tenantId': 'example_tenant_id3'
#         }, {
#             'dataProviderId': 'example_data_provider_id2',
#             'tenantId': 'example_tenant_id4'
#         }]
#
#         # Mock the dsr_dynamodb_util.query_dynamodb_items and sql_util.query_tenant_id functions
#         mock_query_tenant_id.return_value = tenant_list
#
#         # Call the function
#         result = save_tenant_id_and_partner_id(task_instance)
#
#         # Assert that the function returned True
#         self.assertTrue(result)
#
#         # Assert that the update_item method was called with the expected arguments
#         expected_calls = [
#             call({
#                 PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#                 SORT_KEY_ATTRIBUTE_NAME: 'example_sk1',
#                 'advertiserId': 'example_advertiser_id1',
#                 'dataProviderId': 'example_data_provider_id1',
#                 'tenantId': 'example_tenant_id1',
#                 'partnerId': 'example_partner_id'
#             }),
#             call({
#                 PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk2',
#                 SORT_KEY_ATTRIBUTE_NAME: 'example_sk2',
#                 'advertiserId': 'example_advertiser_id2',
#                 'tenantId': 'example_tenant_id2'
#             }),
#             call({
#                 PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk3',
#                 SORT_KEY_ATTRIBUTE_NAME: 'example_sk3',
#                 'dataProviderId': 'example_data_provider_id1',
#                 'tenantId': 'example_tenant_id3'
#             }),
#             call({
#                 PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk4',
#                 SORT_KEY_ATTRIBUTE_NAME: 'example_sk4',
#                 'dataProviderId': 'example_data_provider_id2',
#                 'tenantId': 'example_tenant_id4'
#             })
#         ]
#         mock_update_item.assert_has_calls(expected_calls)
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.query_tenant_id')
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.update_item')
#     @patch('logging.error')
#     def test_save_tenant_id_when_tenant_list_is_empty(self, mock_logging, mock_update_item, mock_query_tenant_id):
#         xcom_pull = [{
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk1',
#             'advertiserId': 'example_advertiser_id1',
#             'dataProviderId': 'example_data_provider_id1'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk2',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk2',
#             'advertiserId': 'example_advertiser_id2'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk3',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk3',
#             'dataProviderId': 'example_data_provider_id2'
#         }]
#
#         # Mock the TaskInstance and its xcom_pull method
#         task_instance = Mock()
#         task_instance.xcom_pull.return_value = xcom_pull
#
#         # Mock the query result from sql_util.query_tenant_id
#         tenant_list: List[str] = []
#
#         mock_query_tenant_id.return_value = tenant_list
#
#         # Call the function
#         result = save_tenant_id_and_partner_id(task_instance)
#
#         self.assertFalse(result)
#         mock_logging.assert_called_once_with('No tenant information found for items.  Check xcom for partner requests')
#         mock_update_item.assert_not_called()
#
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.query_processing_state_gsi_dynamodb_items')
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.put_enriched_object')
#     @patch('io.StringIO')
#     @patch('csv.writer')
#     @patch('logging.info')
#     def test_write_to_s3_enriched_successful_write(
#         self, mock_logging, mock_csv_writer, mock_string_io, mock_put_enriched_object,
#         mock_dsr_dynamodb_util_query_processing_state_gsi_dynamodb_items
#     ):
#         # Mock the TaskInstance and its xcom_pull method
#         mock_task_instance = Mock()
#
#         # Mock dsr_dynamodb_util.query_dynamodb_items
#         mock_items = [{
#             'pk': 'pk1',
#             'sk': 'sk1',
#             'advertiserId': '123',
#             'dataProviderId': '456',
#             'userIdGuid': 'abc',
#             'rawUserId': 'xyz',
#             'userIdType': 'type1',
#             'partnerId': 'partnerId',
#             'tenantId': 'tenantId',
#         }, {
#             'pk': 'pk2',
#             'sk': 'sk2',
#             'advertiserId': '123',
#             'dataProviderId': '456',
#             'userIdGuid': 'cba',
#             'rawUserId': 'lol',
#             'userIdType': 'type1',
#             'partnerId': 'partnerId',
#             'tenantId': 'tenantId',
#         }, {
#             'pk': 'pk3',
#             'sk': 'sk3',
#             'dataProviderId': '654',
#             'userIdGuid': 'abc',
#             'rawUserId': 'xyz',
#             'userIdType': 'type1',
#             'partnerId': 'partnerId',
#             'tenantId': 'tenantId',
#         }]
#         mock_dsr_dynamodb_util_query_processing_state_gsi_dynamodb_items.return_value = mock_items
#
#         # Mock csv_file.getvalue
#         csv_content = 'advertiserId,name\n123,example\n'
#         mock_string_io_instance = Mock()
#         mock_string_io.return_value = mock_string_io_instance
#         mock_string_io_instance.getvalue.return_value = csv_content
#
#         # Call the function
#         result = write_to_s3_enriched(mock_task_instance)
#
#         # Assertions
#         mock_task_instance.xcom_pull.assert_has_calls([
#             call(key='batch_request_id'),
#         ])
#         mock_string_io.assert_called_once()
#         mock_csv_writer_instance = mock_csv_writer.return_value
#
#         expected_calls = [
#             call(['advertiserId', 'name']),
#             call(['123', 'example']),
#         ]
#         mock_csv_writer_instance.writerow.has_calls(expected_calls)
#         mock_string_io_instance.getvalue.assert_called_once()
#         mock_put_enriched_object.assert_called_once_with(csv_content.encode())
#         mock_logging.assert_not_called()
#         self.assertTrue(result)
#
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.query_processing_state_gsi_dynamodb_items')
#     @patch('src.dags.pdg.data_subject_request.util.s3_utils.put_enriched_object')
#     @patch('logging.info')
#     def test_write_to_s3_enriched_no_advertiser_items(
#         self, mock_logging, mock_put_enriched_object, mock_dsr_dynamodb_util_query_processing_state_gsi_dynamodb_items
#     ):
#         # Mock the TaskInstance and its xcom_pull method
#         mock_task_instance = Mock()
#
#         # Mock dsr_dynamodb_util.query_dynamodb_items
#         mock_items = [{
#             'pk': 'pk3',
#             'sk': 'sk3',
#             'dataProviderId': '654',
#             'userIdGuid': 'abc',
#             'rawUserId': 'xyz',
#             'userIdType': 'type1',
#             'partnerId': 'partnerId',
#             'tenantId': 'tenantId',
#         }]
#         mock_dsr_dynamodb_util_query_processing_state_gsi_dynamodb_items.return_value = mock_items
#         # Act
#         write_to_s3_enriched(mock_task_instance)
#
#         # Assert
#         mock_logging.assert_called_once()
#         mock_put_enriched_object.assert_not_called()
#         self.assertFalse(mock_put_enriched_object.called)
#
#     # @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.query_dynamodb_items')
#     # @patch('src.dags.pdg.data_subject_request.util.data_server_util.make_data_server_request')
#     # @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.update_item_attributes')
#     # def test_remove_users_from_segments(
#     #     self, mock_update_item_attributes, mock_make_data_server_request, mock_dsr_dynamodb_util_query_items
#     # ):
#     #     # Mock the TaskInstance and its xcom_pull method
#     #     mock_task_instance = Mock()
#     #     request_id = 'request_id'
#     #     mock_task_instance.xcom_pull.return_value = request_id
#     #
#     #     # Mock dsr_dynamodb_util.query_dynamodb_items
#     #     mock_items = [{
#     #         'pk': 'pk1',
#     #         'sk': 'sk1',
#     #         'advertiserId': '123',
#     #         'dataProviderId': '456',
#     #         'userIdGuid': 'abc',
#     #         'rawUserId': 'xyz',
#     #         'userIdType': 'type1'
#     #     }, {
#     #         'pk': 'pk2',
#     #         'sk': 'sk2',
#     #         'advertiserId': '123',
#     #         'dataProviderId': '456',
#     #         'userIdGuid': 'cba',
#     #         'rawUserId': 'lol',
#     #         'userIdType': 'type1'
#     #     }, {
#     #         'pk': 'pk3',
#     #         'sk': 'sk3',
#     #         'dataProviderId': '654',
#     #         'userIdGuid': 'abc',
#     #         'rawUserId': 'xyz',
#     #         'userIdType': 'type1'
#     #     }]
#     #     mock_dsr_dynamodb_util_query_items.return_value = mock_items, None
#     #     mock_make_data_server_request.return_value = False
#     #
#     #     # Call the function
#     #     result = remove_users_from_segments(mock_task_instance)
#     #
#     #     # Assertions
#     #     mock_task_instance.xcom_pull.assert_called_once_with(key='request_id')
#     #     mock_dsr_dynamodb_util_query_items.assert_called_once_with(
#     #         'request_id', None, 'pk, sk, advertiserId, '
#     #         'dataProviderId, userIdGuid, '
#     #         'rawUserId, userIdType', [(PROCESSING_STATE_ATTRIBUTE_NAME, PARTNER_DSR_PROCESSING_READY_FOR_OPT_OUT)]
#     #     )
#     #     expected_calls = [
#     #         call([{
#     #             'advertiserId': '123',
#     #             'dataProviderId': '456',
#     #             'userIdGuid': 'abc',
#     #             'rawUserId': 'xyz',
#     #             'userIdType': 'type1'
#     #         }, {
#     #             'advertiserId': '123',
#     #             'dataProviderId': '456',
#     #             'userIdGuid': 'cba',
#     #             'rawUserId': 'lol',
#     #             'userIdType': 'type1'
#     #         }]),
#     #         call([{
#     #             'dataProviderId': '654',
#     #             'userIdGuid': 'abc',
#     #             'rawUserId': 'xyz',
#     #             'userIdType': 'type1'
#     #         }]),
#     #     ]
#     #     mock_make_data_server_request.has_calls(expected_calls)
#     #
#     #     mock_update_item_attributes.assert_has_calls([
#     #         call('pk1', 'sk1', {
#     #             PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_OPT_OUT,
#     #         }),
#     #         call('pk2', 'sk2', {
#     #             PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_OPT_OUT,
#     #         }),
#     #         call('pk3', 'sk3', {
#     #             PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_OPT_OUT,
#     #         }),
#     #         call('pk1', 'sk1', {
#     #             PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_READY_FOR_CLOUD,
#     #         }),
#     #         call('pk2', 'sk2', {
#     #             PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_READY_FOR_CLOUD,
#     #         }),
#     #         call('pk3', 'sk3', {
#     #             PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_READY_FOR_CLOUD,
#     #         })
#     #     ])
#     #     self.assertTrue(result)
#
#     def test_map_user_id_type_to_value(self):
#         # Test for Tdid
#         self.assertEqual(map_user_id_type_to_value("Tdid", "GUID1", ""), "GUID1")
#
#         # Test for UnifiedId2
#         self.assertEqual(map_user_id_type_to_value("UnifiedId2", "", "Raw1"), "Raw1")
#
#         # Test for EUID
#         self.assertEqual(map_user_id_type_to_value("EUID", "", "Raw2"), "Raw2")
#
#         # Test for invalid input
#         with self.assertRaisesRegex(AirflowException, "are not present"):
#             map_user_id_type_to_value("", "", "")
