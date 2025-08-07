# TODO fix
# import unittest
# from unittest.mock import Mock, call
#
# from airflow import AirflowException
#
# from src.dags.pdg.data_subject_request.util.dsr_dynamodb_util import \
#     PARTITION_KEY_ATTRIBUTE_NAME, SORT_KEY_ATTRIBUTE_NAME
# from src.dags.pdg.data_subject_request.util.uid2_euid_service import Response, Id
# from src.dags.pdg.data_subject_request.util.uid2_util import emails_to_uid2s_to_tdids
# from src.dags.pdg.data_subject_request.util import uid2_util, dsr_dynamodb_util
# from ttd.ttdenv import TtdEnvFactory
#
#
# class TestUid2Util(unittest.TestCase):
#
#     # Tests that the function successfully calls the UID2 service,
#     # updates the database with UID2 and TDID for each email in the input and updates XCom values.
#     def test_happy_path(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{'email1': {}, 'email2': {}}, 'request_id']
#         task_instance.task_id = 'task_id'
#
#         uid2_client = Mock()
#         uid2_util.create_client = lambda: uid2_client
#         uid2_client.get_identity_map.return_value = Response(
#             status_code=200,
#             email_to_ids_mapping={
#                 'email1': Id(advertising_id='uid2_1', salt_bucket='salt_bucket1'),
#                 'email2': Id(advertising_id='uid2_2', salt_bucket='salt_bucket2')
#             },
#             encrypted=None
#         )
#
#         write_batch_mock = Mock()
#         dsr_dynamodb_util.write_batch = write_batch_mock
#
#         # Call the function
#         emails_to_uid2s_to_tdids(task_instance=task_instance)
#
#         # Assertions
#         # Updates emails_and_identifiers and uiids_string
#         task_instance.xcom_push.assert_has_calls([
#             call(
#                 key='emails_and_identifiers',
#                 value={
#                     'email1': {
#                         'db_sort_key':
#                         'uid2_1#9374c691-3085-fab6-3ba7-cf031be96c80',
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'uid2_1',
#                             'uid2_salt_bucket': 'salt_bucket1',
#                             'tdid': '9374c691-3085-fab6-3ba7-cf031be96c80'
#                         }]
#                     },
#                     'email2': {
#                         'db_sort_key':
#                         'uid2_2#ad5af77e-a437-8036-3090-aa9daa57fdbf',
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'uid2_2',
#                             'uid2_salt_bucket': 'salt_bucket2',
#                             'tdid': 'ad5af77e-a437-8036-3090-aa9daa57fdbf'
#                         }]
#                     }
#                 }
#             ),
#             call(key='uiids_string', value='uid2_1,9374c691-3085-fab6-3ba7-cf031be96c80,uid2_2,ad5af77e-a437-8036-3090-aa9daa57fdbf')
#         ])
#
#         # Writes DynamoDB records
#         write_batch_mock.assert_called_once_with([{
#             PARTITION_KEY_ATTRIBUTE_NAME: 'request_id',
#             SORT_KEY_ATTRIBUTE_NAME: 'uid2_1#9374c691-3085-fab6-3ba7-cf031be96c80',
#             'task_id': True,
#             'uid2_salt_bucket': 'salt_bucket1'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'request_id',
#             SORT_KEY_ATTRIBUTE_NAME: 'uid2_2#ad5af77e-a437-8036-3090-aa9daa57fdbf',
#             'task_id': True,
#             'uid2_salt_bucket': 'salt_bucket2'
#         }])
#
#     def test_non_200_euid_service_raises_error(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{
#             'email1': {
#                 'db_sort_key': 'sort_key1'
#             },
#             'email2': {
#                 'db_sort_key': 'sort_key2'
#             }
#         }, 'request_id', 'prev_id_a,prev_id_b']
#         task_instance.task_id = 'task_id'
#
#         uid2_client = Mock()
#         uid2_util.create_client = lambda: uid2_client
#         uid2_client.get_identity_map.return_value = Response(status_code=400, email_to_ids_mapping=dict(), encrypted=Mock())
#
#         write_batch_mock = Mock()
#         dsr_dynamodb_util.write_batch = write_batch_mock
#
#         # Call the function
#         with self.assertRaises(AirflowException):
#             emails_to_uid2s_to_tdids(task_instance=task_instance)
#
#         write_batch_mock.assert_not_called()
#
#     def test_200_euid_service_with_no_mappings_returns_false(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{'email1': {}, 'email2': {}}, 'request_id', 'prev_id_a,prev_id_b']
#         task_instance.task_id = 'task_id'
#
#         uid2_client = Mock()
#         uid2_util.create_client = lambda: uid2_client
#         uid2_client.get_identity_map.return_value = Response(status_code=200, email_to_ids_mapping=dict(), encrypted=Mock())
#
#         update_item_attributes_mock = Mock()
#         dsr_dynamodb_util.update_item_attributes = update_item_attributes_mock
#
#         # Call the function
#         emails_to_uid2s_to_tdids(task_instance=task_instance)
#
#         update_item_attributes_mock.assert_not_called()
#         task_instance.xcom_push.assert_not_called()
#
#     def test_uid2_service_with_some_email_mappings_sets_opt_out_appropriately_returns_true(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         request_id = 'request_id'
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{'email1': {}, 'email2': {}}, request_id]
#         task_instance.task_id = 'task_id'
#
#         uid2_client = Mock()
#         uid2_util.create_client = lambda: uid2_client
#         uid2_client.get_identity_map.return_value = Response(
#             status_code=200, email_to_ids_mapping={'email1': Id(advertising_id='uid2_1', salt_bucket='salt_bucket1')}, encrypted=None
#         )
#
#         write_batch_mock = Mock()
#         dsr_dynamodb_util.write_batch = write_batch_mock
#
#         # Call the function
#         emails_to_uid2s_to_tdids(task_instance=task_instance)
#
#         # Assertions
#         # Write emails_and_identifiers and uiids_string
#         task_instance.xcom_push.assert_has_calls([
#             call(
#                 key='emails_and_identifiers',
#                 value={
#                     'email1': {
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'uid2_1',
#                             'uid2_salt_bucket': 'salt_bucket1',
#                             'tdid': '9374c691-3085-fab6-3ba7-cf031be96c80'
#                         }],
#                         'db_sort_key':
#                         'uid2_1#9374c691-3085-fab6-3ba7-cf031be96c80',
#                     },
#                     'email2': {
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'optout',
#                             'uid2_salt_bucket': 'optout',
#                             'tdid': request_id
#                         }],
#                         'db_sort_key': f'optout#{request_id}',
#                     }
#                 }
#             ),
#             call(key='uiids_string', value='uid2_1,9374c691-3085-fab6-3ba7-cf031be96c80')
#         ])
#
#         # Writes DynamoDB records
#         write_batch_mock.assert_called_once_with([
#             {
#                 PARTITION_KEY_ATTRIBUTE_NAME: 'request_id',
#                 SORT_KEY_ATTRIBUTE_NAME: 'uid2_1#9374c691-3085-fab6-3ba7-cf031be96c80',
#                 'task_id': True,
#                 'uid2_salt_bucket': 'salt_bucket1'
#             },
#             {
#                 PARTITION_KEY_ATTRIBUTE_NAME: 'request_id',
#                 SORT_KEY_ATTRIBUTE_NAME: f'optout#{request_id}',
#                 'task_id': True,
#                 'uid2_salt_bucket': 'optout'
#             },
#         ])
