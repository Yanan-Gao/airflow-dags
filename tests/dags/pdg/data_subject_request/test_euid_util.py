# TODO fix
# import unittest
# from unittest.mock import Mock, call
#
# from airflow.exceptions import AirflowException
#
# from src.dags.pdg.data_subject_request.util.uid2_euid_service import Response, Id
# from src.dags.pdg.data_subject_request.util.euid_util import emails_to_euids_to_tdids
# from src.dags.pdg.data_subject_request.util import euid_util, dsr_dynamodb_util, tdid_hash
# from ttd.ttdenv import TtdEnvFactory
#
#
# class TestEuidUtil(unittest.TestCase):
#
#     # Tests that the function successfully calls the EUID service,
#     # updates the database with EUID and TDID for each email in the input and updates XCom values.
#     def test_happy_path(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{
#             'email1': {
#                 'db_sort_key': 'sort_key1',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'uid2',
#                     'uid2_salt_bucket': 'salt_bucket',
#                     'tdid': 'tdid'
#                 }]
#             },
#             'email2': {
#                 'db_sort_key': 'sort_key2',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'uid2_2',
#                     'uid2_salt_bucket': 'salt_bucket_2',
#                     'tdid': 'tdid_2'
#                 }]
#             }
#         }, 'request_id', 'prev_id_a,prev_id_b']
#         task_instance.task_id = 'task_id'
#
#         euid_client = Mock()
#         euid_util.create_client = lambda: euid_client
#         euid_client.get_identity_map.return_value = Response(
#             status_code=200,
#             email_to_ids_mapping={
#                 'email1': Id(advertising_id='euid1', salt_bucket='salt_bucket1'),
#                 'email2': Id(advertising_id='euid2', salt_bucket='salt_bucket2')
#             },
#         )
#
#         update_item_attributes_mock = Mock()
#         dsr_dynamodb_util.update_item_attributes = update_item_attributes_mock
#
#         # Call the function
#         emails_to_euids_to_tdids(task_instance=task_instance)
#
#         # Assertions
#         # Updates emails_and_identifiers and uiids_string
#         task_instance.xcom_push.assert_has_calls([
#             call(
#                 key='emails_and_identifiers',
#                 value={
#                     'email1': {
#                         'db_sort_key':
#                         'sort_key1',
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'uid2',
#                             'uid2_salt_bucket': 'salt_bucket',
#                             'tdid': 'tdid'
#                         }, {
#                             'type': 'euid',
#                             'euid': 'euid1',
#                             'euid_salt_bucket': 'salt_bucket1',
#                             'tdid': '76cd5b8a-fc25-f3ba-e35c-eedcddfad4c6'
#                         }]
#                     },
#                     'email2': {
#                         'db_sort_key':
#                         'sort_key2',
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'uid2_2',
#                             'uid2_salt_bucket': 'salt_bucket_2',
#                             'tdid': 'tdid_2'
#                         }, {
#                             'type': 'euid',
#                             'euid': 'euid2',
#                             'euid_salt_bucket': 'salt_bucket2',
#                             'tdid': '5a33dbf9-1c45-c9f2-619d-cc642c68d338'
#                         }]
#                     }
#                 }
#             ),
#             call(
#                 key='uiids_string',
#                 value='prev_id_a,prev_id_b,euid1,76cd5b8a-fc25-f3ba-e35c-eedcddfad4c6,'
#                 'euid2,5a33dbf9-1c45-c9f2-619d-cc642c68d338'
#             )
#         ])
#
#         # Updates DynamoDB records
#         update_item_attributes_mock.assert_has_calls([
#             call(
#                 'request_id', 'sort_key1', {
#                     'task_id': True,
#                     'euid_tdid': 'euid1#76cd5b8a-fc25-f3ba-e35c-eedcddfad4c6',
#                     'euid_salt_bucket': 'salt_bucket1'
#                 }
#             ),
#             call(
#                 'request_id', 'sort_key2', {
#                     'task_id': True,
#                     'euid_tdid': 'euid2#5a33dbf9-1c45-c9f2-619d-cc642c68d338',
#                     'euid_salt_bucket': 'salt_bucket2'
#                 }
#             )
#         ])
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
#         euid_client = Mock()
#         euid_util.create_client = lambda: euid_client
#         euid_client.get_identity_map.return_value = Response(status_code=400, email_to_ids_mapping=dict(), encrypted=Mock())
#
#         update_item_attributes_mock = Mock()
#         dsr_dynamodb_util.update_item_attributes = update_item_attributes_mock
#
#         # Call the function
#         with self.assertRaises(AirflowException):
#             emails_to_euids_to_tdids(task_instance=task_instance)
#
#         update_item_attributes_mock.assert_not_called()
#         task_instance.xcom_push.assert_not_called()
#
#     def test_200_euid_service_with_no_mappings_returns_false(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{'email1': {}, 'email2': {}}, 'request_id', ""]
#         task_instance.task_id = 'task_id'
#
#         euid_client = Mock()
#         euid_util.create_client = lambda: euid_client
#         euid_client.get_identity_map.return_value = Response(status_code=200, email_to_ids_mapping=dict(), encrypted=Mock())
#
#         update_item_attributes_mock = Mock()
#         dsr_dynamodb_util.update_item_attributes = update_item_attributes_mock
#
#         # Call the function
#         self.assertFalse(emails_to_euids_to_tdids(task_instance=task_instance))
#
#         update_item_attributes_mock.assert_not_called()
#         task_instance.xcom_push.assert_not_called()
#
#     def test_euid_service_with_some_email_mappings_sets_opt_out_appropriately_returns_true(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         request_id = 'request_id'
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{
#             'email1': {
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'optout',
#                     'uid2_salt_bucket': 'optout',
#                     'tdid': request_id
#                 }],
#                 'db_sort_key': f'optout#{request_id}',
#             },
#             'email2': {
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'uid2_1',
#                     'uid2_salt_bucket': 'uid2_salt_bucket',
#                     'tdid': 'tdid_from_uid2'
#                 }],
#                 'db_sort_key': 'uid2_1#tdid_from_uid2',
#             }
#         }, request_id, 'prev_id_a,prev_id_b']
#         task_instance.task_id = 'task_id'
#
#         euid_client = Mock()
#         euid_util.create_client = lambda: euid_client
#         euid_value = 'euid_1'
#         euid_tdid = tdid_hash.tdid_hash(euid_value)
#         euid_client.get_identity_map.return_value = Response(
#             status_code=200, email_to_ids_mapping={'email1': Id(advertising_id=euid_value, salt_bucket='euid_salt_bucket')}, encrypted=None
#         )
#
#         update_item_attributes_mock = Mock()
#         dsr_dynamodb_util.update_item_attributes = update_item_attributes_mock
#
#         # Call the function
#         result = emails_to_euids_to_tdids(task_instance=task_instance)
#
#         self.assertTrue(result)
#
#         # Assertions
#         # Updates emails_and_identifiers and uiids_string
#         task_instance.xcom_push.assert_has_calls([
#             call(
#                 key='emails_and_identifiers',
#                 value={
#                     'email1': {
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'optout',
#                             'uid2_salt_bucket': 'optout',
#                             'tdid': request_id
#                         }, {
#                             'type': 'euid',
#                             'euid': euid_value,
#                             'euid_salt_bucket': 'euid_salt_bucket',
#                             'tdid': f'{euid_tdid}'
#                         }],
#                         'db_sort_key':
#                         'optout#request_id',
#                     },
#                     'email2': {
#                         'identifiers': [{
#                             'type': 'uid2',
#                             'uid2': 'uid2_1',
#                             'uid2_salt_bucket': 'uid2_salt_bucket',
#                             'tdid': 'tdid_from_uid2'
#                         }, {
#                             'type': 'euid',
#                             'euid': 'optout',
#                             'euid_salt_bucket': 'optout',
#                             'tdid': f'{request_id}'
#                         }],
#                         'db_sort_key':
#                         'uid2_1#tdid_from_uid2'
#                     }
#                 }
#             ),
#             call(key='uiids_string', value=f'prev_id_a,prev_id_b,{euid_value},{euid_tdid}')
#         ])
#
#         # Updates DynamoDB records
#         update_item_attributes_mock.assert_has_calls([
#             call(
#                 request_id, f'optout#{request_id}', {
#                     'task_id': True,
#                     'euid_tdid': f'{euid_value}#{euid_tdid}',
#                     'euid_salt_bucket': 'euid_salt_bucket'
#                 }
#             ),
#             call(request_id, 'uid2_1#tdid_from_uid2', {
#                 'task_id': True,
#                 'euid_tdid': 'optout#request_id',
#                 'euid_salt_bucket': 'optout'
#             }),
#         ])
#
#     def test_euid_service_with_no_email_mappings_sets_opt_out_appropriately_but_has_previous_mappings_returns_true(self):
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#         # Prepare the necessary inputs
#         task_instance = Mock()
#         task_instance.xcom_pull.side_effect = [{
#             'email1': {
#                 'db_sort_key': 'sort_key1',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'optout',
#                 }]
#             },
#         }, 'request_id', 'prev_id_a,prev_id_b']
#         task_instance.task_id = 'task_id'
#
#         euid_client = Mock()
#         euid_util.create_client = lambda: euid_client
#         euid_client.get_identity_map.return_value = Response(status_code=200, email_to_ids_mapping={})
#
#         update_item_attributes_mock = Mock()
#         dsr_dynamodb_util.update_item_attributes = update_item_attributes_mock
#
#         # Call the function
#         result = emails_to_euids_to_tdids(task_instance=task_instance)
#
#         self.assertTrue(result)
#
#         # Assertions
#         # Updates emails_and_identifiers and uiids_string
#         task_instance.xcom_push.assert_not_called()
#         update_item_attributes_mock.assert_not_called()
