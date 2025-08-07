# TODO fix
# import json
# import unittest
# from unittest.mock import patch, Mock
#
# from src.dags.pdg.data_subject_request.util.uid2_euid_service import UID2AndEUIDClient, \
#     Id
#
#
# class TestUID2AndEUIDService(unittest.TestCase):
#
#     def test_get_identity_map(self):
#         with patch('requests.post') as requests_post:
#             client = UID2AndEUIDClient(api_key="api_key", api_secret="api_secret", base_url='uidservice.com')
#
#             client._encrypt = lambda payload: f'encrypted:{payload}'
#             client._decrypt = lambda content: json.loads(content[len('encrypted:'):])
#
#             mock_response = Mock()
#             mock_response.status_code = 200
#             mock_response.content = 'encrypted:' + json.dumps({
#                 'body': {
#                     'mapped': [{
#                         'identifier': 'someone@ttd.com',
#                         'advertising_id': 'someid',
#                         'bucket_id': 'somesalt'
#                     }],
#                     'status': 'success'
#                 }
#             })
#             requests_post.return_value = mock_response
#
#             response = client.get_identity_map(['someone@ttd.com'])
#
#             requests_post.assert_called_once_with(
#                 'uidservice.com/v2/identity/map',
#                 data='encrypted:{"email":["someone@ttd.com"]}',
#                 headers={'Authorization': 'Bearer api_key'}
#             )
#
#             self.assertEqual(200, response.status_code)
#             self.assertDictEqual({'someone@ttd.com': Id('someid', 'somesalt')}, response.email_to_ids_mapping)
#
#     def test_get_identity_map_with_unmapped(self):
#         with patch('requests.post') as requests_post:
#             client = UID2AndEUIDClient(api_key="api_key", api_secret="api_secret", base_url='uidservice.com')
#
#             client._encrypt = lambda payload: f'encrypted:{payload}'
#             client._decrypt = lambda content: json.loads(content[len('encrypted:'):])
#
#             mock_response = Mock()
#             mock_response.status_code = 200
#             mock_response.content = 'encrypted:' + json.dumps({
#                 'body': {
#                     'mapped': [],
#                     'unmapped': [{
#                         'identifier': 'blah@ttd.com',
#                         'reason': 'optout'
#                     }],
#                     'status': 'success'
#                 }
#             })
#             requests_post.return_value = mock_response
#
#             actual_response = client.get_identity_map(['blah@ttd.com'])
#             self.assertEqual(200, actual_response.status_code)
#             self.assertEqual({}, actual_response.email_to_ids_mapping)
