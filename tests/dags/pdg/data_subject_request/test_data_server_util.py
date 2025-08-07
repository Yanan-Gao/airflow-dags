# import json
# import unittest
# from unittest.mock import patch, Mock, call
#
# from airflow import AirflowException
#
#
# def compute_hmac_sha1(api_key, json_body):
#     pass
#
#
# class TestDataServerUtil(unittest.TestCase):
#
#     @patch('src.dags.pdg.data_subject_request.util.secrets_manager_util.get_secrets')
#     def test_compute_hmac_sha1(self, mock_get_secrets):
#         # Mock secret
#         api_key = 'your_api_key'
#         mock_get_secrets.return_value = {'API_KEY': 'your_api_key'}
#         json_body = '{"key": "value"}'
#
#         # Expected result obtained from a known HMAC-SHA1 computation
#         expected_result = 'wxDvEIzHhMqv6PQN2joZExFNyNU='
#
#         # Call the compute_hmac_sha1 method
#         actual_result = compute_hmac_sha1(api_key, json_body)
#
#         # Assert that the actual result matches the expected result
#         self.assertEqual(expected_result, actual_result)
#
#     @patch('requests.post')
#     @patch('src.dags.pdg.data_subject_request.util.secrets_manager_util.get_secrets')
#     def test_send_internal_data_server_import_request(self, mock_get_secrets, mock_post):
#         # Mock secret
#         api_key = 'your_api_key'
#         mock_get_secrets.return_value = {'API_KEY': 'your_api_key'}
#
#         # Mock the response object
#         mock_response = Mock()
#         mock_response.status_code = 200
#
#         # Set the mock return value for requests.post
#         mock_post.return_value = mock_response
#
#         # Call the function
#         body = {'key': 'value'}
#         send_internal_data_server_import_request(body, 'https://example.com')
#
#         expected_headers = {
#             'Content-Type': 'application/json',
#             'TtdDataset': 'DataSubjectRequest',
#             'TtdSignature': compute_hmac_sha1(api_key, json.dumps(body))
#         }
#
#         # Assert that requests.post was called with the expected arguments
#         mock_post.assert_called_once_with(url='https://example.com', headers=expected_headers, json=json.dumps(body))
#
#     @patch('requests.post')
#     @patch('src.dags.pdg.data_subject_request.util.secrets_manager_util.get_secrets')
#     @patch('src.dags.pdg.data_subject_request.util.data_server_util.max_request_size_bytes',
#            1)
#     def test_send_internal_data_server_import_request_batching(self, mock_get_secrets, mock_post):
#         # Mock secret
#         api_key = 'your_api_key'
#         mock_get_secrets.return_value = {'API_KEY': 'your_api_key'}
#         # Mock data and URL
#         body = {
#             "AdvertiserId": "yourAdvertiserId",
#             "Items": [
#                 {"TDID": "123e4567-e89b-12d3-a456-426652340000", "Data": [{"Name": "1210"}]},
#                 {"TDID": "123e4567-e89b-12d3-a456-426652340001", "Data": [{"Name": "1211"}]},
#             ]
#         }
#         url = "https://example.com/api"
#
#         # Mock the response from requests.post
#         mock_post.return_value.status_code = 200
#
#         # Call the function under test
#         result = send_internal_data_server_import_request(body, url)
#
#         # Assertions
#         self.assertTrue(result)
#         expected_headers = {
#             'Content-Type': 'application/json',
#             'TtdDataset': 'DataSubjectRequest',
#             'TtdSignature': compute_hmac_sha1(api_key, json.dumps(body))
#         }
#         mock_post.has_calls(
#             [
#                 call('https://example.com/api',
#                      headers=expected_headers,
#                      json={'AdvertiserId': 'yourAdvertiserId', 'Items': [
#                          {'TDID': '123e4567-e89b-12d3-a456-426652340000', 'Data': [{'Name': '1210'}]}]}),
#                 call('https://example.com/api',
#                      headers=expected_headers,
#                      json={'AdvertiserId': 'yourAdvertiserId', 'Items': [
#                          {'TDID': '123e4567-e89b-12d3-a456-426652340001', 'Data': [{'Name': '1211'}]}]})]
#         )
#
#     @patch('requests.post')
#     @patch('src.dags.pdg.data_subject_request.util.secrets_manager_util.get_secrets')
#     @patch('src.dags.pdg.data_subject_request.util.data_server_util.max_request_size_bytes',
#            1)
#     def test_send_internal_data_server_import_request_batching_third_party(self, mock_get_secrets, mock_post):
#         # Mock secret
#         api_key = 'your_api_key'
#         mock_get_secrets.return_value = {'API_KEY': 'your_api_key'}
#         # Mock data and URL
#         body = {
#             "DataProviderId": "dataProviderId",
#             "Items": [
#                 {"TDID": "123e4567-e89b-12d3-a456-426652340000", "Data": [{"Name": "1210"}]},
#                 {"TDID": "123e4567-e89b-12d3-a456-426652340001", "Data": [{"Name": "1211"}]},
#             ]
#         }
#         url = "https://example.com/api"
#
#         # Mock the response from requests.post
#         mock_post.return_value.status_code = 200
#
#         # Call the function under test
#         result = send_internal_data_server_import_request(body, url)
#
#         # Assertions
#         self.assertTrue(result)
#         expected_headers = {
#             'Content-Type': 'application/json',
#             'TtdDataset': 'DataSubjectRequest',
#             'TtdSignature': compute_hmac_sha1(api_key, json.dumps(body))
#         }
#         mock_post.has_calls(
#             [
#                 call('https://example.com/api',
#                      headers=expected_headers,
#                      json={'DataProviderId': 'dataProviderId', 'Items': [
#                          {'TDID': '123e4567-e89b-12d3-a456-426652340000', 'Data': [{'Name': '1210'}]}]}),
#                 call('https://example.com/api',
#                      headers=expected_headers,
#                      json={'DataProviderId': 'dataProviderId', 'Items': [
#                          {'TDID': '123e4567-e89b-12d3-a456-426652340001', 'Data': [{'Name': '1211'}]}]})]
#         )
#
#     @patch('src.dags.pdg.data_subject_request.util.secrets_manager_util.get_secrets')
#     @patch('requests.post')  # Mock the requests.post function
#     def test_send_internal_data_server_import_request_failure(self, mock_post, mock_get_secrets):
#         # Mock secret
#         mock_get_secrets.return_value = {'API_KEY': 'your_api_key'}
#
#         # Mock the response object
#         mock_response = Mock()
#         mock_response.status_code = 400  # Set an error status code
#
#         # Set the mock return value for requests.post
#         mock_post.return_value = mock_response
#
#         # Call the function
#         body = {'key': 'value'}
#
#         self.assertFalse(send_internal_data_server_import_request(body, 'https://example.com'))
#
#     def test_get_type_of_request_data_advertiser(self):
#         request_body = {'advertiserId': 'yourAdvertiserId'}
#         result = get_type_of_request_data(request_body)
#
#         expected_result = DataServerRequestType(partner_key='AdvertiserId',
#                                                 partner_value='yourAdvertiserId',
#                                                 url='https://use-ltd-data-int.adsrvr.org/data/internalAdvertiserDataImport',
#                                                 get_segments_callable=sql_util.query_advertiser_segments)
#
#         self.assertEqual(result, expected_result)
#
#     def test_get_type_of_request_data_third_party(self):
#         request_body = {'dataProviderId': 'yourDataProviderId'}
#         result = get_type_of_request_data(request_body)
#
#         expected_result = DataServerRequestType(partner_key='DataProviderId',
#                                                 partner_value='yourDataProviderId',
#                                                 url='https://use-ltd-data-int.adsrvr.org/data/internalThirdPartyDataImport',
#                                                 get_segments_callable=sql_util.query_third_party_segments)
#
#         self.assertEqual(result, expected_result)
#
#     def test_get_type_of_request_data_third_party_when_empty_advertiser_id(self):
#         request_body = {'advertiserId': '', 'dataProviderId': 'yourDataProviderId'}
#         result = get_type_of_request_data(request_body)
#
#         expected_result = DataServerRequestType(partner_key='DataProviderId',
#                                                 partner_value='yourDataProviderId',
#                                                 url='https://use-ltd-data-int.adsrvr.org/data/internalThirdPartyDataImport',
#                                                 get_segments_callable=sql_util.query_third_party_segments)
#
#         self.assertEqual(result, expected_result)
#
#     def test_get_type_of_request_data_unknown(self):
#         request_body = {'UnknownKey': 'unknownValue'}
#
#         with self.assertRaises(AirflowException):
#             get_type_of_request_data(request_body)
#
#     def test_get_type_of_request_data_empty_advertiser_and_data_provider_id(self):
#         request_body = {'advertiserId': '', 'dataProviderId': ''}
#
#         with self.assertRaises(AirflowException):
#             get_type_of_request_data(request_body)
#
#     @patch(
#         'src.dags.pdg.data_subject_request.util.data_server_util.send_internal_data_server_import_request')
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.query_advertiser_segments')
#     # @patch('src.dags.pdg.data_subject_request.util.sql_util.query_third_party_segments')
#     def test_make_data_server_request(self, mock_sql_query_get_advertiser_segments,
#                                       mock_send_internal_data_server_import_request):
#         items = [
#             {
#                 'advertiserId': '123',
#                 'userIdGuid': 'abc',
#                 'rawUserId': 'xyz',
#                 'userIdType': 'Tdid'
#             },
#             {
#                 'advertiserId': '123',
#                 'userIdGuid': 'cba',
#                 'rawUserId': 'lol',
#                 'userIdType': 'IdentityLinkId'
#             },
#         ]
#
#         # Create a mock function for get_segments_callable
#         mock_sql_query_get_advertiser_segments.side_effect = [
#             [123, 432],
#             [543],
#             []
#         ]
#
#         mock_send_internal_data_server_import_request.return_value = False
#
#         # Call the function
#         response = make_data_server_request(items)
#
#         expected_calls = [
#             call(
#                 {
#                     "AdvertiserId": '123',
#                     "Items": [
#                         {
#                             "Tdid": 'abc',
#                             "Data": [
#                                 {
#                                     "Name": "123",
#                                     "TTLInMinutes": 0
#                                 },
#                                 {
#                                     "Name": "432",
#                                     "TTLInMinutes": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "IDL": 'lol',
#                             "Data": [
#                                 {
#                                     "Name": "123",
#                                     "TTLInMinutes": 0
#                                 },
#                                 {
#                                     "Name": "432",
#                                     "TTLInMinutes": 0
#                                 }
#                             ]
#                         }
#                     ]
#                 }, 'https://use-ltd-data-int.adsrvr.org/data/internalAdvertiserDataImport'),
#             call(
#                 {
#                     'AdvertiserId': '123',
#                     'Items': [
#                         {
#                             'Data': [
#                                 {
#                                     'Name': '543',
#                                     'TTLInMinutes': 0
#                                 }
#                             ],
#                             'Tdid': 'abc'
#                         },
#                         {
#                             'Data': [
#                                 {
#                                     'Name': '543',
#                                     'TTLInMinutes': 0
#                                 }
#                             ],
#                             'IDL': 'lol'}]
#                 }, 'https://use-ltd-data-int.adsrvr.org/data/internalAdvertiserDataImport'),
#         ]
#
#         # Assertions
#         mock_sql_query_get_advertiser_segments.assert_has_calls(
#             [
#                 call('123', 0, 1000),
#                 call('123', 1000, 1000),
#                 call('123', 2000, 1000),
#             ])
#         mock_send_internal_data_server_import_request.assert_has_calls(expected_calls)
#         self.assertTrue(response)
#
#     @patch(
#         'src.dags.pdg.data_subject_request.util.data_server_util.send_internal_data_server_import_request')
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.query_advertiser_segments')
#     # @patch('src.dags.pdg.data_subject_request.util.sql_util.query_third_party_segments')
#     def test_make_data_server_request_fails(self, mock_sql_query_get_advertiser_segments,
#                                       mock_send_internal_data_server_import_request):
#         items = [
#             {
#                 'advertiserId': '123',
#                 'userIdGuid': 'abc',
#                 'rawUserId': 'xyz',
#                 'userIdType': 'Tdid'
#             },
#             {
#                 'advertiserId': '123',
#                 'userIdGuid': 'cba',
#                 'rawUserId': 'lol',
#                 'userIdType': 'IdentityLinkId'
#             },
#         ]
#
#         # Create a mock function for get_segments_callable
#         mock_sql_query_get_advertiser_segments.side_effect = [
#             [123, 432],
#             [543],
#             []
#         ]
#
#         mock_send_internal_data_server_import_request.return_value = True
#
#         # Call the function
#         response = make_data_server_request(items)
#
#         expected_calls = [
#             call(
#                 {
#                     "AdvertiserId": '123',
#                     "Items": [
#                         {
#                             "Tdid": 'abc',
#                             "Data": [
#                                 {
#                                     "Name": "123",
#                                     "TTLInMinutes": 0
#                                 },
#                                 {
#                                     "Name": "432",
#                                     "TTLInMinutes": 0
#                                 }
#                             ]
#                         },
#                         {
#                             "IDL": 'lol',
#                             "Data": [
#                                 {
#                                     "Name": "123",
#                                     "TTLInMinutes": 0
#                                 },
#                                 {
#                                     "Name": "432",
#                                     "TTLInMinutes": 0
#                                 }
#                             ]
#                         }
#                     ]
#                 }, 'https://use-ltd-data-int.adsrvr.org/data/internalAdvertiserDataImport'),
#             call(
#                 {
#                     'AdvertiserId': '123',
#                     'Items': [
#                         {
#                             'Data': [
#                                 {
#                                     'Name': '543',
#                                     'TTLInMinutes': 0
#                                 }
#                             ],
#                             'Tdid': 'abc'
#                         },
#                         {
#                             'Data': [
#                                 {
#                                     'Name': '543',
#                                     'TTLInMinutes': 0
#                                 }
#                             ],
#                             'IDL': 'lol'}]
#                 }, 'https://use-ltd-data-int.adsrvr.org/data/internalAdvertiserDataImport'),
#         ]
#
#         # Assertions
#         mock_sql_query_get_advertiser_segments.assert_has_calls(
#             [
#                 call('123', 0, 1000),
#                 call('123', 1000, 1000),
#                 call('123', 2000, 1000),
#             ])
#         mock_send_internal_data_server_import_request.assert_has_calls(expected_calls)
#         self.assertFalse(response)
#
#     @patch(
#         'src.dags.pdg.data_subject_request.util.data_server_util.send_internal_data_server_import_request')
#     @patch('src.dags.pdg.data_subject_request.util.data_server_util.build_request_body')
#     @patch('logging.info')
#     def test_empty_data_returns_early(self, mock_logging,
#                                       mock_build_request_body, mock_send_internal_data_server_import_request):
#         # Mock the build_request_body function to return a request body with empty 'Data'
#         request_body = {
#             "DataProviderId": "dataProviderId",
#             "Items": [
#                 {
#                     "Tdid": "f4330862-f944-43c3-8963-158fdf8274eb",
#                     "Data": []
#                 }
#             ]}
#         mock_build_request_body.return_value = request_body, False
#
#         # Call the function with empty 'Data'
#         response = make_data_server_request([
#             {
#                 'dataProviderId': 'dataProviderId',
#                 'userIdType': 'Tdid',
#                 'userIdGuid': 'f4330862-f944-43c3-8963-158fdf8274eb',
#                 'rawUserId': ''
#             }
#         ])
#
#         # Assert that the function returns early
#         mock_logging.assert_called_once_with(
#             f'No items or data for request: {request_body}.  Not submitting DataServer request.')
#         mock_send_internal_data_server_import_request.assert_not_called()
#         self.assertTrue(response)
