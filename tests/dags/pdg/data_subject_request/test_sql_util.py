# TODO fix
# import unittest
# from textwrap import dedent
# from typing import List
# from unittest.mock import patch, Mock, call
#
# from airflow import AirflowException
# from pymssql._mssql import MSSQLDatabaseException
#
# import src.dags.pdg.data_subject_request.util.sql_util as sql_util
#
#
# class TestSqlUtil(unittest.TestCase):
#
#     @patch('ttd.ttdenv.is_prod', autospec=True)
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_persist_to_ttdglobal_deleterequest(self, mock_mssql_hook, mock_is_prod):
#         # Mock dependencies
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#         mock_connection = Mock()
#         mock_cursor = Mock()
#         mock_hook_instance.get_conn.return_value = mock_connection
#         mock_connection.cursor.return_value = mock_cursor
#         mock_task_instance = Mock()
#
#         # Set up task_instance and emails_and_identifiers data
#         uid2_1 = "uid2_1"
#         euid_1, euid_2 = "euid_1", "euid_2"
#         mock_task_instance.xcom_pull.return_value = {
#             'email1': {
#                 'jira_ticket_number':
#                 'TICKET-01',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': uid2_1,
#                     'uid2_salt_bucket': f'{uid2_1}-salt-bucket',
#                     'tdid': f'tdid-from-{uid2_1}'
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_1}',
#                     'euid_salt_bucket': f'{euid_1}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_1}'
#                 }]
#             },
#             'email2': {
#                 'jira_ticket_number':
#                 'TICKET-02',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'optout',
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_2}',
#                     'euid_salt_bucket': f'{euid_2}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_2}'
#                 }]
#             }
#         }
#
#         # Call the function
#         sql_util.persist_to_ttdglobal_deleterequest(mock_task_instance)
#
#         mock_hook_instance.get_conn.assert_called_once()
#
#         # Assert the SQL query
#         expected_sql_1 = dedent(
#             f"""
#             exec support.prc_AddDeletionRequest
#             @TDID = 'tdid-from-{uid2_1}',
#             @TicketNumber = 'TICKET-01',
#             @RawValue = '{uid2_1}',
#             @SaltBucket = '{uid2_1}-salt-bucket',
#             @IdentitySourceId = 3
#             """
#         )
#         expected_sql_2 = dedent(
#             f"""
#             exec support.prc_AddDeletionRequest
#             @TDID = 'tdid-from-{euid_1}',
#             @TicketNumber = 'TICKET-01',
#             @RawValue = '{euid_1}',
#             @SaltBucket = '{euid_1}-salt-bucket',
#             @IdentitySourceId = 5
#             """
#         )
#         expected_sql_3 = dedent(
#             f"""
#             exec support.prc_AddDeletionRequest
#             @TDID = 'tdid-from-{euid_2}',
#             @TicketNumber = 'TICKET-02',
#             @RawValue = '{euid_2}',
#             @SaltBucket = '{euid_2}-salt-bucket',
#             @IdentitySourceId = 5
#             """
#         )
#
#         mock_cursor.execute.assert_has_calls([call(expected_sql_1), call(expected_sql_2), call(expected_sql_3)])
#
#     @patch('ttd.ttdenv.is_prod', autospec=True)
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_persist_to_ttdglobal_deleterequest_when_duplicate_key_exception(self, mock_mssql_hook, mock_is_prod):
#         # Mock dependencies
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#         mock_connection = Mock()
#         mock_cursor = Mock()
#         mock_hook_instance.get_conn.return_value = mock_connection
#         mock_connection.cursor.return_value = mock_cursor
#         mock_task_instance = Mock()
#
#         # Set up task_instance and emails_and_identifiers data
#         uid2_1 = "uid2_1"
#         euid_1, euid_2 = "euid_1", "euid_2"
#         mock_task_instance.xcom_pull.return_value = {
#             'email1': {
#                 'jira_ticket_number':
#                 'TICKET-01',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': uid2_1,
#                     'uid2_salt_bucket': f'{uid2_1}-salt-bucket',
#                     'tdid': f'tdid-from-{uid2_1}'
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_1}',
#                     'euid_salt_bucket': f'{euid_1}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_1}'
#                 }]
#             },
#             'email2': {
#                 'jira_ticket_number':
#                 'TICKET-02',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'optout',
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_2}',
#                     'euid_salt_bucket': f'{euid_2}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_2}'
#                 }]
#             }
#         }
#
#         # Create a mock MSSQLDatabaseException
#         mock_cursor.execute.side_effect = MSSQLDatabaseException(
#             "Violation of PRIMARY KEY constraint "
#             "'pk_DeletionRequest_TDID'.  Cannot insert duplicate key "
#             "in object 'dbo.DeletionRequest'. The duplicate key value "
#             "is (4c87419e-7422-2a1e-29c0-a8cc566e68f1) .DB-Lib error "
#             "message 20018, severity 14:\nGeneral SQL Server error: "
#             "Check messages from the SQL Server"
#         )
#
#         # Call the function
#         sql_util.persist_to_ttdglobal_deleterequest(mock_task_instance)
#
#         mock_hook_instance.get_conn.assert_called_once()
#
#         # Assert the SQL query
#         expected_sql_1 = dedent(
#             f"""
#                 exec support.prc_AddDeletionRequest
#                 @TDID = 'tdid-from-{uid2_1}',
#                 @TicketNumber = 'TICKET-01',
#                 @RawValue = '{uid2_1}',
#                 @SaltBucket = '{uid2_1}-salt-bucket',
#                 @IdentitySourceId = 3
#                 """
#         )
#         expected_sql_2 = dedent(
#             f"""
#                 exec support.prc_AddDeletionRequest
#                 @TDID = 'tdid-from-{euid_1}',
#                 @TicketNumber = 'TICKET-01',
#                 @RawValue = '{euid_1}',
#                 @SaltBucket = '{euid_1}-salt-bucket',
#                 @IdentitySourceId = 5
#                 """
#         )
#         expected_sql_3 = dedent(
#             f"""
#                 exec support.prc_AddDeletionRequest
#                 @TDID = 'tdid-from-{euid_2}',
#                 @TicketNumber = 'TICKET-02',
#                 @RawValue = '{euid_2}',
#                 @SaltBucket = '{euid_2}-salt-bucket',
#                 @IdentitySourceId = 5
#                 """
#         )
#
#         mock_cursor.execute.assert_has_calls([call(expected_sql_1), call(expected_sql_2), call(expected_sql_3)])
#
#     @patch('ttd.ttdenv.is_prod', autospec=True)
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_persist_to_ttdglobal_deleterequest_unknown_error(self, mock_mssql_hook, mock_is_prod):
#         # Mock dependencies
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#         mock_connection = Mock()
#         mock_cursor = Mock()
#         mock_hook_instance.get_conn.return_value = mock_connection
#         mock_connection.cursor.return_value = mock_cursor
#         mock_task_instance = Mock()
#
#         # Set up task_instance and emails_and_identifiers data
#         uid2_1 = "uid2_1"
#         euid_1, euid_2 = "euid_1", "euid_2"
#         mock_task_instance.xcom_pull.return_value = {
#             'email1': {
#                 'jira_ticket_number':
#                 'TICKET-01',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': uid2_1,
#                     'uid2_salt_bucket': f'{uid2_1}-salt-bucket',
#                     'tdid': f'tdid-from-{uid2_1}'
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_1}',
#                     'euid_salt_bucket': f'{euid_1}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_1}'
#                 }]
#             },
#             'email2': {
#                 'jira_ticket_number':
#                 'TICKET-02',
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': 'optout',
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_2}',
#                     'euid_salt_bucket': f'{euid_2}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_2}'
#                 }]
#             }
#         }
#
#         mock_cursor.execute.side_effect = Exception()
#
#         # Call the function
#         with self.assertRaises(Exception):
#             sql_util.persist_to_ttdglobal_deleterequest(mock_task_instance)
#
#         mock_hook_instance.get_conn.assert_called_once()
#
#         mock_cursor.execute.assert_called_once()
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_query_advertiser_segments(self, mock_mssql_hook):
#         # Mock the MsSqlHook instance
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#
#         # Mock the cursor and execute method
#         mock_cursor = mock_hook_instance.get_conn.return_value.cursor.return_value
#         mock_cursor.fetchall.return_value = [(1, ), (2, ), (3, )]
#
#         # Call the function
#         advertiser_id = 'your_advertiser_id'
#         result = sql_util.query_advertiser_segments(advertiser_id)
#
#         # Assert the expected calls
#         mock_hook_instance.get_conn.assert_called_once()
#         mock_cursor.execute.assert_called_once()
#
#         # Assert the SQL query
#         expected_sql = dedent(
#             """
#             SELECT DISTINCT TargetingDataId
#             FROM dbo.TargetingData TD
#             WHERE TD.DataOwnerId = 'your_advertiser_id'
#             ORDER BY TargetingDataId
#             OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY
#             """
#         )
#         mock_cursor.execute.assert_called_with(expected_sql)
#
#         # Assert the function result
#         self.assertEqual(result, [1, 2, 3])
#
#     def test_query_advertiser_segments_empty_advertiser_id(self):
#         # Call the function with empty advertiser_id
#         with self.assertRaises(AirflowException) as context:
#             sql_util.query_advertiser_segments('')
#
#         self.assertEqual(str(context.exception), 'Advertiser id cannot be empty')
#
#     def test_query_advertiser_segments_none_advertiser_id(self):
#         # Call the function with None advertiser_id
#         with self.assertRaises(AirflowException) as context:
#             sql_util.query_advertiser_segments("")
#
#         self.assertEqual(str(context.exception), 'Advertiser id cannot be empty')
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_query_third_party_segments(self, mock_mssql_hook):
#         # Mock the MsSqlHook instance
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#
#         # Mock the cursor and execute method
#         mock_cursor = mock_hook_instance.get_conn.return_value.cursor.return_value
#         mock_cursor.fetchall.return_value = [(1, ), (2, ), (3, )]
#
#         # Call the function
#         data_provider_id = 'your_data_provider_id'
#         result = sql_util.query_third_party_segments(data_provider_id)
#
#         # Assert the expected calls
#         mock_hook_instance.get_conn.assert_called_once()
#         mock_cursor.execute.assert_called_once()
#
#         # Assert the SQL query
#         expected_sql = dedent(
#             f"""
#             IF NOT EXISTS (SELECT 1
#                            FROM dbo.OfflineDataProvider
#                            WHERE TreatAsFirstParty = 1
#                             AND OfflineDataProviderId = '{data_provider_id}')
#                 SELECT DISTINCT TargetingDataId
#                 FROM dbo.TargetingData
#                 WHERE DataOwnerId = '{data_provider_id}'
#                 ORDER BY TargetingDataId
#                 OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY;
#             ELSE
#                 PRINT 'OfflineDataProviderId is designated as a first party provider.  No segments will be returned.'
#             """
#         )
#         mock_cursor.execute.assert_called_with(expected_sql)
#
#         # Assert the function result
#         self.assertEqual(result, [1, 2, 3])
#
#     def test_query_third_party_segments_empty_data_provider_id(self):
#         # Call the function with empty advertiser_id
#         with self.assertRaises(AirflowException) as context:
#             sql_util.query_third_party_segments('')
#
#         self.assertEqual(str(context.exception), 'Data Provider id cannot be empty')
#
#     def test_query_third_party_segments_none_data_provider_id(self):
#         # Call the function with None advertiser_id
#         with self.assertRaises(AirflowException) as context:
#             sql_util.query_third_party_segments("")
#
#         self.assertEqual(str(context.exception), 'Data Provider id cannot be empty')
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_query_tenant_id(self, mock_mssql_hook):
#         # Mock the MsSqlHook instance
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#
#         # Mock the cursor and execute method
#         mock_cursor = mock_hook_instance.get_conn.return_value.cursor.return_value
#         mock_cursor.fetchall.side_effect = [[('advertiser_id_1', 'partner_id_1', 'tenant_id_1'),
#                                              ('advertiser_id_2', 'partner_id_2', 'tenant_id_2')],
#                                             [('data_provider_id_1', 'tenant_id_3'), ('data_provider_id_2', 'tenant_id_4')]]
#
#         # Define input parameters
#         advertiser_ids = ['advertiser_id_1', 'advertiser_id_2']
#         data_provider_ids = ['data_provider_id_1', 'data_provider_id_2']
#
#         # Call the function
#         results = sql_util.query_tenant_id(advertiser_ids, data_provider_ids)
#
#         # Assert the expected calls
#         mock_hook_instance.get_conn.assert_called_once()
#
#         expected_advertiser_sql = dedent(
#             """
#             SELECT A.AdvertiserId, A.PartnerId, PG.TenantId
#             FROM Advertiser as A
#                      JOIN dbo.Partner AS P ON P.PartnerId = A.PartnerId
#                      JOIN dbo.PartnerGroup AS PG ON PG.PartnerGroupId = P.PartnerGroupId
#             WHERE A.AdvertiserId IN ('advertiser_id_1','advertiser_id_2');
#             """
#         )
#         expected_data_provider_sql = dedent(
#             """
#             SELECT TPD.ThirdPartyDataProviderId, T.TenantId
#             FROM dbo.ThirdPartyDataProvider AS TPD
#                     JOIN
#                  dbo.Tenant AS T ON TPD.CloudServiceId = T.CloudServiceId
#             WHERE TPD.ThirdPartyDataProviderId IN ('data_provider_id_1','data_provider_id_2')
#             """
#         )
#         mock_cursor.execute.assert_has_calls([call(expected_advertiser_sql), call(expected_data_provider_sql)])
#
#         # Assert the result
#         expected_results = [{
#             'advertiserId': 'advertiser_id_1',
#             'partnerId': 'partner_id_1',
#             'tenantId': 'tenant_id_1'
#         }, {
#             'advertiserId': 'advertiser_id_2',
#             'partnerId': 'partner_id_2',
#             'tenantId': 'tenant_id_2'
#         }, {
#             'dataProviderId': 'data_provider_id_1',
#             'tenantId': 'tenant_id_3'
#         }, {
#             'dataProviderId': 'data_provider_id_2',
#             'tenantId': 'tenant_id_4'
#         }]
#         self.assertEqual(results, expected_results)
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_query_tenant_id_no_data_provider_ids(self, mock_mssql_hook):
#         # Mock the MsSqlHook instance
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#
#         # Mock the cursor and execute method
#         mock_cursor = mock_hook_instance.get_conn.return_value.cursor.return_value
#         mock_cursor.fetchall.side_effect = [
#             [('advertiser_id_1', 'partner_id_1', 'tenant_id_1'), ('advertiser_id_2', 'partner_id_2', 'tenant_id_2')],
#         ]
#
#         # Define input parameters
#         advertiser_ids: List[str] = ['advertiser_id_1', 'advertiser_id_2', '']
#         data_provider_ids: List[str] = []
#
#         # Call the function
#         results = sql_util.query_tenant_id(advertiser_ids, data_provider_ids)
#
#         # Assert the expected calls
#         mock_hook_instance.get_conn.assert_called_once()
#
#         expected_advertiser_sql = dedent(
#             """
#                 SELECT A.AdvertiserId, A.PartnerId, PG.TenantId
#                 FROM Advertiser as A
#                          JOIN dbo.Partner AS P ON P.PartnerId = A.PartnerId
#                          JOIN dbo.PartnerGroup AS PG ON PG.PartnerGroupId = P.PartnerGroupId
#                 WHERE A.AdvertiserId IN ('advertiser_id_1','advertiser_id_2');
#                 """
#         )
#         mock_cursor.execute.assert_called_with(expected_advertiser_sql)
#
#         # Assert the result
#         expected_results = [
#             {
#                 'advertiserId': 'advertiser_id_1',
#                 'partnerId': 'partner_id_1',
#                 'tenantId': 'tenant_id_1'
#             },
#             {
#                 'advertiserId': 'advertiser_id_2',
#                 'partnerId': 'partner_id_2',
#                 'tenantId': 'tenant_id_2'
#             },
#         ]
#         self.assertEqual(results, expected_results)
#
#     @patch('src.dags.pdg.data_subject_request.util.sql_util.MsSqlHook')
#     def test_query_tenant_id_no_ids(self, mock_mssql_hook):
#         # Mock the MsSqlHook instance
#         mock_hook_instance = Mock()
#         mock_mssql_hook.return_value = mock_hook_instance
#
#         # Mock the cursor and execute method
#         mock_cursor = mock_hook_instance.get_conn.return_value.cursor.return_value
#
#         # Define input parameters
#         advertiser_ids: List[str] = []
#         data_provider_ids: List[str] = []
#
#         # Call the function
#         results = sql_util.query_tenant_id(advertiser_ids, data_provider_ids)
#
#         # Assert the expected calls
#         mock_hook_instance.get_conn.assert_called_once()
#
#         mock_cursor.execute.assert_not_called()
#
#         # Assert the result
#         expected_results: List[str] = []
#         self.assertEqual(results, expected_results)
