# TODO fix
# import unittest
# from unittest.mock import patch, Mock, call
# from datetime import date, timedelta
#
# from airflow import AirflowException
# from airflow.models import TaskInstance
#
# from src.dags.pdg.data_subject_request.handler import \
#     vertica_scrub_generator_creator as undertest
# from src.dags.pdg.data_subject_request.config.vertica_config import Pii, ScrubType, \
#     VerticaTableConfiguration
# from src.dags.pdg.data_subject_request.handler.vertica_scrub_generator_creator import \
#     _map_user_id_type_and_value_to_vertica_pii_column, IdentitySource
# from src.dags.pdg.data_subject_request.util.dsr_dynamodb_util import \
#     PARTITION_KEY_ATTRIBUTE_NAME, SORT_KEY_ATTRIBUTE_NAME, PARTNER_DSR_USER_ID_GUID, PARTNER_DSR_USER_ID_TYPE, \
#     PARTNER_DSR_USER_ID_RAW
#
# from ttd.ttdenv import TtdEnvFactory
#
# patch_path = "src.dags.pdg.data_subject_request.handler.vertica_scrub_generator_creator"
#
# start_date = date(2023, 1, 1)
# time_since_last_processing = start_date - timedelta(days=8)
# time_since_last_processing_formatted = time_since_last_processing.strftime('%Y-%m-%dT%H:%M:%S')
# request_id = "request_id"
#
#
# class TestVerticaScrubGeneratorCreator(unittest.TestCase):
#     maxDiff = 2000  # increase to see diff on errors
#
#     def setUp(self) -> None:
#         TtdEnvFactory.get_from_system = Mock(return_value=TtdEnvFactory.prod)
#
#     def test_create_generator_tasks_dsr__special_table_name__generates_valid_s3_path(self):
#         start_time = date(2042, 2, 15)
#
#         test_table_config = {
#             c.raw_table_name: c
#             for c in [
#                 VerticaTableConfiguration(
#                     raw_table_name='A.very**special!!t@ble_name',
#                     retention_period=timedelta(days=45),
#                     filter_columns=[Pii.TDID],
#                     pii_columns=[Pii.TDID],
#                     tdid_special_transforms=[],
#                     scrub_type=ScrubType.SCRUB_PII_COLUMNS,
#                     start_processing_offset=timedelta(days=10),
#                     stop_processing_offset=timedelta(days=0)
#                 )
#             ]
#         }
#
#         task_instance = Mock()
#         task_instance.xcom_pull = mock_xcom_pull({
#             'request_id': request_id,
#             'emails_and_identifiers': {
#                 'email1': {
#                     'identifiers': [
#                         {
#                             'type': 'uid2',
#                             'uid2': "uid21",
#                             'uid2_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid1'
#                         },
#                         {
#                             'type': 'euid',
#                             'euid': "euid1",
#                             'euid_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid2'
#                         },
#                     ]
#                 }
#             }
#         })
#
#         with patch(f"{patch_path}.DSR_VERTICA_TABLE_CONFIGURATIONS", test_table_config):
#             tasks = undertest.create_generator_tasks_dsr(task_instance, start_time)
#
#         self.assertEqual(1, len(tasks))
#         _, actual_tdid_log_key, _ = tasks[0]
#         self.assertEqual('ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/TDID', actual_tdid_log_key)
#
#     def test_generate_tasks__scrub_type_row__generates_log_key_run_data(self):
#         start_time = date(2042, 2, 15)
#
#         test_table_config = {
#             c.raw_table_name: c
#             for c in [
#                 VerticaTableConfiguration(
#                     raw_table_name='some_table',
#                     retention_period=timedelta(days=45),
#                     filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
#                     pii_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
#                     tdid_special_transforms=[],
#                     scrub_type=ScrubType.DELETE_ROW,
#                     start_processing_offset=timedelta(days=10),
#                     stop_processing_offset=timedelta(days=0)
#                 )
#             ]
#         }
#
#         task_instance = Mock()
#         task_instance.xcom_pull = mock_xcom_pull({
#             'request_id': request_id,
#             'emails_and_identifiers': {
#                 'email1': {
#                     'identifiers': [
#                         {
#                             'type': 'uid2',
#                             'uid2': "uid21",
#                             'uid2_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid1'
#                         },
#                         {
#                             'type': 'euid',
#                             'euid': "euid1",
#                             'euid_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid2'
#                         },
#                     ]
#                 }
#             }
#         })
#
#         with patch(f"{patch_path}.DSR_VERTICA_TABLE_CONFIGURATIONS", test_table_config):
#             tasks = undertest.create_generator_tasks_dsr(task_instance, start_time)
#
#         self.assertEqual(3, len(tasks))
#         _, actual_tdid_log_key, actual_tdid_run_data = tasks[0]
#         self.assertEqual('ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/TDID', actual_tdid_log_key)
#
#         self.assertEqual({
#             'Mode': 0,
#             'Dates': '2042-01-11:2042-02-15',
#             'Tables': 'some_table',
#             'FilterExpr': 't.TDID in (\'tdid1\',\'tdid2\')',
#             'Replacements': [],
#             'PerformScrub': True
#         }, actual_tdid_run_data)
#
#         _, actual_uid2_log_key, actual_uid2_run_data = tasks[1]
#         self.assertEqual('ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/UnifiedId2', actual_uid2_log_key)
#
#         self.assertEqual({
#             'Mode': 0,
#             'Dates': '2042-01-11:2042-02-15',
#             'Tables': 'some_table',
#             'FilterExpr': 't.UnifiedId2 in (\'uid21\')',
#             'Replacements': [],
#             'PerformScrub': True
#         }, actual_uid2_run_data)
#
#     def test_generate_tasks__scrub_type_pii_only__generates_run_data_with_replacements(self):
#         start_time = date(2042, 2, 15)
#
#         test_table_config = {
#             c.raw_table_name: c
#             for c in [
#                 VerticaTableConfiguration(
#                     raw_table_name='some_table',
#                     retention_period=timedelta(days=45),
#                     filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
#                     pii_columns=[Pii.TDID, Pii.UnifiedId2, Pii.Longitude, Pii.EUID, Pii.PiiScrubReasons],
#                     tdid_special_transforms=[],
#                     scrub_type=ScrubType.SCRUB_PII_COLUMNS,
#                     start_processing_offset=timedelta(days=10),
#                     stop_processing_offset=timedelta(days=0)
#                 )
#             ]
#         }
#
#         task_instance = Mock()
#         task_instance.xcom_pull = mock_xcom_pull({
#             'request_id': request_id,
#             'emails_and_identifiers': {
#                 'email1': {
#                     'identifiers': [
#                         {
#                             'type': 'uid2',
#                             'uid2': "uid21",
#                             'uid2_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid1'
#                         },
#                         {
#                             'type': 'euid',
#                             'euid': "euid1",
#                             'euid_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid2'
#                         },
#                     ]
#                 },
#                 'email2': {
#                     'identifiers': [
#                         {
#                             'type': 'uid2',
#                             'uid2': "uid22",
#                             'uid2_salt_bucket': 'salt-bucket',
#                             'tdid': 'tdid3'
#                         },
#                         {
#                             'type': 'euid',
#                             'euid': "optout",
#                         },
#                     ]
#                 }
#             }
#         })
#
#         with patch(f"{patch_path}.DSR_VERTICA_TABLE_CONFIGURATIONS", test_table_config):
#             tasks = undertest.create_generator_tasks_dsr(task_instance, start_time)
#
#         self.assertEqual(3, len(tasks))
#         _, actual_tdid_log_key, actual_tdid_run_data = tasks[0]
#         self.assertEqual('ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/TDID', actual_tdid_log_key)
#
#         self.assertEqual({
#             'Mode':
#             1,
#             'Dates':
#             '2042-01-11:2042-02-15',
#             'Tables':
#             'some_table',
#             'FilterExpr':
#             't.TDID in (\'tdid1\',\'tdid2\',\'tdid3\')',
#             'Replacements': [{
#                 'Column': 'TDID',
#                 'Expr': "'00000000-0000-0000-0000-000000000000'"
#             }, {
#                 'Column': 'UnifiedId2',
#                 'Expr': "null"
#             }, {
#                 'Column': 'Longitude',
#                 'Expr': "null"
#             }, {
#                 'Column': 'EUID',
#                 'Expr': 'null'
#             }, {
#                 'Column': 'PiiScrubReasons',
#                 'Expr': 'DSR'
#             }],
#             'PerformScrub':
#             True
#         }, actual_tdid_run_data)
#         _, actual_uid2_log_key, actual_uid2_run_data = tasks[1]
#         self.assertEqual('ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/UnifiedId2', actual_uid2_log_key)
#
#         self.assertEqual({
#             'Mode':
#             1,
#             'Dates':
#             '2042-01-11:2042-02-15',
#             'Tables':
#             'some_table',
#             'FilterExpr':
#             't.UnifiedId2 in (\'uid21\',\'uid22\')',
#             'Replacements': [{
#                 'Column': 'TDID',
#                 'Expr': "'00000000-0000-0000-0000-000000000000'"
#             }, {
#                 'Column': 'UnifiedId2',
#                 'Expr': "null"
#             }, {
#                 'Column': 'Longitude',
#                 'Expr': "null"
#             }, {
#                 'Column': 'EUID',
#                 'Expr': 'null'
#             }, {
#                 'Column': 'PiiScrubReasons',
#                 'Expr': 'DSR'
#             }],
#             'PerformScrub':
#             True
#         }, actual_uid2_run_data)
#
#     # def test_filter_on_uiids__contains_special_tdid_transform__produces_valid_filter_exp(self):
#     #     start_time = date(2042, 2, 15)
#     #
#     #     test_table_config = {c.raw_table_name: c for c in [
#     #         VerticaTableConfiguration(
#     #             raw_table_name='some_table',
#     #             retention_period=timedelta(days=45),
#     #             filter_columns=[
#     #                 Pii.TDID,
#     #                 Pii.UnifiedId2,
#     #                 Pii.EUID],
#     #             pii_columns=[
#     #                 Pii.TDID,
#     #                 Pii.UnifiedId2,
#     #                 Pii.AttributedEventTDID,
#     #                 Pii.EUID
#     #             ],
#     #             tdid_special_transforms=["AttributedEventTDID"],
#     #             scrub_type=ScrubType.SCRUB_PII_COLUMNS,
#     #             start_processing_offset=timedelta(days=10),
#     #             stop_processing_offset=timedelta(days=0)
#     #         )
#     #     ]}
#     #
#     #     task_instance = Mock()
#     #     task_instance.xcom_pull = mock_xcom_pull({
#     #         'request_id': request_id,
#     #         'emails_and_identifiers': {
#     #             'email1': {
#     #                 'identifiers': [
#     #                     {
#     #                         'type': 'uid2',
#     #                         'uid2': "uid21",
#     #                         'uid2_salt_bucket': 'salt-bucket',
#     #                         'tdid': 'tdid1'
#     #                     },
#     #                     {
#     #                         'type': 'uid2',
#     #                         'uid2': "uid22",
#     #                         'uid2_salt_bucket': 'salt-bucket',
#     #                         'tdid': 'tdid2'
#     #                     },
#     #                     {
#     #                         'type': 'euid',
#     #                         'euid': "euid1",
#     #                         'euid_salt_bucket': 'salt-bucket',
#     #                         'tdid': 'tdid3'
#     #                     },
#     #                     {
#     #                         'type': 'euid',
#     #                         'euid': "euid2",
#     #                         'euid_salt_bucket': 'salt-bucket',
#     #                         'tdid': 'tdid4'
#     #                     },
#     #                 ]
#     #             }
#     #         }
#     #     })
#     #     with patch(f"{patch_path}.DSR_VERTICA_TABLE_CONFIGURATIONS", test_table_config):
#     #         tasks = undertest.create_generator_tasks_dsr(task_instance, start_time)
#     #
#     #     self.assertEqual(4, len(tasks))
#     #     _, actual_tdid_log_key, actual_tdid_run_data = tasks[0]
#     #     self.assertEqual(
#     #         'ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/TDID',
#     #         actual_tdid_log_key)
#     #
#     #     self.assertEqual(
#     #         {
#     #             'Mode': 1,
#     #             'Dates': '2042-01-11:2042-02-15',
#     #             'Tables': 'some_table',
#     #             'FilterExpr': 't.TDID in (\'tdid1\',\'tdid2\',\'tdid3\',\'tdid4\')',
#     #             'Replacements': [
#     #                 {'Column': 'TDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'UnifiedId2', 'Expr': "null"},
#     #                 {'Column': 'AttributedEventTDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'EUID', 'Expr': 'null'}
#     #             ],
#     #             'PerformScrub': True
#     #         },
#     #         actual_tdid_run_data)
#     #     _, actual_uid2_log_key, actual_uid2_run_data = tasks[1]
#     #     self.assertEqual(
#     #         'ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/UnifiedId2',
#     #         actual_uid2_log_key)
#     #     _, actual_uid2_log_key, actual_uid2_run_data = tasks[1]
#     #     self.assertEqual(
#     #         'ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/UnifiedId2',
#     #         actual_uid2_log_key)
#     #
#     #     self.assertEqual(
#     #         {
#     #             'Mode': 1,
#     #             'Dates': '2042-01-11:2042-02-15',
#     #             'Tables': 'some_table',
#     #             'FilterExpr': 't.UnifiedId2 in (\'uid21\',\'uid22\')',
#     #             'Replacements': [
#     #                 {'Column': 'TDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'UnifiedId2', 'Expr': "null"},
#     #                 {'Column': 'AttributedEventTDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'EUID', 'Expr': 'null'}
#     #             ],
#     #             'PerformScrub': True
#     #         },
#     #         actual_uid2_run_data)
#     #
#     #     _, actual_euid_log_key, actual_euid_run_data = tasks[2]
#     #
#     #     self.assertEqual(
#     #         {
#     #             'Mode': 1,
#     #             'Dates': '2042-01-11:2042-02-15',
#     #             'Tables': 'some_table',
#     #             'FilterExpr': 't.EUID in (\'euid1\',\'euid2\')',
#     #             'Replacements': [
#     #                 {'Column': 'TDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'UnifiedId2', 'Expr': "null"},
#     #                 {'Column': 'AttributedEventTDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'EUID', 'Expr': 'null'}
#     #             ],
#     #             'PerformScrub': True
#     #         },
#     #         actual_euid_run_data)
#     #
#     #     _, actual_special_transform_log_key, actual_special_transform_run_data = tasks[2]
#     #     self.assertEqual(
#     #         'ttd-data-subject-requests/delete/request_id/2042-01-11_to_2042-02-15/EUID',
#     #         actual_special_transform_log_key)
#     #
#     #     self.assertEqual(
#     #         {
#     #             'Mode': 1,
#     #             'Dates': '2042-01-11:2042-02-15',
#     #             'Tables': 'some_table',
#     #             'FilterExpr': 't.EUID in (\'euid1\',\'euid2\')',
#     #             'Replacements': [
#     #                 {'Column': 'TDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'UnifiedId2', 'Expr': "null"},
#     #                 {'Column': 'AttributedEventTDID', 'Expr': "'00000000-0000-0000-0000-000000000000'"},
#     #                 {'Column': 'EUID', 'Expr': 'null'}
#     #             ],
#     #             'PerformScrub': True
#     #         },
#     #         actual_special_transform_run_data)
#
#     @patch('src.dags.pdg.data_subject_request.util.dsr_dynamodb_util.query_dynamodb_items')
#     def test_partner_dsr_generate_tasks__scrub_type_pii_only__generates_valid_run_data(self, mock_query_dynamodb_items):
#         start_time = date(2042, 2, 15)
#
#         test_table_config = {
#             c.raw_table_name: c
#             for c in [
#                 VerticaTableConfiguration(
#                     raw_table_name='some_table',
#                     retention_period=timedelta(days=45),
#                     filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
#                     pii_columns=[Pii.TDID, Pii.UnifiedId2, Pii.Longitude],
#                     tdid_special_transforms=["AttributedEventTDID"],
#                     scrub_type=ScrubType.SCRUB_PII_COLUMNS,
#                     start_processing_offset=timedelta(days=10),
#                     stop_processing_offset=timedelta(days=0)
#                 )
#             ]
#         }
#
#         query_result = [{
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk1',
#             'dataProviderId': '',
#             'userIdType': 'Tdid',
#             'advertiserId': 'advertiser_id_1',
#             'userIdGuid': 'f4330862-f944-43c3-8963-158fdf8274eb',
#             'rawUserId': ''
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk2',
#             'dataProviderId': 'data_provider_id',
#             'userIdType': 'Tdid',
#             'advertiserId': '',
#             'userIdGuid': 'f4330862-f944-43c3-8963-158fdf8274eb',
#             'rawUserId': ''
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk3',
#             'dataProviderId': '',
#             'userIdType': f'{IdentitySource.UnifiedId2.name.lower()}',
#             'advertiserId': 'advertiser_id_2',
#             'userIdGuid': 'guid',
#             'rawUserId': 'raw_uid2'
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk4',
#             'dataProviderId': '',
#             'userIdType': f'{IdentitySource.Tdid.name.lower()}',
#             'advertiserId': 'advertiser_id_2',
#             'userIdGuid': 'tdid1',
#             'rawUserId': ''
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk5',
#             'dataProviderId': '',
#             'userIdType': f'{IdentitySource.Tdid.name.lower()}',
#             'advertiserId': 'advertiser_id_2',
#             'userIdGuid': 'tdid2',
#             'rawUserId': ''
#         }, {
#             PARTITION_KEY_ATTRIBUTE_NAME: 'example_pk1',
#             SORT_KEY_ATTRIBUTE_NAME: 'example_sk5',
#             'dataProviderId': '',
#             'userIdType': f'{IdentitySource.EUID.name.lower()}',
#             'advertiserId': 'advertiser_id_2',
#             'userIdGuid': '',
#             'rawUserId': 'raw_euid'
#         }]
#
#         # Mock task instance
#         task_instance = Mock(spec=TaskInstance)
#         task_instance.xcom_pull.side_effect = lambda key: {'batch_request_id': request_id, 'partner_dsr_requests': query_result}[key]
#
#         with patch(f"{patch_path}.PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS", test_table_config):
#             actual_tasks = undertest.create_generator_tasks_partner_dsr(task_instance, start_time)
#
#         self.assertEqual(6, len(actual_tasks))
#
#         expected_results = [
#             ("TDID", "t.AdvertiserId = 'advertiser_id_1' and t.TDID in ('f4330862-f944-43c3-8963-158fdf8274eb')"),
#             (
#                 "AttributedEventTDID",
#                 "t.AdvertiserId = 'advertiser_id_1' and t.AttributedEventTDID in ('f4330862-f944-43c3-8963-158fdf8274eb')"
#             ),
#             ("TDID", "t.AdvertiserId = 'advertiser_id_2' and t.TDID in ('tdid1','tdid2')"),
#             ("UnifiedId2", "t.AdvertiserId = 'advertiser_id_2' and t.UnifiedId2 in ('raw_uid2')"),
#             ("EUID", "t.AdvertiserId = 'advertiser_id_2' and t.EUID in ('raw_euid')"),
#             ("AttributedEventTDID", "t.AdvertiserId = 'advertiser_id_2' and t.AttributedEventTDID in ('tdid1','tdid2')"),
#         ]
#
#         for expected_result, actual_task in zip(expected_results, actual_tasks):
#             expected_filter_col, expected_filter_exp = expected_result
#             _, actual_tdid_log_key, actual_tdid_run_data = actual_task
#
#             self.assertEqual(
#                 f'ttd-data-subject-requests/partner_dsr/delete/request_id/2042-01-11_to_2042-02-15/{expected_filter_col}',
#                 actual_tdid_log_key
#             )
#
#             self.assertEqual({
#                 'Mode':
#                 1,
#                 'Dates':
#                 '2042-01-11:2042-02-15',
#                 'Tables':
#                 'some_table',
#                 'FilterExpr':
#                 expected_filter_exp,
#                 'Replacements': [{
#                     'Column': 'TDID',
#                     'Expr': "'00000000-0000-0000-0000-000000000000'"
#                 }, {
#                     'Column': 'UnifiedId2',
#                     'Expr': "null"
#                 }, {
#                     'Column': 'Longitude',
#                     'Expr': "null"
#                 }],
#                 'PerformScrub':
#                 True
#             }, actual_tdid_run_data)
#
#     @patch(f'{patch_path}.MsSqlHook')
#     @patch(f'{patch_path}.datetime')
#     def test_push_vertica_scrub_generator_logex_task__given_tasks__calls_proc(self, datetime_mock, MsSqlHookMock):
#         task_instance = Mock()
#         uid2_1, uid2_2 = "uid2_1", "uid2_2"
#         euid_1, euid_2 = "euid_1", "euid_2"
#
#         task_instance.xcom_pull = mock_xcom_pull({
#             'request_id': 'request_1',
#             'emails_and_identifiers': {
#                 'email1': {
#                     'identifiers': [{
#                         'type': 'uid2',
#                         'uid2': uid2_1,
#                         'uid2_salt_bucket': f'{uid2_1}-salt-bucket',
#                         'tdid': f'tdid-from-{uid2_1}'
#                     }, {
#                         'type': 'tdid',
#                         'euid': f'{euid_1}',
#                         'euid_salt_bucket': f'{euid_1}-salt-bucket',
#                         'tdid': f'tdid-from-{euid_1}'
#                     }]
#                 },
#                 'email2': {
#                     'identifiers': [{
#                         'type': 'uid2',
#                         'uid2': uid2_2,
#                         'uid2_salt_bucket': f'{uid2_2}-salt-bucket',
#                         'tdid': f'tdid-from-{uid2_2}'
#                     }, {
#                         'type': 'euid',
#                         'euid': f'{euid_2}',
#                         'euid_salt_bucket': f'{euid_2}-salt-bucket',
#                         'tdid': f'tdid-from-{euid_2}'
#                     }]
#                 }
#             }
#         })
#
#         retention_period_days = 90
#         test_table_config = {
#             "ttd.ClickTracker":
#             VerticaTableConfiguration(
#                 raw_table_name="ttd.ClickTracker",
#                 retention_period=timedelta(days=retention_period_days),
#                 filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
#                 pii_columns=[Pii.TDID, Pii.DeviceAdvertisingId, Pii.IPAddress, Pii.Zip, Pii.UnifiedId2, Pii.EUID, Pii.IdentityLinkId],
#                 tdid_special_transforms=[],
#                 scrub_type=ScrubType.SCRUB_PII_COLUMNS,
#                 start_processing_offset=timedelta(days=10),
#                 stop_processing_offset=timedelta(days=0)
#             )
#         }
#
#         with patch(f'{patch_path}.DSR_VERTICA_TABLE_CONFIGURATIONS', test_table_config):
#             datetime_mock.utcnow().date.return_value = start_date
#
#             undertest.VerticaScrubGeneratorTaskPusher(undertest.create_generator_tasks_dsr
#                                                       ).push_vertica_scrub_generator_logex_task(task_instance, 'some_mssql_conn_id')
#
#         expected_from_date = start_date - timedelta(days=retention_period_days) + timedelta(days=10)
#         expected_to_date = start_date
#         expected_tdid_log_key = f'ttd-data-subject-requests/delete/request_1/{expected_from_date}_to_{expected_to_date}/TDID'
#         expected_tdid_json_run_data = \
#             '{"Mode": 1,' \
#             f' "Dates": "{expected_from_date}:{expected_to_date}",' \
#             ' "Tables": "ttd.ClickTracker",' \
#             f' "FilterExpr": "t.TDID in (\'tdid-from-{uid2_1}\',\'tdid-from-{euid_1}\',\'tdid-from-{uid2_2}\',\'tdid-from-{euid_2}\')",' \
#             ' "Replacements": [{"Column": "TDID", "Expr": "\'00000000-0000-0000-0000-000000000000\'"}, {"Column": "DeviceAdvertisingId", "Expr": "\'00000000-0000-0000-0000-000000000000\'"}, {"Column": "IPAddress", "Expr": "null"}, {"Column": "Zip", "Expr": "null"}, {"Column": "UnifiedId2", "Expr": "null"}, {"Column": "EUID", "Expr": "null"}, {"Column": "IdentityLinkId", "Expr": "null"}],' \
#             ' "PerformScrub": true}'
#
#         expected_uid2_log_key = f'ttd-data-subject-requests/delete/request_1/{expected_from_date}_to_{expected_to_date}/UnifiedId2'
#         expected_euid_log_key = f'ttd-data-subject-requests/delete/request_1/{expected_from_date}_to_{expected_to_date}/EUID'
#         expected_uid2_json_run_data = \
#             '{"Mode": 1,' \
#             f' "Dates": "{expected_from_date}:{expected_to_date}",' \
#             ' "Tables": "ttd.ClickTracker",' \
#             f' "FilterExpr": "t.UnifiedId2 in (\'{uid2_1}\',\'{uid2_2}\')",' \
#             ' "Replacements": [{"Column": "TDID", "Expr": "\'00000000-0000-0000-0000-000000000000\'"}, {"Column": "DeviceAdvertisingId", "Expr": "\'00000000-0000-0000-0000-000000000000\'"}, {"Column": "IPAddress", "Expr": "null"}, {"Column": "Zip", "Expr": "null"}, {"Column": "UnifiedId2", "Expr": "null"}, {"Column": "EUID", "Expr": "null"}, {"Column": "IdentityLinkId", "Expr": "null"}],' \
#             ' "PerformScrub": true}'
#
#         MsSqlHookMock().get_conn().cursor().callproc.assert_has_calls([
#             call('dbo.prc_CreateVerticaScrubGeneratorTask', (expected_tdid_log_key, expected_tdid_json_run_data, False)),
#             call('dbo.prc_CreateVerticaScrubGeneratorTask', (expected_uid2_log_key, expected_uid2_json_run_data, False))
#         ])
#
#         task_instance.xcom_push.assert_has_calls([
#             call(key='table_to_s3_keys', value={"ttd.ClickTracker": [expected_tdid_log_key, expected_uid2_log_key, expected_euid_log_key]})
#         ])
#
#     def test_map_user_id_type_and_value_to_vertica_pii_column_failure_missing_user_id_type(self):
#         request = {PARTNER_DSR_USER_ID_GUID: 'userIdGuid'}
#         with self.assertRaises(AirflowException) as context:
#             _map_user_id_type_and_value_to_vertica_pii_column(
#                 request.get(PARTNER_DSR_USER_ID_TYPE), request.get(PARTNER_DSR_USER_ID_GUID), request.get(PARTNER_DSR_USER_ID_RAW)
#             )
#
#         self.assertTrue('User ID Type: None or User Id GUID: userIdGuid and User Id Raw: None are not present' in str(context.exception))
#
#     def test_map_user_id_type_and_value_to_vertica_pii_column_failure_missing_user_id_raw_and_guid(self):
#         request = {PARTNER_DSR_USER_ID_TYPE: 'type'}
#         with self.assertRaises(AirflowException) as context:
#             _map_user_id_type_and_value_to_vertica_pii_column(
#                 request.get(PARTNER_DSR_USER_ID_TYPE), request.get(PARTNER_DSR_USER_ID_GUID), request.get(PARTNER_DSR_USER_ID_RAW)
#             )
#
#         self.assertTrue('User ID Type: type or User Id GUID: None and User Id Raw: None are not present' in str(context.exception))
#
#     def test_map_user_id_type_and_value_to_vertica_pii_column_happy_path(self):
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.Tdid.name, 'guid_value', '')
#         self.assertEqual((pii_column, pii_value), (Pii.TDID.name, 'guid_value'))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.MiscDeviceId.name, 'guid_value', '')
#         self.assertEqual((pii_column, pii_value), (Pii.DeviceAdvertisingId.name, 'guid_value'))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.UnifiedId2.name, '', 'raw_value')
#         self.assertEqual((pii_column, pii_value), (Pii.UnifiedId2.name, 'raw_value'))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.IdentityLinkId.name, '', 'raw_value')
#         self.assertEqual((pii_column, pii_value), (Pii.IdentityLinkId.name, 'raw_value'))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.EUID.name, '', 'raw_value')
#         self.assertEqual((pii_column, pii_value), (Pii.EUID.name, 'raw_value'))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.ID5.name, 'guid_value', 'raw_value')
#         self.assertEqual((pii_column, pii_value), (None, None))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.NetId.name, 'guid_value', 'raw_value')
#         self.assertEqual((pii_column, pii_value), (None, None))
#
#         pii_column, pii_value = _map_user_id_type_and_value_to_vertica_pii_column(IdentitySource.CoreId.name, 'guid_value', 'raw_value')
#         self.assertEqual((pii_column, pii_value), (None, None))
#
#
# def mock_xcom_pull(mock_xcom_dict):
#
#     def mock_pull(key=None, task_ids=None):
#         return mock_xcom_dict.get(key)
#
#     return mock_pull
