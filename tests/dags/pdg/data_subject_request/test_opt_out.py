# TODO fix
# import unittest
# from unittest.mock import patch, Mock, call
#
# import src.dags.pdg.data_subject_request.util.opt_out_util as opt_out
#
#
# class TestOptOut(unittest.TestCase):
#
#     @patch('src.dags.pdg.data_subject_request.util.opt_out_util.datetime')
#     def test_opt_out__valid_uiids__calls_optout_successfully(self, datetime_mock):
#         uid2_1, uid2_2 = "uid2_1", "uid2_2"
#         euid_1, euid_2 = "euid_1", "euid_2"
#
#         task_instance = Mock()
#         task_instance.xcom_pull.return_value = {
#             'email1': {
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
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': uid2_2,
#                     'uid2_salt_bucket': f'{uid2_2}-salt-bucket',
#                     'tdid': f'tdid-from-{uid2_2}'
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid_2}',
#                     'euid_salt_bucket': f'{euid_2}-salt-bucket',
#                     'tdid': f'tdid-from-{euid_2}'
#                 }]
#             }
#         }
#
#         opt_out.requests.post = Mock(
#             return_value=Mock(status_code=200, json=Mock(return_value={
#                 'InvalidUid2s': [],
#                 'UnsuccessfulOptOuts': []
#             }))
#         )
#
#         opt_out.requests.request = Mock(return_value=Mock(status_code=200))
#
#         datetime_mock.today().timestamp.side_effect = [123, 321]
#
#         opt_out.opt_out(task_instance)
#
#         opt_out.requests.post.assert_called_once_with("https://insight.adsrvr.org/track/uid2optout", json={"uid2Ids": [uid2_1, uid2_2]})
#
#         opt_out.requests.request.assert_has_calls([
#             call("GET", f"https://insight.adsrvr.org/track/euidoptout?action=dooptout&euid={euid_1}&timestamp=123"),
#             call("GET", f"https://insight.adsrvr.org/track/euidoptout?action=dooptout&euid={euid_2}&timestamp=321"),
#         ])
#
#     @patch('src.dags.pdg.data_subject_request.util.opt_out_util.datetime')
#     def test_opt_out_with_already_opted_out_ids_calls_optout_successfully(self, datetime_mock):
#         uid2 = "uid2_1"
#         euid = "euid_1"
#         optout = 'optout'
#
#         task_instance = Mock()
#         task_instance.xcom_pull.return_value = {
#             'email1': {
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': uid2,
#                     'uid2_salt_bucket': f'{uid2}-salt-bucket',
#                     'tdid': f'tdid-from-{uid2}'
#                 }, {
#                     'type': 'euid',
#                     'euid': optout,
#                 }]
#             },
#             'email2': {
#                 'identifiers': [{
#                     'type': 'uid2',
#                     'uid2': optout,
#                 }, {
#                     'type': 'euid',
#                     'euid': f'{euid}',
#                     'euid_salt_bucket': f'{euid}-salt-bucket',
#                     'tdid': f'tdid-from-{euid}'
#                 }]
#             }
#         }
#
#         opt_out.requests.post = Mock(
#             return_value=Mock(status_code=200, json=Mock(return_value={
#                 'InvalidUid2s': [],
#                 'UnsuccessfulOptOuts': []
#             }))
#         )
#
#         opt_out.requests.request = Mock(return_value=Mock(status_code=200))
#
#         datetime_mock.today().timestamp.side_effect = [123, 321]
#
#         opt_out.opt_out(task_instance)
#
#         opt_out.requests.post.assert_called_once_with("https://insight.adsrvr.org/track/uid2optout", json={"uid2Ids": [uid2]})
#
#         opt_out.requests.request.assert_has_calls([
#             call("GET", f"https://insight.adsrvr.org/track/euidoptout?action=dooptout&euid={euid}&timestamp=123"),
#         ])
