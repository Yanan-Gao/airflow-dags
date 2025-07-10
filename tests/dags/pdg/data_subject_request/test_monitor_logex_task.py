# TODO fix
# import unittest
# from unittest.mock import patch, Mock, call
# from textwrap import dedent
#
# from airflow.exceptions import AirflowException
#
# from src.dags.pdg.data_subject_request.handler import monitor_logex_task
#
#
# class TestMonitorLogExTask(unittest.TestCase):
#
#     def test_monitor_logex_task__all_complete__total_outcome_true(self):
#         self.assert_monitor_logex_task_outcome(
#             returned_rows=[(1, 'some/file/path/returned_1', monitor_logex_task.LogFileTaskStatus.Completed.value),
#                            (2, 'some/file/path/returned_2', monitor_logex_task.LogFileTaskStatus.Completed.value)],
#             expected_outcome=True
#         )
#
#     def test_monitor_logex_task__one_not_complete__total_outcome_false(self):
#         self.assert_monitor_logex_task_outcome(
#             returned_rows=[(1, 'some/file/path/returned_1', monitor_logex_task.LogFileTaskStatus.Completed.value),
#                            (2, 'some/file/path/returned_2', monitor_logex_task.LogFileTaskStatus.Processing.value)],
#             expected_outcome=False
#         )
#
#     def test_monitor_logex_task__one_failed__raises_exception(self):
#         with self.assertRaises(AirflowException):
#             self.assert_monitor_logex_task_outcome(
#                 returned_rows=[(1, 'some/file/path/returned_1', monitor_logex_task.LogFileTaskStatus.Completed.value
#                                 ), (2, 'some/file/path/returned_2', monitor_logex_task.LogFileTaskStatus.Failed.value)]
#             )
#
#     def test_monitor_logex_task__one_ignored__raises_exception(self):
#         with self.assertRaises(AirflowException):
#             self.assert_monitor_logex_task_outcome(
#                 returned_rows=[(1, 'some/file/path/returned_1', monitor_logex_task.LogFileTaskStatus.Completed.value
#                                 ), (2, 'some/file/path/returned_2', monitor_logex_task.LogFileTaskStatus.Ignored.value)]
#             )
#
#     def test_monitor_logex_task__returns_no_rows__raises_exception(self):
#         self.assert_monitor_logex_task_outcome(returned_rows=[], expected_outcome=False)
#
#     def assert_monitor_logex_task_outcome(self, returned_rows=[], expected_outcome=None):
#         with patch('src.dags.pdg.data_subject_request.handler.monitor_logex_task.MsSqlHook') as MsSqlHookMock:
#             task_instance = Mock()
#             task_instance.xcom_pull.return_value = {'tableX': ['some/file/path/TDID', 'some/file/path/UnifiedId2']}
#             MsSqlHookMock().get_conn().cursor().__iter__ = Mock(return_value=iter(returned_rows))
#
#             actual_outcome = monitor_logex_task.monitor_logex_task(
#                 task_instance, table_name='tableX', logex_task=monitor_logex_task.LogExTask.VerticaScrub, mssql_conn_id=Mock()
#             )
#
#             self.assertEqual(expected_outcome, actual_outcome)
#             MsSqlHookMock().get_conn().cursor().execute.assert_has_calls([
#                 call(
#                     dedent(
#                         """
#                     SELECT LFT.LogFileId, LF.S3RawLogKey, LFT.LogFileTaskStatusId
#                     FROM dbo.LogFileTask as LFT
#                     INNER JOIN dbo.LogFile as LF
#                     ON LFT.LogFileId = LF.LogFileId
#                     WHERE LF.S3RawLogKey LIKE 'some_file_path_TDID/%' AND LFT.TaskId = dbo.fn_Enum_Task_VerticaScrub()
#                     """
#                     )
#                 ),
#                 call(
#                     dedent(
#                         """
#                     SELECT LFT.LogFileId, LF.S3RawLogKey, LFT.LogFileTaskStatusId
#                     FROM dbo.LogFileTask as LFT
#                     INNER JOIN dbo.LogFile as LF
#                     ON LFT.LogFileId = LF.LogFileId
#                     WHERE LF.S3RawLogKey LIKE 'some_file_path_UnifiedId2/%' AND LFT.TaskId = dbo.fn_Enum_Task_VerticaScrub()
#                     """
#                     )
#                 )
#             ])
