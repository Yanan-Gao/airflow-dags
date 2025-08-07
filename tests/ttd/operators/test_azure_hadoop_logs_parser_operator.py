import os
import unittest
from unittest.mock import patch, MagicMock, call, mock_open

from ttd.operators.azure_hadoop_logs_parser_operator import AzureHadoopLogsParserOperator


class TestAzureHadoopLogsParserOperator(unittest.TestCase):

    def setUp(self):
        self.operator = AzureHadoopLogsParserOperator(
            task_id='test_task', app_id='app_12345', cluster_name='test_cluster', print_parsed_spark_driver_logs=True
        )

    @patch.object(AzureHadoopLogsParserOperator, '_write_driver_logs_to_file')
    @patch.object(AzureHadoopLogsParserOperator, '_log_driver_logs')
    @patch('ttd.operators.azure_hadoop_logs_parser_operator.tempfile.TemporaryDirectory')
    @patch('ttd.operators.azure_hadoop_logs_parser_operator.CloudStorageBuilder')
    @patch('ttd.operators.azure_hadoop_logs_parser_operator.subprocess.run')
    @patch('ttd.operators.azure_hadoop_logs_parser_operator.os.remove')
    def test_execute(
        self, mock_os_remove, mock_subprocess_run, mock_cloud_storage_builder, mock_tempdir, mock_log, mock_write_driver_logs_to_file
    ):
        mock_temp_dir = '/tmp/mock_tempdir'
        mock_tempdir.return_value.__enter__.return_value = mock_temp_dir

        mock_cloud_storage = MagicMock()
        mock_cloud_storage_builder.return_value.build.return_value = mock_cloud_storage

        app_folder = os.path.join('test_cluster-container', '/app-logs/livy/logs/', '2345', 'app_12345')
        mock_cloud_storage.list_keys.return_value = [f"{app_folder}/", f"{app_folder}/log1", f"{app_folder}/log2"]

        self.operator.execute(context={})

        parser_local_path = os.path.join(mock_temp_dir, 'hadooplogparser.jar')
        mock_cloud_storage.download_file.assert_any_call(
            file_path=parser_local_path,
            key=self.operator.parser_executable_path,
            bucket_name=f"{self.operator.parser_container_name}@{self.operator.build_artefacts_storage_account_name}",
        )

        mock_cloud_storage.list_keys.assert_called_once_with(
            prefix=os.path
            .join(self.operator.log_container_name, self.operator.log_folder_prefix, self.operator.batch_id, self.operator.app_id),
            bucket_name=f"{self.operator.log_container_name}@{self.operator.logs_storage_account_name}"
        )

        self.assertEqual(mock_subprocess_run.call_count, 2)
        calls_os_remove = [
            call(f"{mock_temp_dir}/parsed/app_12345/log1"),
            call(f"{mock_temp_dir}/parsed/app_12345/log1.txt"),
            call(f"{mock_temp_dir}/parsed/app_12345/log2"),
            call(f"{mock_temp_dir}/parsed/app_12345/log2.txt")
        ]
        mock_os_remove.assert_has_calls(calls_os_remove)

    def test_upload_log_file(self):
        mock_cloud_storage = MagicMock()
        destination_file_name = 'destination/path/parsed_log.txt'
        parsed_log_path = '/tmp/output_folder/app_12345/log1.txt'

        self.operator.log_container_name = "test_cluster-container"
        self.operator._upload_log_file(
            cloud_storage=mock_cloud_storage, destination_file_name=destination_file_name, parsed_log_path=parsed_log_path
        )

        mock_cloud_storage.upload_file.assert_called_once_with(
            key=destination_file_name,
            file_path=parsed_log_path,
            replace=True,
            bucket_name=f"{self.operator.log_container_name}@{self.operator.logs_storage_account_name}"
        )

    @patch('ttd.operators.azure_hadoop_logs_parser_operator.subprocess.run')
    def test_parse_log_file(self, mock_subprocess_run):
        parser_local_path = '/tmp/parser.jar'
        log_file = '/tmp/output_folder/log1'
        output_folder = '/tmp/output_folder/app_12345'
        file_name = 'log1'
        parsed_log_path = os.path.join(output_folder, f"{file_name}.txt")

        self.operator.log_container_name = "test_cluster-container"
        result = self.operator._parse_log_file(parser_local_path=parser_local_path, log_file=log_file, output_folder=output_folder)

        parser_command = ['java', '-jar', parser_local_path, '-d', log_file, '-o', output_folder]
        mock_subprocess_run.assert_called_once_with(parser_command, check=True)
        self.assertEqual(result, parsed_log_path)

    def test_download_log_file(self):
        mock_cloud_storage = MagicMock()
        log_file = 'some/log/path/log1'
        output_folder = '/tmp/output_folder'
        file_name = 'log1'

        self.operator.log_container_name = "test_cluster-container"
        local_path = self.operator._download_log_file(cloud_storage=mock_cloud_storage, log_file=log_file, output_folder=output_folder)

        expected_local_path = os.path.join(output_folder, file_name)
        mock_cloud_storage.download_file.assert_called_once_with(
            file_path=expected_local_path,
            key=log_file,
            bucket_name=f"{self.operator.log_container_name}@{self.operator.logs_storage_account_name}"
        )
        self.assertEqual(local_path, expected_local_path)

    @patch.object(AzureHadoopLogsParserOperator, '_write_driver_logs_to_file')
    @patch('ttd.operators.azure_hadoop_logs_parser_operator.os.remove')
    @patch.object(AzureHadoopLogsParserOperator, '_log_driver_logs')
    @patch.object(AzureHadoopLogsParserOperator, '_upload_log_file')
    @patch.object(AzureHadoopLogsParserOperator, '_parse_log_file')
    @patch.object(AzureHadoopLogsParserOperator, '_download_log_file')
    def test_process_application_folder_logs(
        self, mock_download_log_file, mock_parse_log_file, mock_upload_log_file, mock_log, mock_os_remove, mock_write_driver_logs_to_file
    ):
        app_folder = 'log_container/app-logs/livy/logs/app_12345'
        log_files = [f"{app_folder}/log1", f"{app_folder}/log2"]
        parser_local_path = '/tmp/parser.jar'
        output_folder = '/tmp/output_folder/app_12345'

        mock_cloud_storage = MagicMock()
        mock_download_log_file.side_effect = ['/tmp/output_folder/log1', '/tmp/output_folder/log2']
        mock_parse_log_file.side_effect = ['/tmp/output_folder/log1.txt', '/tmp/output_folder/log2.txt']
        mock_write_driver_logs_to_file.return_value = True

        self.operator.log_container_name = "test_cluster-container"
        self.operator._process_application_folder_logs(
            app_folder=app_folder,
            log_files=log_files,
            cloud_storage=mock_cloud_storage,
            parser_local_path=parser_local_path,
            output_folder=output_folder
        )

        calls_download = [
            call(cloud_storage=mock_cloud_storage, log_file=log_files[0], output_folder=output_folder),
            call(cloud_storage=mock_cloud_storage, log_file=log_files[1], output_folder=output_folder)
        ]
        mock_download_log_file.assert_has_calls(calls_download)

        calls_parse = [
            call(parser_local_path=parser_local_path, log_file='/tmp/output_folder/log1', output_folder=output_folder),
            call(parser_local_path=parser_local_path, log_file='/tmp/output_folder/log2', output_folder=output_folder)
        ]
        mock_parse_log_file.assert_has_calls(calls_parse)

        destination_file_name1 = os.path.join(app_folder, self.operator.parsed_logs_folder_name, 'log1.txt')
        destination_file_name2 = os.path.join(app_folder, self.operator.parsed_logs_folder_name, 'log2.txt')
        destination_driver_log_file_name = os.path.join(app_folder, self.operator.parsed_logs_folder_name, 'driver_logs.txt')
        calls_upload = [
            call(
                cloud_storage=mock_cloud_storage,
                destination_file_name=destination_file_name1,
                parsed_log_path='/tmp/output_folder/log1.txt'
            ),
            call(
                cloud_storage=mock_cloud_storage,
                destination_file_name=destination_driver_log_file_name,
                parsed_log_path=os.path.join(output_folder, 'driver_logs.txt')
            ),
            call(
                cloud_storage=mock_cloud_storage,
                destination_file_name=destination_file_name2,
                parsed_log_path='/tmp/output_folder/log2.txt'
            ),
        ]
        mock_upload_log_file.assert_has_calls(calls_upload)

        mock_log.assert_called_once_with(path=os.path.join(output_folder, 'driver_logs.txt'))

        calls_remove = [
            call('/tmp/output_folder/log1'),
            call('/tmp/output_folder/log1.txt'),
            call('/tmp/output_folder/log2'),
            call('/tmp/output_folder/log2.txt')
        ]
        mock_os_remove.assert_has_calls(calls_remove)

    @patch('ttd.operators.azure_hadoop_logs_parser_operator.logging.info')
    def test_log_driver_logs(self, mock_logging_info):
        log_lines = ["Container: container_123456_000001 on node1\n", "Log line 1\n", "Log line 2\n"]
        with patch('builtins.open', mock_open(read_data="".join(log_lines))):
            self.operator._log_driver_logs(path="dummy.txt")

        expected_group_start = "::group::Driver logs:"
        expected_group_end = "::endgroup::"
        expected_calls = [call(expected_group_start)] + [call(line) for line in log_lines] + [call(expected_group_end)]

        self.assertEqual(mock_logging_info.call_count, 5)
        self.assertEqual(mock_logging_info.call_args_list, expected_calls)

    def test_write_driver_logs_to_file_no_logs(self):
        mock_file_content = ("Container: container_123456_000002 on node2\n"
                             "Log line 1\n"
                             "Log line 2\n")
        side_effect, write_mo = self._dual_open(mock_file_content)

        with patch('builtins.open', side_effect):
            result = self.operator._write_driver_logs_to_file("dummy_in", "dummy_out")
            written_lines = [c.args[0] for c in write_mo.write.call_args_list]

            self.assertFalse(result)
            self.assertEqual(written_lines, [])

    def test_write_driver_logs_to_file_empty_file(self):
        mock_file_content = ""
        side_effect, write_mo = self._dual_open(mock_file_content)
        with patch('builtins.open', side_effect):
            result = self.operator._write_driver_logs_to_file("dummy_in", "dummy_out")
            written_lines = [c.args[0] for c in write_mo.write.call_args_list]

            self.assertFalse(result)
            self.assertEqual(written_lines, [])

    def test_write_driver_logs_to_file_container_at_beginning(self):
        mock_file_content = (
            "Container: container_123456_000001 on node1\n"
            "Log line 1\n"
            "Log line 2\n"
            "Container: container_123456_000002 on node2\n"
        )
        side_effect, write_mo = self._dual_open(mock_file_content)
        with patch('builtins.open', side_effect):
            result = self.operator._write_driver_logs_to_file("dummy_in", "dummy_out")
            written_lines = [c.args[0] for c in write_mo.write.call_args_list]
            expected_result = ["Container: container_123456_000001 on node1\n", "Log line 1\n", "Log line 2\n"]

            self.assertTrue(result)
            self.assertEqual(written_lines, expected_result)

    def test_write_driver_logs_to_file_container_at_end(self):
        mock_file_content = (
            "Container: container_123456_000002 on node2\n"
            "Log line 1\n"
            "Log line 2\n"
            "Container: container_123456_000001 on node1\n"
            "Log line 3\n"
            "Log line 4\n"
        )
        side_effect, write_mo = self._dual_open(mock_file_content)

        with patch('builtins.open', side_effect):
            result = self.operator._write_driver_logs_to_file("dummy_in", "dummy_out")
            written_lines = [c.args[0] for c in write_mo.write.call_args_list]
            expected_result = ["Container: container_123456_000001 on node1\n", "Log line 3\n", "Log line 4\n"]

            self.assertTrue(result)
            self.assertEqual(written_lines, expected_result)

    def test_write_driver_logs_to_file_container_in_between(self):
        mock_file_content = (
            "Container: container_123456_000002 on node2\n"
            "Log line 1\n"
            "Log line 2\n"
            "Container: container_123456_000001 on node1\n"
            "Log line 3\n"
            "Log line 4\n"
            "Container: container_123456_000003 on node3\n"
            "Log line 5\n"
            "Log line 6\n"
        )
        side_effect, write_mo = self._dual_open(mock_file_content)

        with patch('builtins.open', side_effect):
            result = self.operator._write_driver_logs_to_file("dummy_in", "dummy_out")
            written_lines = [c.args[0] for c in write_mo.write.call_args_list]
            expected_result = ["Container: container_123456_000001 on node1\n", "Log line 3\n", "Log line 4\n"]

            self.assertTrue(result)
            self.assertEqual(written_lines, expected_result)

    def _dual_open(self, read_data: str):
        read_mo = mock_open(read_data=read_data)
        write_mo = mock_open()

        def _side_effect(file, mode="r", *args, **kwargs):
            return read_mo() if "r" in mode else write_mo()

        return _side_effect, write_mo()
