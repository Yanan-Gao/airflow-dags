import unittest
from unittest.mock import patch, MagicMock

from kubernetes.client.models import V1Secret, V1ObjectMeta

from ttd.task_service.k8s_connection_helper import azure
from ttd.operators.task_service_operator import (
    TaskServiceOperator,
    NoBranchNameProvidedError,
    TaskServiceForbiddenError,
    NoBranchDeploymentsInProductionError,
)
from ttd.ttdenv import TtdEnvFactory
from ttd.slack.slack_groups import dataproc


class TestEnvironmentValidation(unittest.TestCase):
    tso = TaskServiceOperator(task_name='task_name', scrum_team=dataproc)

    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_branch_name_required_in_prodtest_environment(self, mock_get_env):
        mock_get_env.return_value = TtdEnvFactory.prodTest

        with self.assertRaises(NoBranchNameProvidedError):
            self.tso._check_configuration_valid_for_environment(branch_name=None)

    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_no_tasks_run_from_dev(self, mock_get_env):
        mock_get_env.return_value = TtdEnvFactory.dev
        with self.assertRaises(TaskServiceForbiddenError):
            self.tso._check_configuration_valid_for_environment(branch_name='alb-DATAPROC-000-create-unit-tests')

    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_no_branch_deployments_in_production(self, mock_get_env):
        mock_get_env.return_value = TtdEnvFactory.prod
        with self.assertRaises(NoBranchDeploymentsInProductionError):
            self.tso._check_configuration_valid_for_environment(branch_name='alb-DATAPROC-000-create-unit-tests')

    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_valid_branch_name_in_non_restrictive_environment(self, mock_get_env):
        mock_get_env.return_value = TtdEnvFactory.prod

        # Ensure no exception is raised
        try:
            self.tso._check_configuration_valid_for_environment()
        except Exception as e:
            self.fail(f'Unexpected exception raised: {e}')

    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_branch_name_allowed_in_prod_test(self, mock_get_env):
        mock_get_env.return_value = TtdEnvFactory.prodTest

        # Ensure no exception is raised
        try:
            self.tso._check_configuration_valid_for_environment(branch_name='alb-DATAPROC-000-create-unit-tests')
        except Exception as e:
            self.fail(f'Unexpected exception raised: {e}')


class TestCopyPreInitFromAws(unittest.TestCase):

    @patch('ttd.task_service.k8s_connection_helper.aws.deployer.get_k8s_api_client')
    @patch('ttd.task_service.k8s_connection_helper.azure.deployer.get_k8s_api_client')
    def test_secret_is_copied_if_not_present(self, mock_get_azure_client, mock_get_aws_client):
        # Arrange
        secret_data = {'secret_key': 'secret_value'}
        secret_name = 'my-test-secret'

        mock_aws_client = MagicMock()
        mock_aws_client.read_namespaced_secret.return_value.data = secret_data
        mock_get_aws_client.return_value = mock_aws_client

        mock_azure_client = MagicMock()
        mock_azure_client.copy_pre_init = True
        mock_azure_client.read_namespaced_secret.side_effect = Exception('Secret not found')
        mock_get_azure_client.return_value = mock_azure_client

        tso = TaskServiceOperator(task_name='task_name', scrum_team=dataproc, k8s_sovereign_connection_helper=azure)

        # Act
        tso._copy_pre_init_from_aws_if_needed(secret_name)

        # Assert
        mock_aws_client.read_namespaced_secret.assert_called_with(name=secret_name, namespace='task-service')

        mock_azure_client.create_namespaced_secret.assert_called_once_with(
            namespace='task-service', body=V1Secret(data=secret_data, metadata=V1ObjectMeta(name=secret_name))
        )
