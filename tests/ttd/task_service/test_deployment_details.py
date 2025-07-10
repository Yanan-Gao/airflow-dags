import unittest
from unittest.mock import patch

from kubernetes.client import ApiException

from ttd.task_service.deployment_details import (
    DeploymentDetails,
    retrieve_deployment_details_from_k8s,
    _cleanse_branch_name,
    _get_config_map_name,
    DeploymentDetailsConfigMapNotFoundError,
    ConfigMapInvalidError,
)
from ttd.ttdenv import TtdEnvFactory


class TestRetrieveFromK8s(unittest.TestCase):
    sample_valid_configmap_data = {
        'CURRENT_VERSION': '20240827.1116.174-release-2024-08-27+37a9e9660052',
        'DEPLOYMENT_ID': '793421',
        'DEPLOYMENT_TIME': '2024-08-28 09:48:57',
        'DOCKER_IMAGE': 'production.docker.adsrvr.org/ttd/taskservice:ADPLATBAMBOO-REL81-174.37a9e9660052',
        'SECRET_NAME': 'tdsc-preinit-test-nonprod-task-service-taskservicek8s-793421'
    }

    @patch('ttd.task_service.deployment_details._read_configmap')
    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_can_parse_config_map_successfully(self, mock_get_env, mock_read_configmap):
        mock_get_env.return_value = TtdEnvFactory.prod
        mock_read_configmap.return_value = self.sample_valid_configmap_data

        details = retrieve_deployment_details_from_k8s(branch_name=None, is_china=False)

        self.assertIsInstance(details, DeploymentDetails)
        self.assertEqual(details.current_version, '20240827.1116.174-release-2024-08-27+37a9e9660052')
        self.assertEqual(details.deployment_id, '793421')
        self.assertEqual(details.deployment_time, '2024-08-28 09:48:57')
        self.assertEqual(details.docker_image, 'production.docker.adsrvr.org/ttd/taskservice:ADPLATBAMBOO-REL81-174.37a9e9660052')
        self.assertEqual(details.secret_name, 'tdsc-preinit-test-nonprod-task-service-taskservicek8s-793421')

    @patch('ttd.task_service.deployment_details._read_configmap')
    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    def test_config_map_not_found_raises_exception(self, mock_get_env, mock_read_configmap):
        mock_get_env.return_value = TtdEnvFactory.prod
        mock_read_configmap.side_effect = ApiException(status=404)

        with self.assertRaises(DeploymentDetailsConfigMapNotFoundError):
            retrieve_deployment_details_from_k8s(is_china=False)

    @patch('ttd.ttdenv.TtdEnvFactory.get_from_system')
    @patch('ttd.task_service.deployment_details._k8s_client')
    @patch('ttd.task_service.deployment_details._namespace')
    @patch('ttd.task_service.deployment_details._get_config_map_name')
    def test_config_map_invalid_raises_exception(self, mock_get_config_map_name, mock_namespace, mock_k8s_client, mock_get_env):
        mock_get_env.return_value = TtdEnvFactory.prod
        mock_namespace.return_value = 'airflow2'
        mock_get_config_map_name.return_value = 'task-service-deployment-details'

        mock_k8s_client.return_value.read_namespaced_config_map.return_value.data = {
            'CURRENT_VERSION': 'version',
            'DEPLOYMENT_ID': '4472',
            'DEPLOYMENT_TIME': '2024-08-28 09:48:57',
            # 'DOCKER_IMAGE' is missing from configmap
            'SECRET_NAME': 'secret_name'
        }

        with self.assertRaises(ConfigMapInvalidError):
            retrieve_deployment_details_from_k8s(is_china=False)


class TestCleanseBranchName(unittest.TestCase):

    def test_lowercase_conversion(self):
        branch_name = "alb-DATAPROC-000-create-unit-tests"
        cleansed_name = _cleanse_branch_name(branch_name)
        self.assertEqual(cleansed_name, "alb-dataproc-000-create-unit-tests")

    def test_truncate_long_branch_name(self):
        long_branch_name = "a" * 300
        cleansed_name = _cleanse_branch_name(long_branch_name)
        self.assertEqual(len(cleansed_name), 253)
        self.assertEqual(cleansed_name, "a" * 253)

    def test_valid_branch_name_unchanged(self):
        branch_name = "alb-dataproc-000-create-unit-tests"
        cleansed_name = _cleanse_branch_name(branch_name)
        self.assertEqual(cleansed_name, branch_name)


class TestGetConfigMapName(unittest.TestCase):

    def test_prod_config_map_name_is_correct(self):
        config_map_name = _get_config_map_name()
        self.assertEqual(config_map_name, 'task-service-deployment-details')

    def test_branch_deployment_config_map_name_contains_branch(self):
        branch_name = "alb-DATAPROC-000-create-unit-tests"
        config_map_name = _get_config_map_name(branch_name=branch_name)
        self.assertEqual(config_map_name, 'task-service-deployment-details-alb-dataproc-000-create-unit-tests')

    def test_config_map_name_correct_for_alicloud_tasks(self):
        config_map_name = _get_config_map_name(is_china=True)
        self.assertEqual(config_map_name, 'task-service-deployment-details-cn')

    def test_china_branch_deployment_config_map_name_contains_branch(self):
        branch_name = "alb-DATAPROC-000-create-unit-tests"
        config_map_name = _get_config_map_name(branch_name=branch_name, is_china=True)
        self.assertEqual(config_map_name, 'task-service-deployment-details-cn-alb-dataproc-000-create-unit-tests')
