import unittest
from unittest.mock import patch, MagicMock
from airflow.models import Connection, Variable
from ttd.secrets.kubernetes_secrets_backend import K8sSecretsBackend
from base64 import b64encode


@patch("kubernetes.client.CoreV1Api", autospec=True)
@patch("kubernetes.config", autospec=True)
@patch("ttd.secrets.kubernetes_secrets_backend.K8sSecretsBackend.namespace", autospec=True)
class TestK8sSecretsBackend(unittest.TestCase):
    conn_id = "example-connection"
    connection = Connection(conn_id=conn_id, host="google.com", password="password")

    conn_uri = connection.get_uri()

    var_id = "example-variable"
    var_val = "what-goes-here?"
    variable = Variable(key=var_id, val=var_val)

    def arrange(self, mock_namespace, mock_config, mock_corev1api) -> None:
        mock_namespace.return_value = "airflow2"
        mock_corev1api_instance = mock_corev1api.return_value

        mock_corev1api_instance.read_namespaced_secret.return_value = MagicMock()
        mock_corev1api_instance.read_namespaced_secret.return_value.data = {self.conn_id: b64encode(bytes(self.conn_uri, "utf-8"))}

        mock_corev1api_instance.read_namespaced_config_map.return_value = MagicMock()
        mock_corev1api_instance.read_namespaced_config_map.return_value.data = {self.var_id: self.var_val}

        self.backend = K8sSecretsBackend()

    def test_get_connection_success(self, mock_namespace, mock_config, mock_corev1api):
        self.arrange(mock_namespace, mock_config, mock_corev1api)
        # Act
        returned_connection = self.backend.get_connection(self.conn_id)

        # Assert
        self.assertEqual(returned_connection.get_uri(), self.conn_uri)

    def test_get_connection_failure(self, mock_namespace, mock_config, mock_corev1api):
        # Arrange
        self.arrange(mock_namespace, mock_config, mock_corev1api)

        # Act
        returned_connection = self.backend.get_connection("different-conn-id")

        # Assert
        self.assertEqual(returned_connection, None)

    def test_get_variable_success(self, mock_namespace, mock_config, mock_corev1api):
        self.arrange(mock_namespace, mock_config, mock_corev1api)
        # Act
        returned_variable_value = self.backend.get_variable(self.var_id)

        # Assert
        self.assertEqual(returned_variable_value, self.var_val)

    def test_get_variable_failure(self, mock_namespace, mock_config, mock_corev1api):
        # Arrange
        self.arrange(mock_namespace, mock_config, mock_corev1api)

        # Act
        returned_connection = self.backend.get_variable("different-var-id")

        # Assert
        self.assertEqual(returned_connection, None)
