from functools import cached_property
from enum import StrEnum, auto, verify, UNIQUE
import time

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow.utils.log.secrets_masker import mask_secret
from ttd.metrics.secrets import SecretsAccessMetrics, SecretsSource, SecretsRequestStatus

from typing import Dict, Optional
import os


@verify(UNIQUE)
class SecretType(StrEnum):
    CONNECTION = auto()
    VARIABLE = auto()


class SecretAccessFailure:

    def __init__(self, variable: str, exception: Exception):
        self.variable = variable
        self.exception = exception


class DagLoadSecretsChecker:
    VIOLATIONS: Dict[str, SecretAccessFailure] = dict()
    CONTEXT: Optional[str] = None
    RUNNING_IN_CI: bool = True if os.environ.get("GITLAB_CI") is not None else False

    @staticmethod
    def set_context(path):
        DagLoadSecretsChecker.CONTEXT = path

    @staticmethod
    def add_violation(variable: str, exception: Exception):
        if (DagLoadSecretsChecker.RUNNING_IN_CI and DagLoadSecretsChecker.CONTEXT is not None):
            DagLoadSecretsChecker.VIOLATIONS[DagLoadSecretsChecker.CONTEXT] = (SecretAccessFailure(variable, exception))


class VaultSecretsBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves connections and variables from the local K8s namespace
    """
    AIRFLOW_VAULT_CONNECTION_NAME = "airflow_vault"
    VARIABLE_ALLOW_LIST = {
        "ENVIRONMENT",
        "SECRETS_BACKEND_MONITORING_ENABLED",
        "AZ_MANAGE_COMPONENTS_VIA_AMBARI",
        "AZ_ELDORADO_MI",
        "AZ_ELDORADO_ARTEFACTS_SA",
        "AZ_STANDARD_LOAD_BALANCERS_ENABLED",
        "AZ_ELDORADO_LOGS_SA",
        "AZ_VIRTUAL_NETWORK_PROFILE",
        "databricks-aws-policy-id-use",
        "databricks-aws-policy-id-vai",
        "databricks-aws-policy-id-or5",
        "databricks-aws-policy-id-jp3",
        "databricks-aws-policy-id-sg4",
        "databricks-aws-policy-id-ie2",
        "databricks-aws-policy-id-de4",
        "databricks-aws-account-id-use",
        "databricks-aws-account-id-vai",
        "databricks-aws-account-id-or5",
        "databricks-aws-account-id-jp3",
        "databricks-aws-account-id-sg4",
        "databricks-aws-account-id-ie2",
        "databricks-aws-account-id-de4",
        "databricks-aifun-allowed-instance-profiles",
    }
    CONNECTIONS_ALLOW_LIST = {"slack-emrbot", 'metrics_db_rw', 'test_metrics_db_rw', 'airflow_metrics_pusher', 'aws_dev'}

    def __init__(self):
        self.metrics_pusher = SecretsAccessMetrics()
        super().__init__()

    @cached_property
    def _kubernetes_client(self):
        from ttd.secrets.kubernetes_secrets_backend import K8sSecretsBackend

        return K8sSecretsBackend()

    @staticmethod
    def _get_current_team_context():
        from airflow.operators.python import get_current_context

        context = get_current_context()
        return context["task"].owner.upper()

    @staticmethod
    def _is_top_level_context(key):
        from airflow.operators.python import get_current_context
        from ttd.secrets import _WEB_CONTEXT

        # from airflow.utils.airflow_flask_app import get_airflow_app  # use as alternative to check if it is web.

        if len(_WEB_CONTEXT) != 0:
            return False

        try:
            context = get_current_context()
            return False
        except Exception as e:
            DagLoadSecretsChecker.add_violation(key, e)
            return True

    @cached_property
    def _vault_client(self):
        from airflow.providers.hashicorp.secrets.vault import VaultBackend

        connection = self._kubernetes_client.get_connection(self.AIRFLOW_VAULT_CONNECTION_NAME)
        role_id = connection.login
        secret_id = connection.password
        client = VaultBackend(
            url="https://vault.adsrvr.org",
            mount_point="secret",
            auth_type="approle",
            variables_path="",
            connections_path="",
            role_id=role_id,
            secret_id=secret_id,
            namespace="team-secrets"
        )
        return client

    def _get_from_vault(self, secret_type: SecretType, secret_id: str) -> Optional[str]:
        secret = None
        current_team = None
        start_time = None

        try:
            current_team = self._get_current_team_context()
            start_time = time.time()
            match secret_type:
                case SecretType.CONNECTION:
                    secret = self._vault_client.get_connection(f"SCRUM-{current_team}/Airflow/Connections/{secret_id}")
                    secret = secret.get_uri() if secret is not None else None
                case SecretType.VARIABLE:
                    secret = self._vault_client.get_variable(f"SCRUM-{current_team}/Airflow/Variables/{secret_id}")
                case _:
                    pass
            end_time = time.time()

            self.metrics_pusher.send_metrics(
                team_name=current_team,
                source=SecretsSource.VAULT,
                request_status=SecretsRequestStatus.SUCCESS,
                start_time=start_time,
                end_time=end_time
            )
        except AirflowException as ae:
            self.log.info(f'Error occurred when fetching {secret_id} from Vault')
            self.log.error(ae, exc_info=True)
        except Exception as e:
            end_time = time.time()
            self.log.error(e, exc_info=True)

            self.metrics_pusher.send_metrics(
                team_name=current_team,
                source=SecretsSource.VAULT,
                request_status=SecretsRequestStatus.ERROR,
                start_time=start_time,
                end_time=end_time
            )

        return secret

    def _get_from_k8s(self, secret_type: SecretType, secret_id: str) -> Optional[str]:
        secret = None
        start_time = None

        try:
            start_time = time.time()
            match secret_type:
                case SecretType.CONNECTION:
                    secret = self._kubernetes_client.get_connection(secret_id)
                    secret = secret.get_uri() if secret is not None else None
                case SecretType.VARIABLE:
                    secret = self._kubernetes_client.get_variable(secret_id)
                case _:
                    pass
            end_time = time.time()

            self.metrics_pusher.send_metrics(
                source=SecretsSource.KUBERNETES,
                request_status=SecretsRequestStatus.SUCCESS,
                start_time=start_time,
                end_time=end_time,
            )
        except AirflowException as ae:
            self.log.error(ae, exc_info=True)
        except Exception as e:
            end_time = time.time()
            self.log.error(e, exc_info=True)

            self.metrics_pusher.send_metrics(
                source=SecretsSource.KUBERNETES,
                request_status=SecretsRequestStatus.ERROR,
                start_time=start_time,
                end_time=end_time,
            )
        return secret

    def _get_secret(self, secret_type: SecretType, secret_id: str) -> Optional[str]:
        get_methods_list = [
            lambda key: self._get_from_vault(secret_type, key),
            lambda key: self._get_from_k8s(secret_type, key),
        ]
        secret = None
        for get_method in get_methods_list:
            secret = get_method(secret_id)
            if secret is not None:
                mask_secret(secret)
                break

        return secret

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """
        Get connection from the team airflow vault first, and otherwise
        from the CONNECTION_SECRET_NAME secrets in the current K8s namespace

        If the connection is for the whole platform, we just retrieve from K8s immediately.
        """
        if conn_id in self.CONNECTIONS_ALLOW_LIST:
            try:
                return self._kubernetes_client.get_connection(conn_id)
            except Exception as e:
                return None

        if self._is_top_level_context(conn_id):
            raise Exception(f"Not allowed to fetch connection at the top level: {conn_id}")

        secret_uri = self._get_secret(SecretType.CONNECTION, conn_id)
        return None if secret_uri is None else Connection(conn_id=conn_id, uri=secret_uri)

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get variable from the team airflow vault first, and otherwise from
        the VARIABLE_CONFIG_MAP_NAME config map in the current K8s namespace
        """
        if key in self.VARIABLE_ALLOW_LIST:
            try:
                return self._kubernetes_client.get_variable(key)
            except Exception as e:
                return None

        if self._is_top_level_context(key):
            raise Exception(f"Not allowed to fetch variable at the top level: {key}")

        return self._get_secret(SecretType.VARIABLE, key)
