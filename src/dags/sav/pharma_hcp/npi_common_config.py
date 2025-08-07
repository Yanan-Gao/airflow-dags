"""
Common configuration for NPI (National Provider Identifier) pipeline DAGs.
"""
from datetime import timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import uuid

from airflow.kubernetes.secret import Secret
from kubernetes.client import V1PodSecurityContext

from ttd.kubernetes.pod_resources import PodResources
from ttd.kubernetes.ephemeral_volume_config import EphemeralVolumeConfig
from ttd.ttdenv import TtdEnvFactory


@dataclass
class NpiEnvironmentConfig:
    """Environment-specific configuration for NPI pipelines."""

    profile: str
    k8s_connection_id: Optional[str]
    k8s_namespace: str
    service_account: str
    spark_env_path: str


@dataclass
class NpiResourceConfig:
    """Resource configuration for different pipeline workload sizes."""

    request_cpu: str
    request_memory: str
    limit_memory: str
    request_ephemeral_storage: str
    limit_ephemeral_storage: str
    volume_storage: str


class NpiCommonConfig:
    """Centralized configuration for NPI pipelines."""

    DOCKER_IMAGE = "production.docker.adsrvr.org/ttd/npireporting:latest"
    NOTIFICATION_SLACK_CHANNEL = "#scrum-sav-alerts"
    GENERATOR_TASK_TRIGGER = "Airflow"

    DEFAULT_STARTUP_TIMEOUT = 600
    DEFAULT_EXECUTION_TIMEOUT = timedelta(hours=5)
    DEFAULT_RETRIES = 3
    DEFAULT_RETRY_DELAY = timedelta(minutes=1)

    SECURITY_CONTEXT = V1PodSecurityContext(run_as_user=1000, fs_group=1000)

    @staticmethod
    def get_environment_config() -> NpiEnvironmentConfig:
        """Get environment-specific configuration based on TTD environment."""
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
            return NpiEnvironmentConfig(
                profile="staging",
                k8s_connection_id=None,
                k8s_namespace="npi-reporting-dev",
                service_account="crm-data-service-account-staging",
                spark_env_path="test"
            )
        else:
            return NpiEnvironmentConfig(
                profile="prod",
                k8s_connection_id="npi-reporting-k8s",
                k8s_namespace="npi-reporting",
                service_account="crm-data-service-account-prod",
                spark_env_path="prod"
            )

    @staticmethod
    def get_resource_config(workload_size: str = "standard") -> NpiResourceConfig:
        """
        Get standardized resource configuration.

        Args:
            workload_size: One of 'light', 'standard'
        """
        env_config = NpiCommonConfig.get_environment_config()
        is_staging = env_config.profile == "staging"

        if workload_size == "light":
            return NpiResourceConfig(
                request_cpu="1",
                request_memory="8Gi" if not is_staging else "4Gi",
                limit_memory="8Gi" if not is_staging else "4Gi",
                request_ephemeral_storage="8Gi" if not is_staging else "4Gi",
                limit_ephemeral_storage="8Gi" if not is_staging else "4Gi",
                volume_storage="16Gi" if not is_staging else "8Gi"
            )
        elif workload_size == "standard":
            return NpiResourceConfig(
                request_cpu="2" if not is_staging else "1",
                request_memory="64Gi" if not is_staging else "24Gi",
                limit_memory="64Gi" if not is_staging else "24Gi",
                request_ephemeral_storage="8Gi" if not is_staging else "4Gi",
                limit_ephemeral_storage="8Gi" if not is_staging else "4Gi",
                volume_storage="64Gi" if not is_staging else "32Gi"
            )
        else:
            raise ValueError(f"Unknown workload_size: {workload_size}")

    @staticmethod
    def create_pod_resources(workload_size: str = "standard") -> PodResources:
        """
        Create a pod resource.

        Args:
            workload_size: Resource tier ('light', 'standard')
        """
        resource_config = NpiCommonConfig.get_resource_config(workload_size)

        return PodResources.from_dict(
            request_cpu=resource_config.request_cpu,
            request_memory=resource_config.request_memory,
            limit_memory=resource_config.limit_memory,
            request_ephemeral_storage=resource_config.request_ephemeral_storage,
            limit_ephemeral_storage=resource_config.request_ephemeral_storage
        )

    @staticmethod
    def create_ephemeral_volume_config(task_name: str, workload_size: str = "standard") -> EphemeralVolumeConfig:
        """Create ephemeral volume configuration for manual volume management."""
        resource_config = NpiCommonConfig.get_resource_config(workload_size)

        volume_name = f"{task_name.replace('_', '-')}-{uuid.uuid4().hex[:8]}-ephemeral-storage"
        return EphemeralVolumeConfig(
            name=volume_name,
            storage_request=resource_config.volume_storage,
            storage_class_name="standard-ssd-encrypted",
            mount_path="/tmp/data"
        )

    @staticmethod
    def get_common_secrets() -> List[Secret]:
        """Get common secrets used across NPI pipelines."""
        env_config = NpiCommonConfig.get_environment_config()
        profile_upper = env_config.profile.upper()

        return [
            Secret(
                deploy_type="env",
                deploy_target="TTD_SPRING_DATASOURCE_SNOWFLAKE_PASSWORD",
                secret="npi-datasource",
                key=f"{profile_upper}_SNOWFLAKE_PASSWORD"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_SPRING_DATASOURCE_PROVISIONING_PASSWORD",
                secret="npi-datasource",
                key=f"{profile_upper}_PROVISIONING_PASSWORD"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_SPRING_DATASOURCE_LOGWORKFLOW_PASSWORD",
                secret="npi-datasource",
                key=f"{profile_upper}_LOGWORKFLOW_PASSWORD"
            ),
            Secret(deploy_type="env", deploy_target="TTD_NPI_S3_SWOOP_ACCESSKEY", secret="npi-datasource", key="S3_SWOOP_ACCESSKEY"),
            Secret(deploy_type="env", deploy_target="TTD_NPI_S3_SWOOP_SECRETKEY", secret="npi-datasource", key="S3_SWOOP_SECRETKEY"),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SWOOPAPI_CLIENTID",
                secret="npi-datasource",
                key=f"{profile_upper}_SWOOPAPI_CLIENTID"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SWOOPAPI_CLIENTSECRET",
                secret="npi-datasource",
                key=f"{profile_upper}_SWOOPAPI_CLIENTSECRET"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_PULLING_SFTPCONFIGJSON",
                secret="npi-datasource",
                key=f"{profile_upper}_TTD_PULLING_SFTP_TOKEN"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_TTDAPI_SERVICEACCOUNTTOKEN",
                secret="npi-datasource",
                key=f"{profile_upper}_TTD_API_TOKEN"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SNOWFLAKEDATALOADING_PROVIDERS_THROTLEIQVIA_ACCESSKEY",
                secret="npi-datasource",
                key=f"{profile_upper}_SNOWFLAKEDATALOADINGACCESSKEY"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SNOWFLAKEDATALOADING_PROVIDERS_THROTLEIQVIA_SECRETKEY",
                secret="npi-datasource",
                key=f"{profile_upper}_SNOWFLAKEDATALOADINGSECRETKEY"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SNOWFLAKEDATALOADING_PROVIDERS_SWOOP_ACCESSKEY",
                secret="npi-datasource",
                key="S3_SWOOP_ACCESSKEY"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SNOWFLAKEDATALOADING_PROVIDERS_SWOOP_SECRETKEY",
                secret="npi-datasource",
                key="S3_SWOOP_SECRETKEY"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SNOWFLAKEDATALOADING_PROVIDERS_THROTLEDIRECT_ACCESSKEY",
                secret="npi-datasource",
                key=f"{profile_upper}_SNOWFLAKEDATALOADINGACCESSKEY"
            ),
            Secret(
                deploy_type="env",
                deploy_target="TTD_NPI_SNOWFLAKEDATALOADING_PROVIDERS_THROTLEDIRECT_SECRETKEY",
                secret="npi-datasource",
                key=f"{profile_upper}_SNOWFLAKEDATALOADINGSECRETKEY"
            )
        ]

    @staticmethod
    def get_java_opts(workload_size: str = "standard") -> str:
        """
        Get optimized Java options.
        """
        resource_config = NpiCommonConfig.get_resource_config(workload_size)

        memory_gb = int(resource_config.request_memory.replace("Gi", ""))
        heap_memory = int(memory_gb * 0.8)

        return (f"-Xms{heap_memory}g "
                f"-Xmx{heap_memory}g "
                "-Djava.io.tmpdir=/tmp/data "
                "-XX:+UseG1GC "
                "-XX:MaxGCPauseMillis=200 ")

    @staticmethod
    def get_s3_paths() -> Dict[str, str]:
        """Get standardized S3 paths for NPI pipelines."""
        env_config = NpiCommonConfig.get_environment_config()
        spark_env_path = env_config.spark_env_path

        return {
            "iqvia_id_export_path": f"s3a://ttd-sav-pii/env={spark_env_path}/npi_matchrate/idexport/",
            "daily_id_output_root": f"s3a://ttd-sav-pii/env={spark_env_path}/npi_matchrate/idexport/",
            "full_id_output_root": f"s3a://ttd-sav-pii/env={spark_env_path}/npi_matchrate/active/v1/",
        }
