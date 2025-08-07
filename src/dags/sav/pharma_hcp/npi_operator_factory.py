"""
Factory functions for creating standardized NPI pipeline operators.
"""
from typing import Dict, Optional, Any

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.base import TtdDag

from dags.sav.pharma_hcp.npi_common_config import NpiCommonConfig


class NpiOperatorFactory:
    """Factory for creating standardized NPI pipeline operators."""

    @staticmethod
    def create_standard_npi_task(
        task_id: str,
        task_name: str,
        generator_task_type: str,
        dag: TtdDag,
        workload_size: str = "standard",
        additional_env_vars: Optional[Dict[str, Any]] = None,
        delete_pod_on_finish: bool = True
    ) -> OpTask:
        """
        Create a standardized NPI task with common configuration.

        Args:
            task_id: Unique task identifier
            task_name: Human-readable task name
            generator_task_type: NPI application generator task type
            dag: Airflow DAG instance
            workload_size: Resource tier ('light', 'standard')
            additional_env_vars: Extra environment variables
            delete_pod_on_finish: Whether to delete the pod on finish

        Returns:
            OpTask configured with TtdKubernetesPodOperator
        """
        env_config = NpiCommonConfig.get_environment_config()
        pod_resources = NpiCommonConfig.create_pod_resources(workload_size=workload_size)

        secrets = NpiCommonConfig.get_common_secrets()

        env_vars = {
            "SPRING_PROFILES_ACTIVE": env_config.profile,
            "TTD_NPI_APPLICATION_GENERATORTASKTYPE": generator_task_type,
            "TTD_NPI_APPLICATION_GENERATORTASKTRIGGER": NpiCommonConfig.GENERATOR_TASK_TRIGGER,
            "JAVA_OPTS": NpiCommonConfig.get_java_opts(workload_size)
        }

        if additional_env_vars:
            env_vars.update(additional_env_vars)

        operator_kwargs = {
            "kubernetes_conn_id": env_config.k8s_connection_id,
            "namespace": env_config.k8s_namespace,
            "service_account_name": env_config.service_account,
            "image": NpiCommonConfig.DOCKER_IMAGE,
            "image_pull_policy": "Always",
            "name": task_name,
            "task_id": task_id,
            "dnspolicy": "ClusterFirst",
            "get_logs": True,
            "dag": dag,
            "startup_timeout_seconds": NpiCommonConfig.DEFAULT_STARTUP_TIMEOUT,
            "execution_timeout": NpiCommonConfig.DEFAULT_EXECUTION_TIMEOUT,
            "on_finish_action": "delete_pod" if delete_pod_on_finish else "keep_pod",
            "log_events_on_failure": True,
            "resources": pod_resources,
            "secrets": secrets,
            "env_vars": env_vars,
            "security_context": NpiCommonConfig.SECURITY_CONTEXT
        }

        ephemeral_volume = NpiCommonConfig.create_ephemeral_volume_config(task_name=task_name, workload_size=workload_size)
        operator_kwargs.update({"volumes": [ephemeral_volume.create_volume()], "volume_mounts": [ephemeral_volume.create_volume_mount()]})

        return OpTask(op=TtdKubernetesPodOperator(**operator_kwargs))
