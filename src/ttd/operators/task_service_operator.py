import os
from datetime import timedelta
from functools import reduce
from typing import Optional, List, Dict

from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes import kube_client
from airflow.utils.state import State

from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1EnvVar, V1EmptyDirVolumeSource, V1CSIVolumeSource

from kubernetes.client.models import (
    V1VolumeMount,
    V1Secret,
    V1ObjectMeta,
    V1Volume,
)

from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import SlackTeam
from ttd.task_service.alerting_metrics import (
    AlertingMetricsMixin,
    AlertingMetricsConfiguration,
)
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.kubernetes.pod_resources import PodResources
from ttd.task_service.k8s_connection_helper import (
    aws,
    K8sSovereignConnectionHelper,
    alicloud,
)
from ttd.task_service.persistent_storage.persistent_storage import (
    PersistentStorageFactory,
)
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
    PersistentStorageType,
)
from ttd.task_service.persistent_storage.persistent_storage_handler import (
    PersistentStorageHandler,
)
from ttd.task_service.deployment_details import retrieve_deployment_details_from_k8s
from ttd.task_service.vertica_clusters import VerticaCluster
from ttd.ttdenv import TtdEnvFactory


class TaskServiceOperator(TtdKubernetesPodOperator, AlertingMetricsMixin):
    ui_color = "#1d8880"
    ui_fgcolor = "#f5f5d0"

    default_mount_path = "/opt/ttd/etc/"
    default_kubectl_configs_path = "/tmp/tmp_kubeconfigs/"
    task_service_namespace = "task-service"

    def __init__(
        self,
        task_name: str,
        scrum_team: SlackTeam,
        task_config_name: Optional[str] = None,
        task_data: Optional[str] = None,
        task_name_suffix: Optional[str] = None,
        branch_name: Optional[str] = None,
        k8s_sovereign_connection_helper: K8sSovereignConnectionHelper = aws,
        resources: PodResources = TaskServicePodResources.medium(),
        persistent_storage_config: Optional[PersistentStorageConfig] = None,
        alerting_configuration: Optional[AlertingMetricsConfiguration] = AlertingMetricsConfiguration(),
        retries: int = 2,
        retry_delay: timedelta = timedelta(minutes=5),
        task_execution_timeout: timedelta = timedelta(hours=1),
        configuration_overrides: Optional[Dict[str, str]] = None,
        telnet_commands: Optional[List[str]] = None,
        vertica_cluster: Optional[VerticaCluster] = None,
        k8s_annotations: Optional[Dict[str, str]] = None,
        service_name: Optional[str] = None,
        *args,
        **kwargs,
    ):
        self.airflow_task_name = TaskServiceOperator.format_task_name(task_name, task_name_suffix)
        self.k8s_sovereign_connection_helper = k8s_sovereign_connection_helper
        self.branch_name = branch_name
        self.scrum_team = scrum_team
        self.jira_key = scrum_team.jira_team.upper()
        self.alerting_configuration = alerting_configuration
        self.service_name = service_name
        self.volumes = []
        self.volume_mounts = []

        empty_dir_volume = V1Volume(name="empty-dir", empty_dir=V1EmptyDirVolumeSource())
        empty_dir_mount = V1VolumeMount(
            name=empty_dir_volume.name,
            mount_path=f"{self.__class__.default_mount_path}TTD.Domain.TaskExecution.TaskService.Host",
            sub_path=None,
            read_only=False,
        )
        application_data_dir_mount = V1VolumeMount(
            name=empty_dir_volume.name,
            mount_path=f"{self.__class__.default_mount_path}TaskService/ApplicationData/",
            sub_path=None,
            read_only=False,
        )
        spire_agent_socket_volume = V1Volume(name="spire-agent-socket", csi=V1CSIVolumeSource(driver="csi.spiffe.io", read_only=True))
        spire_agent_socket_mount = V1VolumeMount(
            name=spire_agent_socket_volume.name,
            mount_path="/run/spire",
            sub_path=None,
            read_only=True,
        )
        self.volumes.extend([empty_dir_volume, spire_agent_socket_volume])
        self.volume_mounts.extend([empty_dir_mount, application_data_dir_mount, spire_agent_socket_mount])

        self.persistent_storage = None
        self._configure_persistent_storage_if_provided(
            config=persistent_storage_config,
            task_name=task_name,
            task_suffix=task_name_suffix,
        )

        command_arguments = ["--task-name", task_name, "--scrum-team", self.jira_key]
        if task_config_name is not None:
            command_arguments.extend(["--config-name", task_config_name])
        if task_data is not None:
            command_arguments.extend(["--task-data", task_data])
        if vertica_cluster is not None:
            command_arguments.extend(["--vertica-cluster", vertica_cluster.name])

        env_vars = self._generate_environment_variables(configuration_overrides, telnet_commands)

        self.airflow_task_name = TaskServiceOperator.format_task_name(task_name, task_name_suffix)

        all_annotations = {
            "sumologic.com/include": "true",
            "sumologic.com/sourceCategory": "task-service-k8s",
            "sumologic.com/sourceName": self.airflow_task_name,
            "karpenter.sh/do-not-evict": "true",
            "karpenter.sh/do-not-disrupt": "true",
        }
        if k8s_annotations:
            all_annotations.update(**k8s_annotations)

        AlertingMetricsMixin.__init__(self, alerting_configuration)

        k8s_connection_id = "kubernetes_default" if self.k8s_sovereign_connection_helper.executer is None \
            else self.k8s_sovereign_connection_helper.executer.connection_name
        TtdKubernetesPodOperator.__init__(
            self,
            kubernetes_conn_id=k8s_connection_id,
            namespace=self.task_service_namespace,
            get_logs=True,
            log_events_on_failure=True,
            is_delete_operator_pod=True,
            startup_timeout_seconds=2 * 60 if self.k8s_sovereign_connection_helper == aws else 8 * 60,
            image="will-replace-on-execute",
            name=self.airflow_task_name,
            task_id=self.airflow_task_name,
            arguments=command_arguments,
            secrets=[],
            volumes=self.volumes,
            volume_mounts=self.volume_mounts,
            retries=retries,
            retry_delay=retry_delay,
            execution_timeout=task_execution_timeout,
            annotations=all_annotations,
            resources=resources,
            env_vars=env_vars,
            service_name=service_name,
            *args,
            **kwargs,
        )

    @staticmethod
    def format_task_name(name: str, suffix: Optional[str]) -> str:
        name = reduce(lambda x, y: x + ("-" if y.isupper() else "") + y, name).lower()
        return name if suffix is None else name + "-" + suffix

    def _generate_environment_variables(
        self,
        configuration_overrides: Optional[Dict[str, str]] = None,
        telnet_commands: Optional[List[str]] = None,
    ) -> List[V1EnvVar]:
        telnet_commands = telnet_commands or []
        configuration_overrides = configuration_overrides or {}

        global_configuration_overrides: Dict[str, str] = {
            "SpiffeServiceName": self.service_name if self.service_name is not None else '',
            "PodNamespace": self.task_service_namespace,
        }
        global_telnet_commands = ["changeField VerticaMultiClusterConnectionHelper.ReadVerticaConfigFromLwdbFeatureSwitch true"]
        global_env_vars = {"TTD_ENVIRONMENT": "Production"}

        self.telnet_commands = telnet_commands + global_telnet_commands
        self.configuration_overrides = {
            **configuration_overrides,
            **global_configuration_overrides,
        }

        self.configuration_overrides["TelnetCommandServer.CommandsFromAirflow"] = r" \n ".join(self.telnet_commands)
        env_var_overrides = {"TaskService_" + key: value for key, value in self.configuration_overrides.items()}

        env_vars = {**env_var_overrides, **global_env_vars}

        return [V1EnvVar(name=name, value=value) for name, value in env_vars.items()]

    def _configure_persistent_storage_if_provided(
        self,
        config: Optional[PersistentStorageConfig] = None,
        task_name: Optional[str] = None,
        task_suffix: Optional[str] = None,
    ) -> None:
        if config is None:
            return

        config.base_name = config.base_name or self.airflow_task_name
        config.mount_path = config.mount_path or os.path.join(self.default_mount_path, "pv", config.storage_type.value)
        config.annotations["task-name"] = task_name or self.airflow_task_name
        config.annotations["task-suffix"] = task_suffix or ""
        config.annotations["team"] = self.jira_key
        config.annotations["slack-alarm-channel"] = self.scrum_team.alarm_channel

        storage = PersistentStorageFactory.create_storage(
            config=config,
            cloud_provider=self.k8s_sovereign_connection_helper.cloud_provider,
        )
        storage_volume, storage_volume_mounts = storage.get_volume_configs()

        self.persistent_storage = storage
        self.volumes.append(storage_volume)
        self.volume_mounts.append(storage_volume_mounts)

    def _copy_pre_init_from_aws_if_needed(self, secret_name: str) -> None:
        if not self.k8s_sovereign_connection_helper.copy_pre_init:
            return

        deployer_client = (self.k8s_sovereign_connection_helper.deployer.get_k8s_api_client())
        if self._is_secret_created(deployer_client, secret_name):
            return

        pre_init_secret_data = (
            aws.deployer.get_k8s_api_client().read_namespaced_secret(name=secret_name, namespace=self.task_service_namespace).data
        )

        self.log.info(f"No pre_init secret found. Copying from AWS. To be copied: {secret_name}")

        deployer_client.create_namespaced_secret(
            namespace=self.task_service_namespace,
            body=V1Secret(
                data=pre_init_secret_data,
                metadata=V1ObjectMeta(name=secret_name),
            ),
        )

    def _is_secret_created(self, k8s_client, secret_name: str) -> bool:
        existing_secrets = k8s_client.list_namespaced_secret(self.task_service_namespace).items
        for secret in existing_secrets:
            if secret.metadata.name == secret_name:
                self.log.info(f"Secret with name {secret_name} found.")
                return True
        return False

    def _check_configuration_valid_for_environment(self, branch_name: Optional[str] = None) -> None:
        environment = TtdEnvFactory.get_from_system()

        if environment in (TtdEnvFactory.test, TtdEnvFactory.prodTest) and branch_name is None:
            raise NoBranchNameProvidedError(
                "When running in the test or prodTest environment you must use a branch deployment. "
                "Provide a branch_name."
            )

        if environment == TtdEnvFactory.dev:
            raise TaskServiceForbiddenError("Running tasks is not possible in the dev environment")

        if environment == TtdEnvFactory.prod and branch_name is not None:
            raise NoBranchDeploymentsInProductionError("When running on production, you can't use a branch deployment.")

        if branch_name is not None:
            self.log.info(f"Running from branch: {branch_name}")

    def _get_pre_init_secret_obj(self, secret_name: str) -> Secret:
        from kubernetes.client import V1KeyToPath

        return Secret(
            deploy_type='volume',
            deploy_target=self.__class__.default_mount_path,
            secret=secret_name,
            items=[V1KeyToPath(key='tdsc', path='TaskServicePreInit.txt')]
        )

    def execute(self, context):
        self._check_configuration_valid_for_environment(self.branch_name)

        deployment_details = retrieve_deployment_details_from_k8s(self.branch_name, self.k8s_sovereign_connection_helper == alicloud)

        self.secrets = [self._get_pre_init_secret_obj(deployment_details.secret_name)]
        self.image = deployment_details.docker_image

        self.log.info(f"TaskService version: {deployment_details.current_version}")

        final_state = State.FAILED
        persistent_storage_handler = PersistentStorageHandler(
            client=kube_client.get_kube_client() if self.k8s_sovereign_connection_helper ==
            aws else self.k8s_sovereign_connection_helper.executer.get_k8s_api_client()
        )
        try:
            self._copy_pre_init_from_aws_if_needed(deployment_details.secret_name)

            self.log.info(f"Following telnet commands applied: {self.telnet_commands}")
            self.log.info(f"Following overrides applied: {self.configuration_overrides}")

            self.on_start(context=context, cloud_provider=self.k8s_sovereign_connection_helper.cloud_provider, jira_key=self.jira_key)

            if self.persistent_storage is not None:
                persistent_storage_handler.provision_pvc(self.persistent_storage)

            super(TaskServiceOperator, self).execute(context)
            final_state = State.SUCCESS
        finally:
            self.on_finish(
                context=context,
                cloud_provider=self.k8s_sovereign_connection_helper.cloud_provider,
                scrum_team=self.scrum_team,
                state=final_state,
            )

            if self.config_file is not None:
                os.remove(self.config_file)

            if self.persistent_storage is not None and self.persistent_storage.config.storage_type == PersistentStorageType.ONE_POD_TEMP:
                persistent_storage_handler.delete_pvc(self.persistent_storage)


class NoBranchNameProvidedError(AirflowFailException):
    pass


class NoBranchDeploymentsInProductionError(AirflowFailException):
    pass


class TaskServiceForbiddenError(AirflowFailException):
    pass
