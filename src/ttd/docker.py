import abc
from datetime import timedelta
from typing import Dict, List, Optional, Sequence, Tuple, Union

from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.aws.emr_task_visitor import EmrTaskVisitor
from ttd.eldorado.emr_cluster_scaling_properties import \
    EmrClusterScalingProperties
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.openlineage import OpenlineageConfig
from ttd.ttdenv import TtdEnv, TtdEnvFactory

INTERNAL_DOCKER_REGISTRY = "internal.docker.adsrvr.org"
PRODUCTION_DOCKER_REGISTRY = "production.docker.adsrvr.org"

DEFAULT_INTERNAL_DOCKER_BOOTSTRAP_SCRIPT = "s3://ttd-build-artefacts/mlops/scripts/docker/v1/install_docker_gpu.sh"
DEFAULT_COPY_ENTRYPOINT_FROM_IMAGE = "s3://ttd-build-artefacts/mlops/scripts/copy_from_image/v1/copy_entrypoint_from_image.sh"
DEFAULT_CGROUP = "s3://ttd-build-artefacts/mlops/scripts/docker/gpu/v1/cgroup.sh"
DEFAULT_DOCKER_IDLE_PREVENTION = "s3://ttd-build-artefacts/mlops/scripts/docker/gpu/v1/setup-docker-idle-prevention-cron.sh"

OPEN_LINEAGE_PACKAGE = "io.openlineage:openlineage-spark:0.19.2"


class DockerSetupBootstrapScripts:

    def __init__(
        self,
        install_docker_gpu_location: Optional[str] = DEFAULT_INTERNAL_DOCKER_BOOTSTRAP_SCRIPT,
        copy_entrypoint_from_image_location: Optional[str] = DEFAULT_COPY_ENTRYPOINT_FROM_IMAGE,
        cgroup_location: Optional[str] = DEFAULT_CGROUP,
        docker_idle_protection: Optional[str] = DEFAULT_DOCKER_IDLE_PREVENTION,
    ):

        self.install_docker_gpu_location = install_docker_gpu_location
        self.copy_entrypoint_from_image_location = copy_entrypoint_from_image_location
        self.cgroup_location = cgroup_location
        self.docker_idle_protection = docker_idle_protection


class DockerCommandBuilder:

    def __init__(
        self,
        docker_image_name: str,
        docker_registry: str = "internal.docker.adsrvr.org",
        docker_image_tag: str = "latest",
        execution_language: str = "python3",
        path_to_app: str = "lib/app/job.py",
        additional_parameters: Optional[List[str]] = None,
        additional_execution_parameters: Optional[List[str]] = None,
    ):
        self.docker_image_name = docker_image_name
        self.docker_registry = docker_registry
        self.docker_image_tag = docker_image_tag
        self.execution_language = execution_language
        self.path_to_app = path_to_app

        self.additional_parameters = additional_parameters or []

        self.additional_execution_parameters = additional_execution_parameters or []

    def build_command(self) -> str:
        base_command = "docker run"
        entry = f"--entrypoint {self.execution_language} {self.docker_registry}/{self.docker_image_name}:{self.docker_image_tag} {self.path_to_app}"
        arguments = " ".join(str(x) for x in self.additional_parameters if x)
        execution_arguments = " ".join(str(x) for x in self.additional_execution_parameters if x)
        return f"{base_command} {arguments} {entry} {execution_arguments}"


class DockerRunEmrTask(EmrJobTask):

    def __init__(
        self,
        name: str,
        docker_run_command: Union[str, DockerCommandBuilder],
        timeout_timedelta: timedelta = timedelta(hours=4),
        action_on_failure: str = "TERMINATE_CLUSTER",
        configure_cluster_automatically: bool = False,
        cluster_specs: Optional[EmrClusterSpecs] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
        do_xcom_push: bool = False,
        xcom_json_path: Optional[str] = None,
    ):
        """
        Run a `docker run` command for a step

        :param name: Name of the task
        :param docker_run_command: The string for the docker run command (i.e. "docker run --arg ...")
        :param timeout_timedelta:
        :param action_on_failure: What to do when the task fails
        :param configure_cluster_automatically: Configure the cluster automatically
        :param cluster_specs: Cluster config
        :param cluster_calc_defaults: Class to use to calculate the defaults
        :param do_xcom_push see parent class
        :param xcom_json_path see parent class
        """

        command_line_arguments = ["-c"]

        if isinstance(docker_run_command, str):
            command_line_arguments.append(docker_run_command)
        else:
            self.docker_run_builder = docker_run_command
            command_line_arguments.append(docker_run_command.build_command())

        super().__init__(
            name,
            class_name=None,
            executable_path="bash",
            command_line_arguments=command_line_arguments,
            timeout_timedelta=timeout_timedelta,
            action_on_failure=action_on_failure,
            configure_cluster_automatically=configure_cluster_automatically,
            cluster_specs=cluster_specs,
            cluster_calc_defaults=cluster_calc_defaults,
            do_xcom_push=do_xcom_push,
            xcom_json_path=xcom_json_path,
        )


class PySparkEmrTask(EmrJobTask):

    def __init__(
        self,
        name: str,
        entry_point_path: str,
        image_name: str,
        image_tag: Optional[str] = "latest",
        docker_registry: Optional[str] = PRODUCTION_DOCKER_REGISTRY,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        command_line_arguments: Optional[List[str]] = None,
        timeout_timedelta: timedelta = timedelta(hours=4),
        action_on_failure: str = "TERMINATE_CLUSTER",
        configure_cluster_automatically: bool = False,
        cluster_specs: EmrClusterSpecs = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
        packages: Optional[List[str]] = None,
        python_distribution: str = "python3",
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        do_xcom_push: bool = False,
        xcom_json_path: Optional[str] = None,
    ):
        """
        Add a pyspark step using docker for dependency management

        :param name: Name of the task
        :param entry_point_path: Path to the entrypoint on the cluster
        :param image_name: Name of the docker image to run
        :param image_tag: Tag of the docker image to run
        :param docker_registry: The registry where this docker image is published
        :param eldorado_config_option_pairs_list: Configuration pairs
        :param additional_args_option_pairs_list: Spark configurations
        :param command_line_arguments: Additional command line arguments
        :param timeout_timedelta: Timeout on the cluster
        :param action_on_failure: What to do if the step fails
        :param configure_cluster_automatically: Whether to configure the cluster automatically
        :param cluster_specs: The config
        :param cluster_calc_defaults: Class instance to calculate defaults with
        :param packages: List of packages to load onto the spark cluster
        :param do_xcom_push see parent class
        :param xcom_json_path see parent class
        """
        full_docker_image_name = f"{docker_registry}/{image_name}:{image_tag}"

        base_configs = [
            ("master", "yarn"),
            ("executor-cores", "4"),
            ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
            ("conf", "spark.executor.defaultJavaOptions=-XshowSettings"),
            ("conf", "spark.sql.shuffle.partitions=3000"),
            ("conf", "spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker"),
            (
                "conf",
                f"spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE={full_docker_image_name}",
            ),
            ("conf", "spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker"),
            (
                "conf",
                f"spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE={full_docker_image_name}",
            ),
            ("conf", f"spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON={python_distribution}"),
            ("conf", f"spark.yarn.appMasterEnv.PYSPARK_PYTHON={python_distribution}"),
            (
                "conf",
                f"spark.yarn.appMasterEnv.ttdenv={cluster_specs.environment if cluster_specs else TtdEnvFactory.get_from_system()}",
            ),
            ("num-executors", "2"),
        ]

        if packages is None:
            packages = []

        if len(packages) > 0:
            base_configs.append(("packages", ",".join(packages)))

        base_configs.extend(additional_args_option_pairs_list)  # type: ignore

        super().__init__(
            name,
            class_name=None,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=base_configs,
            executable_path=entry_point_path,
            command_line_arguments=command_line_arguments,
            timeout_timedelta=timeout_timedelta,
            action_on_failure=action_on_failure,
            configure_cluster_automatically=configure_cluster_automatically,
            cluster_specs=cluster_specs,
            cluster_calc_defaults=cluster_calc_defaults,
            job_jar="command-runner.jar",
            is_pyspark=True,
            openlineage_config=openlineage_config,
            do_xcom_push=do_xcom_push,
            xcom_json_path=xcom_json_path,
        )


class DockerEmrClusterTask(EmrClusterTask):

    def __init__(
        self,
        name: str,
        image_name: str,
        image_tag: str,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes,
        cluster_tags: Dict[str, str],
        core_fleet_instance_type_configs: EmrFleetInstanceTypes,
        docker_registry: str = PRODUCTION_DOCKER_REGISTRY,
        entrypoint_in_image: Optional[str] = None,
        ec2_subnet_ids: List[str] = None,
        log_uri: Optional[str] = None,
        emr_release_label: Optional[str] = None,
        ec2_key_name: Optional[str] = None,
        instance_configuration_spark_log4j: dict = None,
        additional_application_configurations: Optional[List[dict]] = None,
        emr_managed_master_security_group: Optional[str] = None,
        emr_managed_slave_security_group: Optional[str] = None,
        service_access_security_group: Optional[str] = None,
        use_on_demand_on_timeout: bool = False,
        bootstrap_script_actions: List[ScriptBootstrapAction] = None,
        enable_prometheus_monitoring: bool = False,
        cluster_auto_terminates: bool = False,
        cluster_auto_termination_idle_timeout_seconds: int = 20 * 60,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        disable_vmem_pmem_checks: bool = True,
        task_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        managed_cluster_scaling_config: Optional[EmrClusterScalingProperties] = None,
        spark_use_gpu: bool = False,
        path_to_spark_gpu_discovery_script: Optional[str] = None,
        builtin_bootstrap_script_configuration: Optional[DockerSetupBootstrapScripts] = None,
        retries: int = 1,
    ):
        """
        El-Dorado Fleet Instance Cluster configured with docker

        :param name: Name of cluster
        :param image_name: Name of the docker image to load onto this cluster
        :param image_tag: tag of the docker image to load onto this cluster
        :param docker_registry: docker registry to load the image from
        :param master_fleet_instance_type_configs: Fleet Instance Type configs for master nodes
        :param core_fleet_instance_type_configs: Fleet Instance Type configs for Core Nodes - if none, just use master nodes
        :param ec2_subnet_ids: EC2 subnet ids to use for emr fleet instance cluster
        :param log_uri: Where to store cluster logs
        :param cluster_tags: Tags for clusters
        :param emr_release_label: EMR version to use
        :param ec2_key_name: EC2 key name to use
        :param additional_application_configurations: Additional configs for cluster
        :param emr_managed_master_security_group: security group
        :param emr_managed_slave_security_group: security group
        :param service_access_security_group: security group
        :param cluster_auto_terminates: Kill cluster after all steps are finished
        :param environment: execution environment
        :param spark_use_gpu: Configure spark to use GPUs with docker containers
        :param path_to_spark_gpu_discovery_script: Local path to run the GPU discovery script. For docker containers,
               this script must live inside the container.
        """

        if builtin_bootstrap_script_configuration is None:
            builtin_bootstrap_script_configuration = DockerSetupBootstrapScripts()

        if not bootstrap_script_actions:
            bootstrap_script_actions = []

        spark_defaults = {
            "spark.driver.defaultJavaOptions":
            "-XX:OnOutOfMemoryError='kill -9 %p' -XX:MaxHeapFreeRatio=70",
            "spark.executor.defaultJavaOptions":
            "-verbose:gc -Xlog:gc*::time -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' -XX:MaxHeapFreeRatio=70 -XX:+IgnoreUnrecognizedVMOptions",
        }

        if path_to_spark_gpu_discovery_script is not None:
            spark_defaults.update({
                "maximizeResourceAllocation": "true",
                "spark.driver.resource.gpu.discoveryScript": path_to_spark_gpu_discovery_script,
                "spark.executor.resource.gpu.discoveryScript": path_to_spark_gpu_discovery_script,
                "spark.task.resource.gpu.discoveryScript": path_to_spark_gpu_discovery_script,
            })

        container_executor_configurations = [{
            "Classification": "docker",
            "Properties": {
                "docker.allowed.devices": "all",
                "docker.allowed.ro-mounts": "/home/hadoop,/etc/passwd,/usr/share,/usr/lib",
                "docker.allowed.runtimes": "nvidia",
                "docker.allowed.volume-drivers": "nvidia-docker",
                "docker.trusted.registries": f"local,centos,{docker_registry}",
                "docker.privileged-containers.registries": f"local,centos,{docker_registry}",
            },
        }]

        if spark_use_gpu:
            container_executor_configurations.extend([
                {
                    "Classification": "gpu",
                    "Properties": {
                        "module.enabled": "true"
                    }
                },
                {
                    "Classification": "cgroups",
                    "Properties": {
                        "root": "/sys/fs/cgroup",
                        "yarn-hierarchy": "yarn",
                    },
                },
            ])

        # Apply base configurations
        base_additional_application_configurations = [
            {
                "Classification": "container-executor",
                "Configurations": container_executor_configurations,
            },
            {
                "Classification": "spark-defaults",
                "Properties": spark_defaults,
                "Configurations": [],
            },
        ]

        if spark_use_gpu:
            base_additional_application_configurations.append({
                "Classification": "yarn-site",
                "Properties": {
                    "yarn.nodemanager.resource-plugins": "yarn.io/gpu",
                    "yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices": "auto",
                    "yarn.nodemanager.resource-plugins.gpu.docker-plugin": "nvidia-docker-v2",
                    "yarn.resource-types": "yarn.io/gpu",
                },
            })
            base_additional_application_configurations.append({
                "Classification": "capacity-scheduler",
                "Properties": {
                    "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
                },
            })

        if additional_application_configurations:
            base_additional_application_configurations.extend(additional_application_configurations)

        # Apply bootstrap script for docker
        bootstrap_script_actions.append(
            ScriptBootstrapAction(
                path=builtin_bootstrap_script_configuration.install_docker_gpu_location,
                args=[docker_registry, image_name, image_tag],
            )
        )

        if cluster_auto_termination_idle_timeout_seconds > 0:
            bootstrap_script_actions.append(ScriptBootstrapAction(builtin_bootstrap_script_configuration.docker_idle_protection))

        # In order to use pyspark on EMR, we actually need to make the entrypoint available on the machine, not just
        # in the docker container. The COPY_ENTRYPOINT_FROM_IMAGE bootstrap script will copy the entrypoint(s) from
        # the container to the local machine, machine it available for a pyspark-submit command.
        if entrypoint_in_image:
            bootstrap_script_actions.append(
                ScriptBootstrapAction(
                    path=builtin_bootstrap_script_configuration.copy_entrypoint_from_image_location,
                    args=[
                        f"{docker_registry}/{image_name}",
                        image_tag,
                        entrypoint_in_image,
                    ],
                )
            )

        if spark_use_gpu:
            bootstrap_script_actions.append(ScriptBootstrapAction(path=builtin_bootstrap_script_configuration.cgroup_location, ))

        super().__init__(
            name=name,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            ec2_subnet_ids=ec2_subnet_ids,
            log_uri=log_uri,
            cluster_tags=cluster_tags,
            emr_release_label=emr_release_label,
            ec2_key_name=ec2_key_name,
            instance_configuration_spark_log4j=instance_configuration_spark_log4j,
            additional_application_configurations=base_additional_application_configurations,
            emr_managed_master_security_group=emr_managed_master_security_group,
            emr_managed_slave_security_group=emr_managed_slave_security_group,
            service_access_security_group=service_access_security_group,
            use_on_demand_on_timeout=use_on_demand_on_timeout,
            bootstrap_script_actions=bootstrap_script_actions,
            enable_prometheus_monitoring=enable_prometheus_monitoring,
            cluster_auto_terminates=cluster_auto_terminates,
            cluster_auto_termination_idle_timeout_seconds=cluster_auto_termination_idle_timeout_seconds,
            environment=environment,
            disable_vmem_pmem_checks=disable_vmem_pmem_checks,
            task_fleet_instance_type_configs=task_fleet_instance_type_configs,
            managed_cluster_scaling_config=managed_cluster_scaling_config,
            retries=retries,
        )


class ExtendedEmrTaskVisitor(EmrTaskVisitor, abc.ABC):

    def visit_docker_run_task(self, node: DockerRunEmrTask):
        self.visit_base_task(node)

    def visit_docker_emr_cluster_task(self, node: DockerEmrClusterTask):
        self.visit_base_task(node)

    def visit_pyspark_emr_cluster_task(self, node: PySparkEmrTask):
        self.visit_base_task(node)
