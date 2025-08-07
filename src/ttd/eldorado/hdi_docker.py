from datetime import timedelta
from typing import Dict, Optional, List, Sequence, Tuple, Union

from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.eldorado.azure.hdi_cluster_specs import HdiClusterSpecs
from ttd.eldorado.hdiversion import HDIClusterVersions, HDIVersion
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.hdinsight.script_action_spec import HdiScriptActionSpecBase, ScriptActionSpec
from ttd.ttdenv import TtdEnvFactory, TtdEnv
from ttd.docker import PRODUCTION_DOCKER_REGISTRY, DockerCommandBuilder
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask, HTTPS_SCRIPTS_ROOT

ABFS_SCRIPTS_ROOT = "abfs://ttd-build-artefacts@ttdeldorado.blob.core.windows.net/eldorado-core/release/v1-spark-3.2.1/latest/azure-scripts"


class HDIPysparkClusterTask(HDIClusterTask):

    def __init__(
        self,
        name: str,
        vm_config: HDIVMConfig,
        image_name: str,
        image_tag: str,
        entrypoint_in_image: str,
        docker_registry: str = PRODUCTION_DOCKER_REGISTRY,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        region: str = "eastus",
        cluster_tags: Optional[Dict[str, str]] = None,
        cluster_version: HDIVersion = HDIClusterVersions.AzureHdiSpark31,
        extra_script_actions: Optional[List[HdiScriptActionSpecBase]] = None,
        permanent_cluster: bool = False,
        enable_openlineage: bool = False,
    ):
        super().__init__(
            name=name,
            vm_config=vm_config,
            environment=environment,
            region=region,
            cluster_tags=cluster_tags,
            cluster_version=cluster_version,
            extra_script_actions=extra_script_actions,
            permanent_cluster=permanent_cluster,
            enable_openlineage=enable_openlineage,
            enable_docker=True,
            post_docker_install_scripts=[
                ScriptActionSpec(
                    action_name="copy-entrypoint-from-image",
                    script_uri=f"{HTTPS_SCRIPTS_ROOT}/copy-entrypoint-from-image.sh",
                    parameters=[
                        f"{docker_registry}/{image_name}",
                        image_tag,
                        entrypoint_in_image,
                    ]
                )
            ]
        )


class HDIDockerRunJobTask(HDIJobTask):

    def __init__(
        self,
        name: str,
        docker_run_command: Union[str, DockerCommandBuilder],
        timeout_timedelta: timedelta = timedelta(hours=4),
        action_on_failure: str = "TERMINATE_CLUSTER",
        configure_cluster_automatically: bool = False,
        cluster_specs: Optional[HdiClusterSpecs] = None,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
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
        """

        command_line_arguments = ["bash", "-c"]

        if isinstance(docker_run_command, str):
            command_line_arguments.append(docker_run_command)
        else:
            self.docker_run_builder = docker_run_command
            command_line_arguments.append(docker_run_command.build_command())

        super().__init__(
            name,
            class_name=None,
            cluster_specs=cluster_specs,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            jar_path=f"{ABFS_SCRIPTS_ROOT}/command-runner.py",
            configure_cluster_automatically=configure_cluster_automatically,
            command_line_arguments=command_line_arguments,
            cluster_calc_defaults=cluster_calc_defaults,
            is_docker=True,
        )


class HDIPysparkJobTask(HDIJobTask):

    def __init__(
        self,
        name: str,
        entry_point_path_in_container: str,
        image_name: str,
        image_tag: Optional[str] = "latest",
        docker_registry: Optional[str] = PRODUCTION_DOCKER_REGISTRY,
        cluster_specs: Optional[HdiClusterSpecs] = None,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        configure_cluster_automatically: bool = False,
        command_line_arguments: Optional[List[str]] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
        eldorado_vault_certificate_mount_path: str = "/opt/application/certificate-eldorado-vault-sp.pfx",
    ):

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
            ("conf", "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=python3.9"),
            ("conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.9"),
            (
                "conf",
                f"spark.yarn.appMasterEnv.ttdenv={cluster_specs.environment if cluster_specs else TtdEnvFactory.get_from_system()}",
            ),
            ("num-executors", "2"),
            (
                "conf",
                f"spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/usr/hdp/current/spark3-client/jars/:/usr/hdp/current/spark3-client/jars/:ro,/etc/group:/etc/group:ro,/etc/passwd:/etc/passwd:ro,/usr/lib/hdinsight-common/:/usr/lib/hdinsight-common/:ro,/usr/lib/hdinsight-spark/:/usr/lib/hdinsight-spark/:ro,/usr/lib/hdinsight-logging/:/usr/lib/hdinsight-logging/:ro,/usr/lib/hdinsight-datalake/:/usr/lib/hdinsight-datalake/:ro,/home/sshuser/app/:/home/sshuser/app:ro,/certificate-eldorado-vault-sp.pfx:/{eldorado_vault_certificate_mount_path}:ro"
            ),
            (
                "conf",
                "spark.executor.extraClassPath=/usr/hdp/current/spark3-client/jars/*:/usr/lib/hdinsight-common/*:/usr/lib/hdinsight-common/*:/usr/lib/hdinsight-spark/*:/usr/lib/hdinsight-logging/*:/usr/lib/hdinsight-datalake/*"
            ),
            (
                "conf",
                "spark.driver.extraClassPath=/usr/hdp/current/spark3-client/jars/*:/usr/lib/hdinsight-common/*:/usr/lib/hdinsight-common/*:/usr/lib/hdinsight-spark/*:/usr/lib/hdinsight-logging/*:/usr/lib/hdinsight-datalake/*"
            ),
            (
                "conf",
                f"spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/usr/hdp/current/spark3-client/jars/:/usr/hdp/current/spark3-client/jars/:ro,/etc/group:/etc/group:ro,/etc/passwd:/etc/passwd:ro,/usr/lib/hdinsight-common/:/usr/lib/hdinsight-common/:ro,/usr/lib/hdinsight-spark/:/usr/lib/hdinsight-spark/:ro,/usr/lib/hdinsight-logging/:/usr/lib/hdinsight-logging/:ro,/usr/lib/hdinsight-datalake/:/usr/lib/hdinsight-datalake/:ro,/home/sshuser/app/:/home/sshuser/app:ro,/certificate-eldorado-vault-sp.pfx:/{eldorado_vault_certificate_mount_path}:ro"
            ),
        ]

        if additional_args_option_pairs_list is not None:
            base_configs.extend(additional_args_option_pairs_list)  # type: ignore

        if command_line_arguments is not None:
            command_line_arguments.insert(0, entry_point_path_in_container)
        else:
            command_line_arguments = [entry_point_path_in_container]

        super().__init__(
            name,
            class_name=None,
            cluster_specs=cluster_specs,
            eldorado_config_option_pairs_list=eldorado_config_option_pairs_list,
            additional_args_option_pairs_list=base_configs,
            jar_path=f"{ABFS_SCRIPTS_ROOT}/pyspark-entrypoint.py",
            configure_cluster_automatically=configure_cluster_automatically,
            command_line_arguments=command_line_arguments,
            cluster_calc_defaults=cluster_calc_defaults,
            is_pyspark=True
        )
