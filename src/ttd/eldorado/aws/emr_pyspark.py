from datetime import timedelta
from typing import List, Optional, Sequence, Tuple

from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.openlineage import OpenlineageConfig


# Run a pyspark task from a location on cloud storage
class S3PysparkEmrTask(EmrJobTask):

    def __init__(
        self,
        name: str,
        entry_point_path: str,
        eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
        command_line_arguments: Optional[List[str]] = None,
        timeout_timedelta: timedelta = timedelta(hours=4),
        action_on_failure: str = "TERMINATE_CLUSTER",
        configure_cluster_automatically: bool = False,
        cluster_specs: Optional[EmrClusterSpecs] = None,
        cluster_calc_defaults: ClusterCalcDefaults = ClusterCalcDefaults(),
        packages: Optional[List[str]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        do_xcom_push: bool = False,
        xcom_json_path: Optional[str] = None,
        maximize_resource_allocation=False,
    ):
        """
        Add a pyspark step using docker for dependency management

        :param name: Name of the task
        :param entry_point_path: Path to the entrypoint - this is a location that exists in s3.
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

        base_configs = []

        if packages is None:
            packages = []

        if len(packages) > 0:
            base_configs.append(("packages", ",".join(packages)))

        if additional_args_option_pairs_list is None:
            additional_args_option_pairs_list = []
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
            maximize_resource_allocation=maximize_resource_allocation
        )
