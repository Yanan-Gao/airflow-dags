from typing import Any, Dict, List, Optional, Tuple
from datetime import timedelta

from ttd.alicloud.alicloud_instance_types import AliCloudInstanceType, AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.eldorado.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.eldorado.base import TtdDag

from dags.idnt.identity_clusters import ComputeType, IdentityClusters
from alibabacloud_emr20160408 import models as emr_models
from dags.idnt.statics import Executables, RunTimes
from ttd.slack.slack_groups import IDENTITY
from ttd.ttdenv import TtdEnvFactory


class AliCloudInstanceTypeConfig:
    instance_types: List[AliCloudInstanceType] = [
        AliCloudInstanceTypes.ECS_G6_3X(),
        AliCloudInstanceTypes.ECS_G6_4X(),
        AliCloudInstanceTypes.ECS_G6_6X(),
        AliCloudInstanceTypes.ECS_G6_8X(),
        AliCloudInstanceTypes.ECS_G6_13X(),
        AliCloudInstanceTypes.ECS_G6_26X(),
        AliCloudInstanceTypes.ECS_R6_X(),
        AliCloudInstanceTypes.ECS_R6_2X(),
        AliCloudInstanceTypes.ECS_R6_3X(),
        AliCloudInstanceTypes.ECS_R6_4X(),
        AliCloudInstanceTypes.ECS_R6_6X(),
        AliCloudInstanceTypes.ECS_R6_8X(),
        AliCloudInstanceTypes.ECS_R6_13X(),
        AliCloudInstanceTypes.ECS_R6_26X(),
    ]
    disk_sizes: List[int] = [40, 80, 160, 320, 640, 1280, 2560]

    def __init__(self, memory_per_core: int, disk_per_core: int):
        self.memory_per_core = memory_per_core
        self.disk_per_core = disk_per_core

    def get_instance_types(self, num_cores: int, cpu_bounds: Tuple[int, int], min_node_count: int) -> ElDoradoAliCloudInstanceTypes:
        instances = list(
            filter(lambda x: cpu_bounds[0] <= x.cores <= cpu_bounds[1] and x.memory >= x.cores * self.memory_per_core, self.instance_types)
        )
        assert len(
            instances
        ) > 0, f"No instance types were left after filtering for cpu bounds: {cpu_bounds}, memory per core: {self.memory_per_core}"

        instance = instances[0]
        node_count = max(min_node_count, (num_cores + instance.cores - 1) // instance.cores)

        instance_disk_size = instance.cores * self.disk_per_core
        disk_size = next((x for x in self.disk_sizes if x >= instance_disk_size), max(self.disk_sizes))
        disk_count = max(0, (instance_disk_size + disk_size - 1) // disk_size)

        return ElDoradoAliCloudInstanceTypes(instance) \
            .with_node_count(node_count) \
            .with_data_disk_count(disk_count) \
            .with_data_disk_size_gb(disk_size)

    @classmethod
    def from_compute_type(cls, compute_type: ComputeType):
        # TODO: need to update these instance types for different purposes
        default_memory_per_core: Dict[ComputeType, int] = {
            ComputeType.GENERAL: 4,
            ComputeType.ARM_GENERAL: 4,
            ComputeType.MEMORY: 8,
            ComputeType.STORAGE: 4,
            ComputeType.ARM_STORAGE_LARGE: 4,
        }

        default_disk_per_core: Dict[ComputeType, int] = {
            ComputeType.GENERAL: 20,
            ComputeType.ARM_GENERAL: 20,
            ComputeType.MEMORY: 10,
            ComputeType.STORAGE: 40,
            ComputeType.ARM_STORAGE_LARGE: 80,
        }

        return AliCloudInstanceTypeConfig(default_memory_per_core[compute_type], default_disk_per_core[compute_type])

    @classmethod
    def create(cls, compute_type):
        return compute_type if isinstance(compute_type, AliCloudInstanceTypeConfig) else cls.from_compute_type(compute_type)


class IdentityClustersAliyun:
    """
    This class is a replacement of IdentityClusters for creating EMR cluster on Aliyun.
    So all class methods should have same schema as IdentityClusters.
    """

    @classmethod
    def get_cluster(
        cls,
        name: str,
        dag: str | TtdDag,  # ignored
        num_cores: int,
        core_type: ComputeType | AliCloudInstanceTypeConfig = ComputeType.GENERAL,
        master_type: ComputeType | AliCloudInstanceTypeConfig = ComputeType.GENERAL,
        cpu_bounds: Tuple[int, int] = (0, 2048),
        use_delta: bool = True,
        parallelism_multiplier: float = 2.0,
        additional_app_configs: Optional[List[Dict[str, Any]]] = None,
        **kwargs
    ) -> AliCloudClusterTask:
        """Get an Aliyun EMR cluster.

        This is the main entry to create identity clusters in an easy and convenient way.

        Produce a reasonably configured EMR cluster for a family of instance types and cpu sizes.

        Args:
            name (str): Name of the cluster, this must be unique.
            dag (str | TtdDag): Either name of the dag or the TtdDag itself. Ignored
            num_cores (int): Approximate number of cores. Will be fine-tuned to fit all instances.
            core_type (ComputeType, optional): Instance type family for core instances, for nvme - most common - use `ComputeType.STORAGE`. Defaults to ComputeType.GENERAL.
            master_type (ComputeType, optional): Instance type family for the master node, default is probably good enough for most use cases. Defaults to ComputeType.GENERAL.
            cpu_bounds (Tuple[int, int], optional): Minimum and maximum CPU sizes per instance. This will filter out any instance types are outside these boundaries. Defaults to (0, 2048).
            use_delta (bool, optional): Whether to enable delta tables. Defaults to True.
            parallelism_multiplier (float, optional): CPU to spark partitions ratio. Defaults to 2.0.
            additional_app_configs (Optional[List[Dict[str, Any]]], optional): Any additional spark configs you'd like to add. Try it with `None` first! Defaults to None.

        Returns:
            AliCloudClusterTask: Cluster airflow task with configurations ready to go.
        """

        master_instance_type = cls.get_master_instance_type(master_type)
        core_instance_type = cls.get_core_instance_type(core_type, num_cores, cpu_bounds)
        emr_default_config = cls.get_emr_default_config(
            core_instance_type.instance_type.cores * core_instance_type.node_count,
            use_delta=use_delta,
            parallelism_multiplier=parallelism_multiplier,
            additional_app_configs=additional_app_configs
        )

        if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
            name = f"{name}_{TtdEnvFactory.get_from_system().execution_env}"

        default_kwargs = {
            "name": name,
            "master_instance_type": master_instance_type,
            "core_instance_type": core_instance_type,
            "emr_default_config": emr_default_config,
            "cluster_tags": {
                "Team": IDENTITY.team.jira_team
            },
            "retries": 10,
            "retry_delay": timedelta(minutes=10),
        }
        if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
            default_kwargs.update({"retries": 0})

        full_kwargs = default_kwargs | kwargs
        return AliCloudClusterTask(**full_kwargs)

    @classmethod
    def get_master_instance_type(cls, compute_type: ComputeType | AliCloudInstanceTypeConfig) -> ElDoradoAliCloudInstanceTypes:
        return AliCloudInstanceTypeConfig.create(compute_type).get_instance_types(16, (16, 16), 1)

    @classmethod
    def get_core_instance_type(
        cls, compute_type: ComputeType, num_cores: int, cpu_bounds: Tuple[int, int]
    ) -> ElDoradoAliCloudInstanceTypes:
        return AliCloudInstanceTypeConfig.create(compute_type).get_instance_types(num_cores, cpu_bounds, 2)

    @classmethod
    def get_emr_default_config(
        cls, num_cores: int, use_delta: bool, parallelism_multiplier: float, additional_app_configs: Optional[List[Dict[str, Any]]]
    ) -> List[emr_models.CreateClusterV2RequestConfig]:
        app_config = IdentityClusters.get_config(num_cores, use_delta, parallelism_multiplier)

        if additional_app_configs:
            app_config.extend(additional_app_configs)

        spark_defaults = next(filter(lambda x: x["Classification"] == "spark-defaults", app_config), None)

        if spark_defaults is None:
            return []
        return list(
            map(
                lambda x: emr_models.CreateClusterV2RequestConfig(
                    service_name="SPARK",
                    file_name="spark-defaults",
                    config_key=x[0],
                    config_value=x[1],
                ), spark_defaults["Properties"].items()
            )
        )

    @classmethod
    def task(
        cls,
        class_name: str,
        eldorado_configs: Optional[List[Tuple[str, str]]] = None,
        extra_args: Optional[List[Tuple[str, str]]] = None,
        timeout_hours: int = 4,  # ignored
        runDate: str = RunTimes.previous_full_day,
        runDate_arg: str = "runDate",
        executable_path: str = Executables.identity_repo_executable_aliyun,
        action_on_failure: str = "TERMINATE_CLUSTER",  # ignored
        task_name_suffix: str = "",
    ) -> AliCloudJobTask:
        """Produce an EMR task for a job.

        Note that the `runDate` is set to be yesterday's date compared to the runtime of the DAG.
        This is usually what you need. This used to be `"{{ macros.ds_add(next_ds, -1) }}"`
        in airflow1. You can however use whatever date you need here, just be careful.

        Args:
            class_name (str): Name of the scala object to be executed.
            eldorado_configs (List[Tuple[str, str]], optional): Additional eldorado configs to be
                passed in to the driver, e.g. `[("INPUT_LOCATION", "s3://bucket/prefix")]`.
                Defaults to [].
            extra_args (Optional[List[Tuple[str, str]]], optional): Configuration arguments to spark-submit command -
                `additional_args_option_pairs_list` of EMRJobTask, e.g. `[("conf", "spark.sql.ignoreCorruptFiles=true")]`.
                Defaults to None.
            timeout_hours (int, optional): Timeout in hours. Defaults to 4. Ignored.
            runDate (str, optional): Date of dataset being generated.
                Defaults to RunTimes.previous_full_day.
            runDate_arg (str, optional): Name of runDate argument. For older jobs, set this to `date`
                for new pipelines, this is `runDate`. Defaults to "runDate".
            executable_path (str, optional): Name of the executable jar run.
                Defaults to identity repo executable.
            task_name_suffix (str): a suffix to add to class_name to form final task name, a way to make task name unique

        Returns:
            AliCloudJobTask: Task to be added to a cluster.
        """
        full_configs = IdentityClusters.default_config_pairs(runDate_arg=runDate_arg, runDate=runDate, configs=eldorado_configs)

        extra_args_modified: List[Tuple[str, str]] = [
            ("spark.kryoserializer.buffer.max", "256m"),
            # Switch to native Hadoop job committer, to avoid InvalidArgument Part number error
            # https://www.alibabacloud.com/help/en/emr/emr-on-ecs/user-guide/fix-the-invalidargument-part-number-error-that-occurs-when-i-access-oss
            # https://www.alibabacloud.com/help/en/emr/switch-to-native-hadoop-job-committer
            ("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter"),
        ]
        if extra_args is not None:
            extra_args_modified.extend(dict(pair[1].split("=", 1) for pair in extra_args if pair[0] == "conf").items())
            extra_args_modified.extend(pair for pair in extra_args if pair[0] != "conf")

        return AliCloudJobTask(
            name=f"{class_name}{task_name_suffix}",
            class_name=class_name,
            eldorado_config_option_pairs_list=full_configs,
            additional_args_option_pairs_list=extra_args_modified,
            jar_path=executable_path,
            configure_cluster_automatically=True,
        )

    def set_step_concurrency(emr_cluster_task: AliCloudClusterTask, concurrency: int = 10) -> AliCloudClusterTask:
        """Modify a cluster to execute steps in parallel.

        Not supported
        """
        return emr_cluster_task
