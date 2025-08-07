"""Helper functions to create clusters often used by Identity Team.

Many of our workflows involve EMR on AWS. These functions are meant to simplify and centralise our cluster configurations.

If you're adding a new worklfow/cluster please use the defaults provided and then tune if needed.

To add a cluster, use the following:

```python
cluster = IdentityClusters.get_cluster(
    "myAwesomeCluster",
    # this is the TtdDag you're adding this to
    my_dag,
    # rough size is fine
    num_cores=200,
    # this is also default so could be omitted, but often changed to STORAGE
    core_type=ComputeType.GENERAL)

cluster.add_sequential_task(
    IdentityClusters.task("com.thetradedesk.idnt.identity.MY_MAIN_CLASS")
)
```

This will create an EMR cluster with fleet configurations and append the task of executing your
scala class found in the identity repo.

Things to note/conveniences:
- The cores will be adjusted to suit your instance types and maximise cluster utilisation, i.e. a rough number is fine here.
- Need nvme storage? Use core_type=ComputeType.STORAGE
- Need to execute from another repo? Migrate the code to identity. Joking aside, use `IdentityClusters.task(..., executable_path="another.jar")` to overwrite the jar path.
- Need to add special configs? Hopefully you won't but you can overwrite all configs inside get_cluster.
- Need to use a different `runDate`, overwrite it inside `IdentityClusters.task`.

Want to add new instances types, refresh existing architectures to new ones? Add them to:
`__unset_master_types` and `_emr_instance_types` to their respective categories.
"""
from datetime import timedelta
from enum import Enum
from math import ceil, lcm
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import re

from dags.idnt.identity_helpers import PathHelpers
from dags.idnt.statics import Executables, RunTimes, Tags
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.compute_optimized.c5a import C5a
from ttd.ec2.emr_instance_types.compute_optimized.c7a import C7a
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ec2.emr_instance_types.storage_optimized.i3en import I3en
from ttd.ec2.emr_instance_types.storage_optimized.i4g import I4g
from ttd.ec2.emr_instance_types.storage_optimized.i4i import I4i
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.ttdenv import TtdEnvFactory


class ComputeType(Enum):
    """AWS instance type groupings.

    See: https://aws.amazon.com/ec2/instance-types/
    """
    GENERAL = "m"
    COMPUTE = "c"
    STORAGE = "i"
    NEW_STORAGE = "i4"
    MEMORY = "r"
    ARM_GENERAL = "mg"
    ARM_MEMORY = "rg"
    ARM_STORAGE_LARGE = "rg_ebs"


class FleetType(Enum):
    """AWS EMR instance fleet types.
    """
    MASTER = "master"
    CORE = "core"
    TASK = "task"


class IdentityClusters:
    _log_location = "s3://ttd-identity/datapipeline/logs"

    @staticmethod
    def _log_uri(application_name: str, name: str) -> str:
        return f"s3://ttd-identity/datapipeline/logs/{application_name}/{name}/"

    __ebs_master_size = 256

    # these are reasonably sized, not too big (since there's little benefit from it) and not too small
    # so we get enough network throughput. We also add local storage to instances that are not nvme backed
    __unset_master_types: Dict[ComputeType, Set[EmrInstanceType]] = {
        ComputeType.GENERAL: {M5.m5_4xlarge().with_ebs_size_gb(__ebs_master_size),
                              M5.m5_8xlarge().with_ebs_size_gb(__ebs_master_size)},
        ComputeType.STORAGE: {I3.i3_8xlarge(), I3en.i3en_6xlarge()},
        ComputeType.NEW_STORAGE: {I4i.i4i_4xlarge(), I4i.i4i_8xlarge()},
        ComputeType.MEMORY: {
            R5d.r5d_8xlarge().with_ebs_size_gb(__ebs_master_size),
            R6gd.r6gd_8xlarge().with_ebs_size_gb(__ebs_master_size),
        },
        ComputeType.COMPUTE: {
            C5a.c5a_8xlarge().with_ebs_size_gb(__ebs_master_size),
            C7a.c7a_4xlarge().with_ebs_size_gb(__ebs_master_size),
        },
        ComputeType.ARM_GENERAL: {
            M6g.m6g_8xlarge().with_ebs_size_gb(__ebs_master_size),
            M6g.m6g_12xlarge().with_ebs_size_gb(__ebs_master_size),
            M7g.m7g_8xlarge().with_ebs_size_gb(__ebs_master_size),
            M7g.m7g_12xlarge().with_ebs_size_gb(__ebs_master_size),
        }
    }

    # we select one master for each cluster, so we set the weighted capacity to 1
    _emr_master_types: Dict[ComputeType, Set[EmrInstanceType]] = {
        instance_grouping: set(map(lambda c: c.with_fleet_weighted_capacity(1), instances))
        for instance_grouping, instances in __unset_master_types.items()
    }

    # These are the instances we're using for clusters. If you update this, please check if `__unset_master_types` needs updating too.
    _emr_instance_types: Dict[ComputeType, Set[EmrInstanceType]] = {
        ComputeType.GENERAL: {M5.m5_4xlarge(), M5.m5_8xlarge(), M5.m5_12xlarge(),
                              M5.m5_16xlarge()},
        ComputeType.STORAGE: {I3.i3_8xlarge(),
                              I3.i3_16xlarge(),
                              I3en.i3en_6xlarge(),
                              I3en.i3en_12xlarge(),
                              I3en.i3en_24xlarge()},
        ComputeType.NEW_STORAGE: {I4i.i4i_8xlarge(),
                                  I4i.i4i_16xlarge(),
                                  I4i.i4i_32xlarge(),
                                  I4g.i4g_8xlarge(),
                                  I4g.i4g_16xlarge()},
        ComputeType.MEMORY: {
            R5d.r5d_16xlarge(),
            R6gd.r6gd_16xlarge(),
        },
        ComputeType.COMPUTE: {
            C5a.c5a_12xlarge(),
            C5a.c5a_16xlarge(),
            C5a.c5a_24xlarge(),
            C7a.c7a_8xlarge(),
            C7a.c7a_12xlarge(),
            C7a.c7a_16xlarge(),
        },
        ComputeType.ARM_GENERAL: {
            M6g.m6g_8xlarge(),
            M6g.m6g_12xlarge(),
            M6g.m6g_16xlarge(),
            M7g.m7g_8xlarge(),
            M7g.m7g_12xlarge(),
            M7g.m7g_16xlarge(),
        },
        ComputeType.ARM_MEMORY: {
            R6g.r6g_8xlarge(),
            R6g.r6g_12xlarge(),
            R6g.r6g_16xlarge(),
            R7g.r7g_8xlarge(),
            R7g.r7g_12xlarge(),
            R7g.r7g_16xlarge(),
        },
        ComputeType.ARM_STORAGE_LARGE: {
            R6g.r6g_8xlarge().with_ebs_size_gb(8_000).with_ebs_iops(12_000).with_ebs_throughput(800),
            R6g.r6g_12xlarge().with_ebs_size_gb(12_000).with_ebs_iops(12_000).with_ebs_throughput(800),
            R6g.r6g_16xlarge().with_ebs_size_gb(16_000).with_ebs_iops(12_000).with_ebs_throughput(800),
            R7g.r7g_8xlarge().with_ebs_size_gb(8_000).with_ebs_iops(12_000).with_ebs_throughput(800),
            R7g.r7g_12xlarge().with_ebs_size_gb(12_000).with_ebs_iops(12_000).with_ebs_throughput(800),
            R7g.r7g_16xlarge().with_ebs_size_gb(16_000).with_ebs_iops(12_000).with_ebs_throughput(800),
        }
    }

    @classmethod
    def get_master_fleet(cls, compute_type: ComputeType = ComputeType.GENERAL) -> EmrFleetInstanceTypes:
        """Create a master fleet with the specified compute type.

        This creates a master fleet that has exactly one instance. Usually, you can get away
        with the general (m) types.

        Args:
            compute_type (ComputeType): Type of master instance. Defaults to ComputeType.GENERAL.

        Returns:
            EmrFleetInstanceTypes: Fleet for EMR master.
        """
        instances = list(cls._emr_master_types[compute_type])
        return EmrFleetInstanceTypes(
            instance_types=instances,
            on_demand_weighted_capacity=1,
            node_group=FleetType.MASTER.value,
        )

    @classmethod
    def get_fleet(
        cls,
        compute_type: ComputeType,
        num_cores: int,
        cpu_bounds: Tuple[int, int] = (0, 2048),
        fleet_type: FleetType = FleetType.CORE,
    ) -> EmrFleetInstanceTypes:
        """Create a fleet (non-master) for specified ComputeType.

        This creates a fleet configuration (core or task) based on the specified instance group.

        Args:
            compute_type (ComputeType): Instance type to use.
            num_cores (int): Approximate number of cores to use. Note that we adjust that to guarantee each instance types can fit into this, even if AWS picks multiple types.
            cpu_bounds (Tuple[int, int], optional): Minimum and maximum CPU cores per instance. Instance types outside the boundaries will not be selected. Defaults to (0, 2048).
            fleet_type (FleetType, optional): Type of fleet to be created. Defaults to FleetType.CORE.

        Returns:
            EmrFleetInstanceTypes: Fleet of instances with specified ComputeType.
        """
        instances = list(filter(lambda c: cpu_bounds[0] <= c.cores <= cpu_bounds[1], cls._emr_instance_types[compute_type]))
        assert len(instances) > 0, f"No instance types were left after filtering for cpu bounds: {cpu_bounds} out of {compute_type}"
        final_size = cls._finalize_cluster_size(num_cores, instances)

        return EmrFleetInstanceTypes(instance_types=instances, on_demand_weighted_capacity=final_size, node_group=fleet_type.value)

    @staticmethod
    def _finalize_cluster_size(requested_size: int, instance_types: Iterable[EmrInstanceType]) -> int:
        """Returns a cluster size that is sutiable for the specified instance types.

        We want to make sure all (mixed) instance types fit into the fleet and we always run the same number of CPUs.
        To acheieve this, we take the requested size, and find the closest number to it that is divisible by the least
        common multiple of the CPU sizes of the instance types.

        Args:
            requested_size (int): Requested number of CPUs.
            instance_types (Iterable[EmrInstanceType]): Instance types requested.

        Returns:
            int: Adjusted CPU count to be suitable for instances.
        """
        common_multiple = lcm(*map(lambda c: c.cores, instance_types))
        return ceil(requested_size / common_multiple) * common_multiple

    @staticmethod
    def get_config(num_cores: int, use_delta: bool = True, parallelism_multiplier: float = 2.0) -> List[Dict[str, Any]]:
        """Produce spark defaults based on number of cores and desired relative parallelism.

        These are our most sensible defaults. If you find that these these don't work for you, please raise it with the team
        as we want optimisations to benefit as many pipelines as possible without fine-tuning each individual cluster.
        Ideally, we don't want folks to use/touch these manually unless there is a very pressing business need,
        but rather use `get_cluster` to get a ready to use cluster with sensibly tuned configs.

        Args:
            num_cores (int): Number of cores in the cluster.
            use_delta (bool, optional): Whether to enable delta table usage in Spark. Defaults to True.
            parallelism_multiplier (float, optional): Ratio of partitions/paralellism to number of cores. Defaults to 2.0.

        Returns:
            List[Dict[str, Any]]: Cluster configs.

        TODO(leonid.vasilev):
            With --deploy-mode=cluster EMR allocates the driver in one of the YARN-containers on CORE fleet which
            makes some CPUs unusable for a job's execution. And, consequently, assuming that all CORE fleet CPUs are available
            might multiple the runtime of some stages: let's say the total number of CPUs is 960, only 928 of those are usable;
            a stage with 2*960=1920 partitions would be executed as follows: first 928 partitions are processed, next 928 partitions
            and then the last 64 with most CPUs not processing anything. Assuming it takes the same time to process each partition,
            runtime multiplies by ~1.5.
            We may want to subtract CPUs allocated for the driver here or experiment with --deploy-mode=client.
        """
        parallelism = ceil(num_cores * parallelism_multiplier)

        spark_defaults_properties = {
            "spark.sql.shuffle.partitions": str(parallelism),
            "spark.default.parallelism": str(parallelism),
            # performance tuning settings
            "spark.dynamicAllocation.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.executor.extraJavaOptions": "-server -XX:+UseParallelGC",
            "spark.driver.maxResultSize": "0",
            "spark.sql.adaptive.enabled": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            # set attempts to 1
            "spark.yarn.maxAppAttempts": "1",
            # shuffle tuning
            "spark.sql.files.maxPartitionBytes": "2147483648",
            "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "2GB",
        }

        if use_delta:
            spark_defaults_properties.update({
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            })

        return [
            {
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "true"
                },
            },
            {
                "Classification": "spark-defaults",
                "Properties": spark_defaults_properties,
            },
        ]

    @classmethod
    def get_cluster(
        cls,
        name: str,
        dag: str | TtdDag,
        num_cores: int,
        core_type: ComputeType = ComputeType.GENERAL,
        master_type: ComputeType = ComputeType.GENERAL,
        cpu_bounds: Tuple[int, int] = (0, 2048),
        use_delta: bool = True,
        parallelism_multiplier: float = 2.0,
        additional_app_configs: Optional[List[Dict[str, Any]]] = None,
        ganglia_memory_limit_mb: Optional[int] = None,
        **kwargs
    ) -> EmrClusterTask:
        """Get an EMR cluster.

        This is the main entry to create identity clusters in an easy and convenient way.

        Produce a reasonably configured EMR cluster for a family of instance types and cpu sizes.

        Args:
            name (str): Name of the cluster, this must be unique.
            dag (str | TtdDag): Either name of the dag or the TtdDag itself. Logs will be saved according to this name.
            num_cores (int): Approximate number of cores. Will be fine-tuned to fit all instances.
            core_type (ComputeType, optional): Instance type family for core instances, for nvme - most common - use `ComputeType.STORAGE`. Defaults to ComputeType.GENERAL.
            master_type (ComputeType, optional): Instance type family for the master node, default is probably good enough for most use cases. Defaults to ComputeType.GENERAL.
            cpu_bounds (Tuple[int, int], optional): Minimum and maximum CPU sizes per instance. This will filter out any instance types are outside these boundaries. Defaults to (0, 2048).
            use_delta (bool, optional): Whether to enable delta tables. Defaults to True.
            parallelism_multiplier (float, optional): CPU to spark partitions ratio. Defaults to 2.0.
            additional_app_configs (Optional[List[Dict[str, Any]]], optional): Any additional spark configs you'd like to add. Try it with `None` first! Defaults to None.
            ganglia_memory_limit_mb (Optional[Int], optional): Memory limit for Ganglia web server, per PHP script. The default is 128 MB which might not be enough for some large clusters; the sign of that issue is Ganglia's UI returning error 500.

        Raises:
            ValueError: If the type of `dag` is not valid.

        Returns:
            EmrClusterTask: Cluster airflow task with configurations ready to go.
        """

        EMR_RELEASE_KWARG = "emr_release_label"
        emr_version = kwargs.get(EMR_RELEASE_KWARG, Executables.emr_version)

        # ganglia is no longer on emr 7
        is_emr6 = re.match("emr-6", emr_version) is not None

        master_fleet = cls.get_master_fleet(master_type)
        core_fleet = cls.get_fleet(core_type, num_cores, fleet_type=FleetType.CORE, cpu_bounds=cpu_bounds)
        app_config = cls.get_config(core_fleet.get_scaled_total_cores(), use_delta=use_delta, parallelism_multiplier=parallelism_multiplier)

        if additional_app_configs:
            app_config.extend(additional_app_configs)

        if isinstance(dag, str):
            dag_name = dag
        elif isinstance(dag, TtdDag):
            dag_name = dag.airflow_dag.dag_id
        else:
            raise ValueError(f"Dag must be either TtdDag or str, got {type(dag)}")

        if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
            name = f"{name}_{TtdEnvFactory.get_from_system().execution_env}"

        if ganglia_memory_limit_mb is None and num_cores > 4096 and is_emr6:
            # ganglia was removed in 6.15+ versions, so 7+ will fail with this bootstrap
            # Increased default Ganglia memory limit for our large clusters. This is 4x
            # Ganglia's default 128M.
            ganglia_memory_limit_mb = 512

        bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None
        if ganglia_memory_limit_mb is not None:
            bootstrap_script_actions = []
            bootstrap_script_actions.append(
                ScriptBootstrapAction(
                    name="increase ganglia memory limit",
                    path=Executables.set_ganglia_memory_limit_script,
                    args=[str(ganglia_memory_limit_mb) + 'M'],
                )
            )

        default_kwargs = {
            "name": name,
            "log_uri": cls._log_uri(dag_name, name),
            "master_fleet_instance_type_configs": master_fleet,
            "core_fleet_instance_type_configs": core_fleet,
            "cluster_tags": Tags.cluster,
            EMR_RELEASE_KWARG: emr_version,
            "additional_application_configurations": app_config,
            "bootstrap_script_actions": bootstrap_script_actions,
            "use_on_demand_on_timeout": True,
            "cluster_auto_terminates": False,
            "retries": 0
        }

        full_kwargs = default_kwargs | kwargs

        return EmrClusterTask(**full_kwargs)

    def set_step_concurrency(emr_cluster_task: EmrClusterTask, concurrency: int = 10) -> EmrClusterTask:
        """Modify a cluster to execute steps in parallel.

        For this to work, you must ensure all steps are added with `add_parallel_body_task`
        and the `EMRJobTask` must have `action_on_failure="CONTINUE"` as per AWS docs here:
        https://docs.aws.amazon.com/emr/latest/APIReference/API_StepConfig.html#EMR-Type-StepConfig-ActionOnFailure

        Args:
            emr_cluster_task (EmrClusterTask): Cluster task to be modified.
            concurrency (int, optional): Level of concurrency. Defaults to 10.

        Returns:
            EmrClusterTask: New, modified cluster task. Note that original is mutated.
        """
        job_flow = emr_cluster_task._setup_tasks.last_airflow_op().job_flow_overrides
        job_flow["StepConcurrencyLevel"] = concurrency

        # overwrite all setups steps as CONTINUE
        for step in job_flow["Steps"]:
            step["ActionOnFailure"] = "CONTINUE"

        return emr_cluster_task

    @staticmethod
    def default_config_pairs(
        configs: Optional[List[Tuple[str, str]]] = None,
        runDate_arg: str = "runDate",
        runDate: str = RunTimes.previous_full_day,
    ) -> List[Tuple[str, str]]:
        """Produce a default set of eldorado_config_options with runDate and prodTest runName.

        Args:
            configs (List[Tuple[str, str]], optional): Optional configs to extend. Defaults to [].
            runDate_arg (str, optional): Name of runDate argument, this sets the key for the
                date of the execution date argument. Set this to `date` for older jobs.
                Defaults to "runDate".
            runDate (str, optional): Execution date. This usually labels the final dataset
                with this partition. Defaults to RunTimes.previous_full_day.

        Returns:
            List[Tuple[str, str]]: Configs for EMR cluster with runName if non-production and
            default runDate of yesterday if no runDate was provided.

        TODO:
            Hide this method and use task instead in DAGs.
        """
        if configs:
            new_configs = configs.copy()
        else:
            new_configs = []

        if Tags.environment() != "prod":
            new_configs.append(("runName", PathHelpers.get_test_airflow_run_name()))
            new_configs.append(("openlineage.enable", "false"))

        new_configs.append(("cleanupAfterEachDataStep", "true"))

        # append runDate of yesterday if not already specified
        if not list(filter(lambda c: c[0] == runDate_arg, new_configs)):
            new_configs.append((runDate_arg, runDate))

        return new_configs

    @classmethod
    def task(
        cls,
        class_name: str,
        eldorado_configs: Optional[List[Tuple[str, str]]] = None,
        extra_args: Optional[List[Tuple[str, str]]] = None,
        timeout_hours: int = 4,
        runDate: str = RunTimes.previous_full_day,
        runDate_arg: str = "runDate",
        executable_path: str = Executables.identity_repo_executable,
        action_on_failure: str = "TERMINATE_CLUSTER",
        task_name_suffix: str = "",
    ) -> EmrJobTask:
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
            timeout_hours (int, optional): Timeout in hours. Defaults to 4.
            runDate (str, optional): Date of dataset being generated.
                Defaults to RunTimes.previous_full_day.
            runDate_arg (str, optional): Name of runDate argument. For older jobs, set this to `date`
                for new pipelines, this is `runDate`. Defaults to "runDate".
            executable_path (str, optional): Name of the executable jar run.
                Defaults to identity repo executable.
            task_name_suffix (str): a suffix to add to class_name to form final task name, a way to make task name unique

        Returns:
            EmrJobTask: Task to be added to a cluster.

        TODO(leonid.vasilev)):
            When extra_args is None, eldorado configures Spark automatically which might interfere with our defaults
            We likely need to set EmrJobTask.configure_cluster_automatically=False here.
        """
        full_configs = cls.default_config_pairs(runDate_arg=runDate_arg, runDate=runDate, configs=eldorado_configs)

        return EmrJobTask(
            name=f"{class_name}{task_name_suffix}",
            class_name=class_name,
            eldorado_config_option_pairs_list=full_configs,
            additional_args_option_pairs_list=extra_args,
            executable_path=executable_path,
            timeout_timedelta=timedelta(hours=timeout_hours),
            action_on_failure=action_on_failure
        )

    @staticmethod
    def dict_to_configs(configs: Dict[str, str]) -> List[Tuple[str, str]]:
        """Translates a dictionary of configs to the required Tuple format.

        Args:
            configs (Dict[str, str]): EMR configs as dictionary.

        Returns:
            List[Tuple[str, str]]: Same configs as a list of tuples.
        """
        return [(key, value) for key, value in configs.items()]
