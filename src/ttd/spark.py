from typing import Dict, Optional, List, Tuple, Union
from datetime import timedelta
import abc
from enum import Enum

from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties
from ttd.eldorado.databricks.autoscaling_config import DatabricksAutoscalingConfig
from ttd.eldorado.databricks.databricks_runtime import candidate_runtime_versions, DatabricksRuntimeSpecification
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.semver import SemverVersion
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.base import TtdDag
from airflow.models import BaseOperator
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.eldorado.databricks.region import DatabricksRegion
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.mixins.tagged_cluster_mixin import TaggedClusterMixin
from ttd.spark_workflow.tasks.spark_task import SparkTask
from ttd.tasks.setup_teardown import SetupTeardownTask
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.tasks.base import BaseTask

from airflow.operators.python import BranchPythonOperator
from ttd.tasks.op import OpTask

import warnings
import functools

from ttd.ttdenv import TtdEnvFactory, ProdTestEnv


def experimental(func):
    """This is a decorator which can be used to mark functions
    as experimental. It will result in a warning being emitted
    when the function is used."""

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', Warning)  # turn off filter
        warnings.warn("Call to experimental function {}.".format(func.__name__), category=Warning, stacklevel=2)
        warnings.simplefilter('default', Warning)  # reset filter
        return func(*args, **kwargs)

    return new_func


class SparkJobBuilderException(Exception):
    pass


class SparkVersionSpec(Enum):
    SPARK_3_2_1 = SemverVersion(3, 2, 1)
    SPARK_3_3_0 = SemverVersion(3, 3, 0)
    SPARK_3_3_2 = SemverVersion(3, 3, 2)
    SPARK_3_4_1 = SemverVersion(3, 4, 1)
    SPARK_3_5_0 = SemverVersion(3, 5, 0)


class EmrVersionSpec:

    def __init__(self, spark_version: SemverVersion, python_version: Optional[SemverVersion] = None) -> None:
        self.emr_release_label = ""
        self.python_version = python_version

        MAJOR_VERSION_SIX_PYTHON_VERSION = SemverVersion(3, 7, 0)
        MAJOR_VERSION_SEVEN_PYTHON_VERSION = SemverVersion(3, 9, 0)

        if spark_version == SparkVersionSpec.SPARK_3_2_1.value:
            self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2_1

            if python_version is not None and python_version.is_compatible_with(MAJOR_VERSION_SIX_PYTHON_VERSION):
                self.python_version = None

        elif spark_version == SparkVersionSpec.SPARK_3_3_0.value:
            self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

            if python_version is not None and python_version.is_compatible_with(MAJOR_VERSION_SIX_PYTHON_VERSION):
                self.python_version = None

        elif spark_version == SparkVersionSpec.SPARK_3_3_2.value:
            self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3_2

            if python_version is not None and python_version.is_compatible_with(MAJOR_VERSION_SIX_PYTHON_VERSION):
                self.python_version = None

        elif spark_version == SparkVersionSpec.SPARK_3_4_1.value:
            self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_4

            if python_version is not None and python_version.is_compatible_with(MAJOR_VERSION_SIX_PYTHON_VERSION):
                self.python_version = None

        elif spark_version == SparkVersionSpec.SPARK_3_5_0.value:
            self.emr_release_label = "emr-7.3.0"

            if python_version is not None and python_version.is_compatible_with(MAJOR_VERSION_SEVEN_PYTHON_VERSION):
                self.python_version = None

        else:
            raise SparkJobBuilderException("Unable to find a candidate emr version for the given python and spark versions")


class SparkRuntimeEnvironment:
    pass


class Databricks(SparkRuntimeEnvironment):
    photon = False

    def __init__(self, photon=False):
        super().__init__()
        self.photon = photon
        self.suffix = "_with-photon" if photon else ""

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Databricks) and self.photon == other.photon


class AwsEmr(SparkRuntimeEnvironment):

    def __init__(self):
        super().__init__()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AwsEmr)


class Region:
    DEFAULT_REGION = "us-east-1"
    US_EAST_1 = "us-east-1"
    US_WEST_2 = "us-west-2"
    AP_NORTHEAST_1 = "ap-northeast-1"
    AP_SOUTHEAST_1 = "ap-southeast-1"
    EU_WEST_1 = "eu-west-1"
    EU_CENTRAL_1 = "eu-central-1"

    def __init__(self, region) -> None:
        self.region = region

    def as_databricks(self) -> DatabricksRegion:
        if self.region == Region.US_EAST_1:
            return DatabricksRegion.use()

        elif self.region == Region.US_WEST_2:
            return DatabricksRegion.or5()

        elif self.region == Region.AP_NORTHEAST_1:
            return DatabricksRegion.jp3()

        elif self.region == Region.AP_SOUTHEAST_1:
            return DatabricksRegion.sg4()

        elif self.region == Region.EU_WEST_1:
            return DatabricksRegion.ie2()

        elif self.region == Region.EU_CENTRAL_1:
            return DatabricksRegion.de4()

        raise Exception("Unable to map to databricks region")

    def as_emr(self) -> str:
        return self.region


class EbsConfiguration:
    ebs_volume_count: int
    ebs_volume_size_gb: int
    ebs_volume_iops: Optional[int] = None
    ebs_volume_throughput: Optional[int] = None
    ebs_volume_type: str = "GENERAL_PURPOSE_SSD"

    def __init__(
        self,
        ebs_volume_count: int = 0,
        ebs_volume_size_gb: int = 0,
        ebs_volume_throughput=None,
        ebs_volume_iops=None,
    ):
        self.ebs_volume_count = ebs_volume_count
        self.ebs_volume_size_gb = ebs_volume_size_gb
        self.ebs_volume_throughput = ebs_volume_throughput
        self.ebs_volume_iops = ebs_volume_iops

    def as_databricks(self) -> DatabricksEbsConfiguration:
        return DatabricksEbsConfiguration(
            ebs_volume_count=self.ebs_volume_count,
            ebs_volume_size_gb=self.ebs_volume_size_gb,
            ebs_volume_iops=self.ebs_volume_iops,
            ebs_volume_throughput=self.ebs_volume_throughput,
        )


def _instance_to_fleet_instance(instance: EmrInstanceType, instance_count: int, ebs_configuration: Optional[EbsConfiguration] = None):
    instance = instance.with_fleet_weighted_capacity(1)

    if ebs_configuration is not None:

        if ebs_configuration.ebs_volume_size_gb != 0:
            instance = instance.with_ebs_size_gb(ebs_configuration.ebs_volume_size_gb)

        if ebs_configuration.ebs_volume_iops is not None:
            instance = instance.with_ebs_iops(ebs_configuration.ebs_volume_iops)

        if ebs_configuration.ebs_volume_throughput is not None:
            instance = instance.with_ebs_throughput(ebs_configuration.ebs_volume_throughput)

    return EmrFleetInstanceTypes(instance_types=[instance], on_demand_weighted_capacity=instance_count)


def _find_all_nodes(tasks: List[SparkTask]) -> List[SparkTask]:
    all_tasks = {}
    worklist = tasks[:]

    while len(worklist) > 0:
        next = worklist.pop()
        all_tasks[next.task_name] = next

        for dependent in next.depends_on:
            if dependent.task_name not in all_tasks:
                worklist.append(dependent)

    return list(all_tasks.values())


class AutoscalingConfiguration:

    def __init__(self, min_workers: int, max_workers: int) -> None:
        self.min_workers = min_workers
        self.max_workers = max_workers


class SparkJobBuilder(TaggedClusterMixin, SetupTeardownTask):

    @experimental
    def __init__(
        self,
        job_name: str,
        instance_type: EmrInstanceType,
        instance_count: int,
        spark_version: SemverVersion,
        tasks: List[SparkTask],
        driver_instance_type: Optional[EmrInstanceType] = None,
        python_version: Optional[SemverVersion] = None,
        region: Region = Region(Region.DEFAULT_REGION),
        ebs_configuration: Optional[EbsConfiguration] = None,
        spark_configurations: Optional[List[Tuple[str, str]]] = None,
        eldorado_cluster_configurations: Optional[List[Tuple[str, str]]] = None,
        tags: Optional[Dict[str, str]] = None,
        retry_delay: timedelta = timedelta(minutes=1),
        retries=0,
        # Including extra packages
        whls_to_install: Optional[List[str]] = None,
        jars_to_install: Optional[List[str]] = None,
        log_uri: Optional[str] = None,
        autoscaling: Optional[AutoscalingConfiguration] = None,
        candidate_databricks_runtimes: Optional[List[DatabricksRuntimeSpecification]] = None,
        ebs_root_volume_size: Optional[int] = None,
        additional_application: Optional[List[str]] = None,
        bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None,
        deploy_mode: Optional[str] = None,
    ):
        self.spark_tasks = tasks

        self.job_name = job_name
        self.instance_type = instance_type
        self.instance_count = instance_count
        self.spark_version = spark_version
        self.region = region
        self.spark_configurations = spark_configurations if spark_configurations is not None else []
        self.retries = retries
        self.tags: Dict[str, str] = tags if tags is not None else {}
        self.whls_to_install = whls_to_install if whls_to_install is not None else []
        self.ebs_configuration = ebs_configuration
        self.eldorado_cluster_configurations = eldorado_cluster_configurations
        self.jars_to_install = jars_to_install if jars_to_install else []
        self.retry_delay = retry_delay
        self.log_uri = log_uri
        self.autoscaling = autoscaling
        self.driver_instance_type = driver_instance_type
        self.candidate_databricks_runtimes = candidate_databricks_runtimes if candidate_databricks_runtimes is not None else candidate_runtime_versions(
            spark_version, python_version if python_version else None
        )
        self.ebs_root_volume_size = ebs_root_volume_size
        self.additional_application = additional_application

        # Candidate databricks runtimes. In theory, we could select multiple, so
        # for now we're assuming that there is at least one compatible.

        if len(self.candidate_databricks_runtimes) == 0:
            raise SparkJobBuilderException("Unable to find a candidate databricks runtime to match the given spark/python versions")

        # If there are many, select the candidate version that is "latest"
        self.databricks_runtime_version = max([x.label for x in self.candidate_databricks_runtimes])

        # Figure out if this expands to the correct version
        self.emr_version_spec = EmrVersionSpec(spark_version, python_version if python_version else None)
        self.bootstrap_script_actions = bootstrap_script_actions
        self.deploy_mode = deploy_mode

    def to_backend(self, runtime: SparkRuntimeEnvironment) -> DatabricksWorkflow | EmrClusterTask:
        if isinstance(runtime, Databricks):
            return self.to_databricks(runtime)
        elif isinstance(runtime, AwsEmr):
            return self.to_emr(runtime)
        else:
            raise SparkJobBuilderException(f"Unsupported runtime found in spark job builder: {runtime}")

    def to_databricks(self, runtime: Databricks) -> DatabricksWorkflow:
        all_tasks = _find_all_nodes(self.spark_tasks)

        # Set up dependencies between tasks - this is now encoded on the spark
        # task item itself.
        task_map = {x.task_name: x.as_databricks(self.whls_to_install, self.jars_to_install) for x in all_tasks}

        for spark_task in self.spark_tasks:
            spark_task_in_question = task_map[spark_task.task_name]
            for dependent in spark_task.depends_on:
                dependent_task = task_map[dependent.task_name]
                dependent_task >> spark_task_in_question

        return DatabricksWorkflow(
            job_name=f'{self.job_name}{runtime.suffix}',
            cluster_name=f'{self.job_name}_cluster{runtime.suffix}',
            databricks_spark_version=self.databricks_runtime_version,
            worker_node_type=self.instance_type.instance_name,
            worker_node_count=self.instance_count,
            driver_node_type=self.driver_instance_type.instance_name if self.driver_instance_type is not None else None,
            spark_configs={x[0]: x[1]
                           for x in self.spark_configurations},
            tasks=[x for x in task_map.values()],
            cluster_tags=self.tags,
            ebs_config=self.ebs_configuration.as_databricks() if self.ebs_configuration is not None else DatabricksEbsConfiguration(),
            retries=self.retries,
            eldorado_cluster_options_list=self.eldorado_cluster_configurations,
            retry_delay=self.retry_delay,
            region=self.region.as_databricks(),
            autoscaling_config=DatabricksAutoscalingConfig(self.autoscaling.min_workers, self.autoscaling.max_workers)
            if self.autoscaling is not None else None,
            bootstrap_script_actions=self.bootstrap_script_actions
        )

    def to_emr(self, runtime: AwsEmr) -> EmrClusterTask:
        cluster = EmrClusterTask(
            name=f'{self.job_name}_cluster',
            master_fleet_instance_type_configs=_instance_to_fleet_instance(self.driver_instance_type, 1)
            if self.driver_instance_type is not None else _instance_to_fleet_instance(self.instance_type, 1),
            core_fleet_instance_type_configs=_instance_to_fleet_instance(self.instance_type, self.instance_count, self.ebs_configuration),
            emr_release_label=self.emr_version_spec.emr_release_label,
            cluster_tags=self.tags,
            whls_to_install=self.whls_to_install,
            retries=self.retries,
            python_version=
            f"{self.emr_version_spec.python_version.major}.{self.emr_version_spec.python_version.minor}.{self.emr_version_spec.python_version.patch}"
            if self.emr_version_spec.python_version is not None else None,
            maximize_resource_allocation=True,
            retry_delay=self.retry_delay,
            log_uri=self.log_uri,
            enable_prometheus_monitoring=True,
            custom_java_version=17,
            region_name=self.region.as_emr(),
            managed_cluster_scaling_config=EmrClusterScalingProperties(self.autoscaling.max_workers, self.autoscaling.min_workers)
            if self.autoscaling is not None else None,
            ebs_root_volume_size=self.ebs_root_volume_size,
            additional_application=self.additional_application,
            bootstrap_script_actions=self.bootstrap_script_actions,
        )

        all_tasks = _find_all_nodes(self.spark_tasks)

        task_map = {
            x.task_name:
            x.as_emr(
                spark_configurations=self.spark_configurations,
                cluster_level_eldorado_configurations=self.eldorado_cluster_configurations,
                jar_paths=self.jars_to_install
            )
            for x in all_tasks
        }

        if self.deploy_mode is not None:
            for task_name in task_map.keys():
                task_map[task_name].deploy_mode = self.deploy_mode

        for spark_task in self.spark_tasks:
            spark_task_in_question = task_map[spark_task.task_name]
            for dependent in spark_task.depends_on:
                dependent_task = task_map[dependent.task_name]
                dependent_task >> spark_task_in_question

            # Put it on the cluster
            cluster.add_parallel_body_task(spark_task_in_question)

        return cluster


class ParallelTasks(BaseTask):

    @experimental
    def __init__(self, task_id: str, tasks: List[BaseTask]):
        super().__init__(task_id)
        self._tasks = tasks

    @property
    def _child_tasks(self) -> List["BaseTask"]:
        return [x for x in self._tasks]

    def _adopt_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        for task in self._tasks:
            task._set_ttd_dag(ttd_dag)

    def first_airflow_op(self) -> Union[Optional["BaseOperator"], List["BaseOperator"]]:
        return [x.first_airflow_op() for x in self._tasks]

    def last_airflow_op(self) -> Union[Optional["BaseOperator"], List["BaseOperator"]]:
        return [x.last_airflow_op() for x in self._tasks]

    def __rshift__(self, other: "BaseTask") -> "BaseTask":
        super().__rshift__(other)

        for task in self._tasks:
            task >> other

        return other


class SparkRuntimeChooser(abc.ABC):
    """
    Chooses the runtime environment in which to run the given tasks,
    given a list of possible tasks and associated runtimes
    """

    @abc.abstractmethod
    def choose(self, context, tasks: List[SparkRuntimeEnvironment]) -> Union[SparkRuntimeEnvironment, List[SparkRuntimeEnvironment]]:
        pass

    @abc.abstractmethod
    def possible_backends(self) -> List[SparkRuntimeEnvironment]:
        pass


class RandomSparkBackend(SparkRuntimeChooser):
    """
    Chooses a single backend for a given logical date. Will be stable if retried.
    """
    photon = False

    def __init__(self, photon=False):
        self.photon = photon

    def choose(self, context, tasks: List[SparkRuntimeEnvironment]) -> Union[SparkRuntimeEnvironment, List[SparkRuntimeEnvironment]]:
        logical_date = int(context["logical_date"].int_timestamp)
        index = logical_date % len(tasks)
        return tasks[index]

    def possible_backends(self) -> List[SparkRuntimeEnvironment]:
        return [Databricks(), AwsEmr()]


class AllBackends(SparkRuntimeChooser):
    """
    Runs the spark job on all of the given backends simultaneously. By default, this is just basic Databricks and EMR.
    If provided, this will also include photon for databricks. Note, if photon is True, it will run on both plain databricks
    and databricks with photon enabled.

    If you'd just like to run photon enabled and plain EMR, you can use the `CustomBackends` class and pass in the runtimes
    you'd like to run against.
    """
    photon = False

    def __init__(self, parallel=False, photon=False):
        self.photon = photon
        self.parallel = parallel

    def choose(self, context, tasks: List[SparkRuntimeEnvironment]) -> Union[SparkRuntimeEnvironment, List[SparkRuntimeEnvironment]]:
        return tasks

    def possible_backends(self) -> List[SparkRuntimeEnvironment]:
        base_list = [Databricks(), AwsEmr()]
        if self.photon:
            base_list.append(Databricks(photon=True))

        return base_list


class CustomBackends(SparkRuntimeChooser):
    """
    Runs the spark job on all of the provided backends simultanously.
    """

    def __init__(self, runtimes: List[SparkRuntimeEnvironment]):
        super().__init__()
        self.runtimes = runtimes

    def choose(self, context, tasks: List[SparkRuntimeEnvironment]) -> Union[SparkRuntimeEnvironment, List[SparkRuntimeEnvironment]]:
        return tasks

    def possible_backends(self) -> List[SparkRuntimeEnvironment]:
        return self.runtimes


class SingleBackend(SparkRuntimeChooser):
    """
    Runs the spark job on only one of the given backends
    """

    def __init__(self, runtime: SparkRuntimeEnvironment) -> None:
        self.runtime = runtime

    def choose(self, context, tasks: List[SparkRuntimeEnvironment]) -> Union[SparkRuntimeEnvironment, List[SparkRuntimeEnvironment]]:
        return self.runtime

    def possible_backends(self) -> List[SparkRuntimeEnvironment]:
        return [self.runtime]


class SparkWorkflow(ParallelTasks):
    """
    Defines a spark cluster with a variety of tasks to be run agnostic of backend.
    Using this operator will allow you to define a spark job in one way, and have
    the resulting configurations be created for either EMR or Databricks. The operator
    defaults to creating the spark job on a random backend, which will be persistent
    across within a dag run - i.e. a job ran on databricks for a given dag run will
    always run on databricks if retried.
    """

    @experimental
    def __init__(
        self,
        job_name: str,
        tasks: List[SparkTask],
        instance_type: EmrInstanceType,
        instance_count: int,
        spark_version: SemverVersion,
        driver_instance_type: Optional[EmrInstanceType] = None,
        python_version: Optional[SemverVersion] = None,
        ebs_configuration: Optional[EbsConfiguration] = None,
        region: Region = Region(Region.DEFAULT_REGION),
        spark_configurations: Optional[List[Tuple[str, str]]] = None,
        eldorado_cluster_configurations: Optional[List[Tuple[str, str]]] = None,
        tags: Optional[Dict[str, str]] = None,
        retries=1,
        retry_delay: timedelta = timedelta(minutes=1),
        backend_chooser: Optional[SparkRuntimeChooser] = None,
        whls_to_install: Optional[List[str]] = None,
        jars_to_install: Optional[List[str]] = None,
        autoscaling: Optional[AutoscalingConfiguration] = None,
        candidate_databricks_runtimes: Optional[List[DatabricksRuntimeSpecification]] = None,
        ebs_root_volume_size: Optional[int] = None,
        additional_application: Optional[List[str]] = None,
        bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None,
        deploy_mode: Optional[str] = None,
    ):
        """
        Constructs a new spark workflow. The cluster configuration provided here is shared
        by all of the tasks. Any wheels or dependent libraries are also shared by all of
        the tasks.

        By default, in production, this will expand to randomly choose the backend that
        the job runs on, either EMR or Databricks. This can be overwritten, just
        provide the backend via the `backend_chooser` function.

        By default, in prod test (when you do a branch deployment in Airflow), this
        will run jobs on _both_ backends. If you do not wish to opt in to this behavior,
        please provide a specific backend. The goal to run both by default is to allow
        testing across a variety of platforms via one configuration, ideally speeding
        up the iteration workflow.

        Based on the spark version (and optionally, the python version) specified, the
        runtime will be automatically selected. An equivalent versioned runtime will be
        set up, either on databricks or EMR (or both). You can find enums for the various
        supported spark versions in this file - `SparkVersionSpec`, and enums for the
        various supported python versions (if needed) in `ttd.python_versions`, defined
        as `PythonVersions`.

        Any jars passed to be installed here will be installed at the cluster level
        on databricks, and otherwise passed to the spark submit on EMR. Any whls specified
        here will be installed at the cluster level for both EMR and Databricks.

        Spark configurations are set for the whole workflow, and cannot be changed
        between tasks. This is different from the typical way of doing things on EMR, where
        tasks can have separate spark configs. In addition, we typically pass args
        to jobs that use el dorado via the spark jvm extra driver options. Since we cannot
        change those between tasks, it is recommend to pass those arguments at the task level,
        and handle parsing those at the application level.

        An example usage of this can be found in `src/dags/aifun/databricks_test.py`, where
        a spark workflow is defined using some test assets.
        """
        builder = SparkJobBuilder(
            job_name=job_name,
            tasks=tasks,
            instance_type=instance_type,
            instance_count=instance_count,
            spark_version=spark_version,
            driver_instance_type=driver_instance_type,
            python_version=python_version,
            region=region,
            spark_configurations=spark_configurations,
            eldorado_cluster_configurations=eldorado_cluster_configurations,
            tags=tags,
            retries=retries,
            retry_delay=retry_delay,
            ebs_configuration=ebs_configuration,
            whls_to_install=whls_to_install,
            jars_to_install=jars_to_install,
            autoscaling=autoscaling,
            candidate_databricks_runtimes=candidate_databricks_runtimes,
            ebs_root_volume_size=ebs_root_volume_size,
            additional_application=additional_application,
            bootstrap_script_actions=bootstrap_script_actions,
            deploy_mode=deploy_mode,
        )

        workflows = []

        if backend_chooser is None:
            env = TtdEnvFactory.get_from_system()
            if env == ProdTestEnv():
                backend_chooser = AllBackends()
            else:
                backend_chooser = RandomSparkBackend()

        if isinstance(backend_chooser, SingleBackend):
            workflows.append((backend_chooser.runtime, builder.to_backend(backend_chooser.runtime)))
        else:
            workflows = [(x, builder.to_backend(x)) for x in backend_chooser.possible_backends()]

        if isinstance(backend_chooser, AllBackends):
            if not backend_chooser.parallel and len(workflows) > 1:
                for previous, current in zip(workflows, workflows[1:]):
                    # Make sure that the tasks do not execute in parallel, assuming
                    # that these can't for whatever reason.
                    previous[1] >> current[1]

        super().__init__(f"{builder.job_name}_chain", tasks=[x[1] for x in workflows])

        self.task_group_id = f"dynamic-spark-backend-selection-{job_name}"
        self.as_taskgroup(self.task_group_id)

        def decide_branch(**context):
            backends = backend_chooser.choose(context, [x[0] for x in workflows])
            # Coerce to a list so that we can select our choices more easily
            backends = backends if isinstance(backends, list) else [backends]
            potential_tasks = [x[1].first_airflow_op().task_id for x in workflows if x[0] in backends]  # type: ignore
            return potential_tasks

        self.conditional_operator = OpTask(
            op=BranchPythonOperator(
                task_id=f'{builder.job_name}-decide-spark-job-backend',
                provide_context=True,
                python_callable=decide_branch,
            )
        )

        for op in self._tasks:
            self.conditional_operator >> op

    @property
    def _child_tasks(self) -> List["BaseTask"]:
        tasks: List["BaseTask"] = [self.conditional_operator]
        for task in self._tasks:
            tasks.append(task)
        return tasks

    def _adopt_ttd_dag(self, ttd_dag: "TtdDag") -> None:
        self.conditional_operator._set_ttd_dag(ttd_dag)

        for task in self._tasks:
            task._set_ttd_dag(ttd_dag)

    def first_airflow_op(self) -> Union[Optional["BaseOperator"], List["BaseOperator"]]:
        return self.conditional_operator.first_airflow_op()
