import typing
from datetime import timedelta
from typing import Dict, Optional, List

from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.xcom import XCOM_RETURN_KEY

from ttd.semver import SemverVersion
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.ec2_subnet import DevComputeAccount, ProductionAccount, AwsAccount
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.storage_optimized.i3 import I3
from ttd.ec2.emr_instance_types.storage_optimized.i4i import I4i
from ttd.eldorado import tag_utils
from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConf
from ttd.eldorado.aws.emr_monitor_cluster_startup import EmrMonitorClusterStartup
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.cluster_configs.disable_vmem_pmem_checks_configuration import DisableVmemPmemChecksConfiguration
from ttd.eldorado.aws.emr_cluster_specs import get_emr_fleet_spec, EmrClusterSpecs
from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.aws.cluster_configs.monitoring_configuration import MonitoringConfigurationSpark3
from ttd.eldorado.aws.cluster_configs.rolling_history_server_conf import RollingHistoryServerConf
from ttd.eldorado.aws.cluster_configs.java_configuration import JavaHadoopConf, JavaSparkEnvConf, JavaSparkDefaultsConf
from ttd.eldorado.aws.cluster_configs.maximize_resource_allocation import MaximizeResourceAllocationConf
from ttd.eldorado.aws.cluster_configs.pyspark import PysparkConfiguration
from ttd.eldorado.aws.cluster_configs.log4j2_asyncevents_conf import Log4j1AsyncEventQueueConf, Log4j2AsyncEventQueueConf
from ttd.eldorado.aws.cluster_configs.openlineage_cluster_config import OpenlineageClusterConfiguration
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.eldorado.visitor import AbstractVisitor
from ttd.metrics.cluster_lifecycle import ClusterTaskDataBuilder
from ttd.mixins.tagged_cluster_mixin import TaggedClusterMixin
from ttd.monads.maybe import Just
from ttd.operators.ec2_subnets_operator import EC2SubnetsOperator
from ttd.operators.ttd_emr_create_job_flow_operator import TtdEmrCreateJobFlowOperator
from ttd.operators.ttd_emr_terminate_job_flow_operator import TtdEmrTerminateJobFlowOperator
from ttd.tasks.base import BaseTask
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from ttd.tasks.setup_teardown import SetupTeardownTask
from ttd.ttdenv import TtdEnv, TtdEnvFactory
from ttd.openlineage import OpenlineageConfig, OpenlineageTransport
from ttd.python_versions import PythonVersionSpecificationException


class EmrClusterTask(TaggedClusterMixin, SetupTeardownTask):

    EC2_KEY_NAME: str = "TheTradeDeskDeveloper_USEAST"

    def __init__(
        self,
        name: str,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes,
        core_fleet_instance_type_configs: EmrFleetInstanceTypes,
        cluster_tags: Optional[Dict[str, str]] = None,
        ec2_subnet_ids: Optional[List[str]] = None,
        log_uri: Optional[str] = None,
        emr_release_label: str = AwsEmrVersions.AWS_EMR_SPARK_3_2,
        ec2_key_name: Optional[str] = None,
        pass_ec2_key_name: bool = True,
        instance_configuration_spark_log4j: Optional[dict] = None,
        additional_application_configurations: Optional[List[dict]] = None,
        additional_application: Optional[List[str]] = None,
        emr_managed_master_security_group: Optional[str] = None,
        emr_managed_slave_security_group: Optional[str] = None,
        service_access_security_group: Optional[str] = None,
        use_on_demand_on_timeout: bool = False,
        bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None,
        enable_prometheus_monitoring: bool = True,
        enable_spark_history_server_stats: bool = True,
        max_spark_history_logs: Optional[int] = None,
        cluster_auto_terminates: bool = False,
        cluster_auto_termination_idle_timeout_seconds: int = 20 * 60,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        disable_vmem_pmem_checks: bool = True,
        task_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        managed_cluster_scaling_config: Optional[EmrClusterScalingProperties] = None,
        region_name: str = "us-east-1",
        retries: int = 1,
        retry_delay: Optional[timedelta] = timedelta(minutes=20),
        whls_to_install: Optional[List[str]] = None,
        python_version: Optional[str] = None,
        maximize_resource_allocation: bool = False,
        custom_java_version: Optional[int] = None,
        ebs_root_volume_size: Optional[int] = None,
        force_i3_instances: bool = False,
        disable_ttd_certs: bool = False,
    ):
        """
        El-Dorado Fleet Instance Cluster

        @param name: Name of cluster
        @param master_fleet_instance_type_configs: Fleet Instance Type configs for master nodes
        @param core_fleet_instance_type_configs: Fleet Instance Type configs for Core Nodes
        @param ec2_subnet_ids: EC2 subnet ids to use for emr fleet instance cluster
        @param log_uri: Where to store cluster logs
        @param cluster_tags: Tags for clusters
        @param emr_release_label: EMR version to use
        @param ec2_key_name: EC2 key name to use. If you wish to not pass any ec2 key, set pass_ec2_key_name=False
        @param pass_ec2_key_name: When set to false, do not pass anything to Ec2 key name
        @param additional_application_configurations: Additional configs for cluster
        @param emr_managed_master_security_group: security group
        @param emr_managed_slave_security_group: security group
        @param service_access_security_group: security group
        @param cluster_auto_terminates: Kill cluster after all steps are finished
        @param environment: execution environment
        @param retries: Number of times to retry cluster upon failure.
        @param retry_delay: Delay between cluster retries.
        @param custom_java_version: If set, spark configuration to use the custom java version will be added to your cluster. Tested with java 11 and 17. Please note that you will need to choose the compatible emr version as well.
        @param force_i3_instances: We are adding i4 instances to the fleet of any cluster requesting only i3. This flag allows this to be disabled.
        """
        if not AwsEmrVersions.is_spark_3(emr_release_label):
            raise ClusterVersionNotSupported(f"AWS EMR version={emr_release_label} isn't supported. Use higher version supporting Spark 3")

        self.name = name
        self.environment = environment
        self.emr_release_label = emr_release_label

        self.cluster_tags = cluster_tags or {}
        self.cluster_tags.update({
            "Environment": self.environment.execution_env,
            "Process": self.name,
            "Resource": "EMR",
            "Source": "Airflow",
            "ClusterVersion": self.emr_release_label,
        })
        self.whls_to_install = whls_to_install[:] if whls_to_install is not None else []
        self.cluster_tags = tag_utils.add_creator_tag(self.cluster_tags)

        aws_account: AwsAccount = DevComputeAccount if TtdEnvFactory.get_from_system() == TtdEnvFactory.dev else ProductionAccount
        aws_vpc = aws_account.default_vpc

        self.python_semver = None
        self.python_version = python_version
        if python_version is not None:
            split = python_version.split('.')
            if len(split) != 3:
                raise PythonVersionSpecificationException(f"Python version must be specified as X.Y.Z, found: {python_version}")
            self.python_semver = SemverVersion(int(split[0]), int(split[1]), int(split[2]))

        self.bootstrap_script_actions = []
        self.bootstrap_script_actions += bootstrap_script_actions if bootstrap_script_actions is not None else []
        additional_application_configurations = (additional_application_configurations or [])

        self.maximize_resource_allocation = maximize_resource_allocation
        for configuration in additional_application_configurations:
            if configuration.get("Classification") == "spark" and configuration.get("Properties") and configuration["Properties"].get(
                    "maximizeResourceAllocation") == "true":
                self.maximize_resource_allocation = True

        # Handle automated application configurations
        managed_configs = self.get_managed_configs(
            disable_vmem_pmem_checks=disable_vmem_pmem_checks,
            enable_prometheus_monitoring=enable_prometheus_monitoring,
            enable_spark_history_server_stats=enable_spark_history_server_stats,
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            max_spark_history_logs=max_spark_history_logs,
            custom_java_version=custom_java_version
        )
        for config in managed_configs:
            additional_application_configurations = config.merge(additional_application_configurations)
        # Set up managed bootstrap scripts
        self.bootstrap_script_actions += self.get_managed_bootstrap_scripts(
            region_name=region_name,
            disable_vmem_pmem_checks=disable_vmem_pmem_checks,
            enable_spark_history_server_stats=enable_spark_history_server_stats,
            disable_ttd_certs=disable_ttd_certs
        )
        # Setup tags
        if enable_prometheus_monitoring:
            self.cluster_tags["Monitoring"] = "aws_emr"

        core_fleet_instance_type_configs = self._mutate_i3_instance_fleet(
            core_fleet_instance_type_configs, force_i3_instances, self.emr_release_label
        )
        self.core_fleet_instance_type_configs = core_fleet_instance_type_configs
        self.task_fleet_instance_type_configs = task_fleet_instance_type_configs

        subnet_task = OpTask(
            op=EC2SubnetsOperator(
                name=name,
                instance_types=[
                    *master_fleet_instance_type_configs.instance_types,
                    *core_fleet_instance_type_configs.instance_types,
                ],
                region_name=region_name,
            )
        )
        self.ec2_subnet_ids = (subnet_task.first_airflow_op().ec2_subnets_template if ec2_subnet_ids is None else ec2_subnet_ids)
        self.region_name = region_name

        ec2_key_to_use = None
        if pass_ec2_key_name:
            ec2_key_to_use = ec2_key_name or aws_account.ec2_ssh_key_name

        job_flow_spec = get_emr_fleet_spec(
            name=name + " @ {{ ts }} :: Airflow",
            log_uri=log_uri or aws_account.logs_uri,
            core_fleet_config=core_fleet_instance_type_configs,
            master_fleet_config=master_fleet_instance_type_configs,
            cluster_tags=self.cluster_tags,
            emr_release_label=self.emr_release_label,
            ec2_subnet_ids=self.ec2_subnet_ids,
            ec2_key_name=ec2_key_to_use,
            emr_managed_master_security_group=emr_managed_master_security_group or aws_vpc.security_groups.emr_managed_master_sg.id,
            emr_managed_slave_security_group=emr_managed_slave_security_group or aws_vpc.security_groups.emr_managed_slave_sg.id,
            service_access_security_group=service_access_security_group
            or aws_vpc.security_groups.service_access_sg.map(lambda sg: sg.id).or_else(lambda: Just(None)).get(),
            instance_configuration_spark_log4j=instance_configuration_spark_log4j,
            additional_application_configurations=additional_application_configurations,
            use_on_demand_on_timeout=use_on_demand_on_timeout,
            bootstrap_script_actions=self.bootstrap_script_actions,
            cluster_auto_terminates=cluster_auto_terminates,
            cluster_auto_termination_idle_timeout_seconds=cluster_auto_termination_idle_timeout_seconds,
            task_fleet_config=task_fleet_instance_type_configs if task_fleet_instance_type_configs is not None else None,
            managed_cluster_scaling_config=managed_cluster_scaling_config,
            additional_application=additional_application,
            ebs_root_volume_size=ebs_root_volume_size,
        )

        if "Steps" not in job_flow_spec:
            job_flow_spec["Steps"] = []

        cluster_task_data = ClusterTaskDataBuilder(
            name, cluster_version=self.emr_release_label
        ).for_aws(
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        ).build()

        create_cluster_task = OpTask(
            op=TtdEmrCreateJobFlowOperator(
                region_name=region_name,
                environment=self.environment,
                task_id=self.create_cluster_task_id(name),
                job_flow_overrides=job_flow_spec,
                aws_conn_id="aws_default",
                cluster_task_data=cluster_task_data,
            )
        )

        select_subnet_and_create_cluster_tasks = ChainOfTasks(
            task_id=f"select_subnet_and_create_cluster_{name}",
            tasks=[subnet_task, create_cluster_task],
        ).as_taskgroup(f"setup_{name}")

        self.cluster_id = self.job_flow_id(name)

        terminate_cluster_task = OpTask(
            op=TtdEmrTerminateJobFlowOperator(
                task_id=f"kill_cluster_{name}",
                job_flow_id=self.cluster_id,
                aws_conn_id="aws_default",
                # Trigger the cluster's termination when all the steps are done, regardless of whether they failed or succeeded
                trigger_rule=TriggerRule.ALL_DONE,
                region_name=region_name,
            )
        )

        monitor_cluster_startup_task_id = f'monitor_startup_{name}'

        monitor_cluster_startup = OpTask(
            op=EmrMonitorClusterStartup(
                task_id=monitor_cluster_startup_task_id,
                cluster_task_name=name,
                job_flow_id=self.cluster_id,
                aws_conn_id="aws_default",
                region_name=region_name,
                master_fleet_instance_type_configs=master_fleet_instance_type_configs,
                core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            )
        )

        # To keep track of openlineage configs added to this cluster so that its setup correctly
        self.observed_openlineage_configs: List[OpenlineageConfig] = []

        super().__init__(
            create_cluster_op_tags=job_flow_spec["Tags"],
            max_tag_length=256,
            task_id=name,
            setup_task=select_subnet_and_create_cluster_tasks,
            teardown_task=terminate_cluster_task,
            task_group_id=name,
            retries=retries,
            retry_delay=retry_delay,
            retry_op_kwargs_override={"excluded_task_ids": [monitor_cluster_startup_task_id]}
        )

        self.add_leaf_task(monitor_cluster_startup)

    def _adopt_ttd_dag(self, ttd_dag: TtdDag):
        super(EmrClusterTask, self)._adopt_ttd_dag(ttd_dag)
        self.generate_and_set_tags(ttd_dag)

    @staticmethod
    def _mutate_i3_instance_fleet(
        requested_core_fleet: EmrFleetInstanceTypes,
        force_i3_instances: bool,
        emr_release_label: str,
    ) -> EmrFleetInstanceTypes:
        """
        Manipulates instance fleet to reduce our usage of i3/i3en instances:

        1. Removes i3en instances if other types are present.
        2. Removes i3 instances if other types are present.
        3. If only i3 types remain, attempts to also request i4i (dependent on EMR version).

        :param requested_core_fleet: The core fleet requested. We will create a new instance if we change it.
        :param force_i3_instances: Forces us to stop requesting i4i instances for the given cluster.
        :param emr_release_label: The EMR version of the cluster.
        """
        i3_instance_mapping: dict[str, EmrInstanceType] = {
            I3.i3_2xlarge().instance_name: I4i.i4i_2xlarge(),
            I3.i3_4xlarge().instance_name: I4i.i4i_4xlarge(),
            I3.i3_8xlarge().instance_name: I4i.i4i_8xlarge(),
            I3.i3_16xlarge().instance_name: I4i.i4i_16xlarge(),
        }

        request = requested_core_fleet.instance_types
        request_without_i3en = [it for it in request if 'i3en' not in it.instance_name]

        # 1. Return original if fleet is only i3en
        if not request_without_i3en:
            return requested_core_fleet

        # 2. Remove i3en instances if others present
        if any('i3en' in it.instance_name for it in request):
            requested_core_fleet = EmrFleetInstanceTypes.copy_with_different_instances(requested_core_fleet, request_without_i3en)

        request_without_i3 = [it for it in request_without_i3en if it.instance_name not in i3_instance_mapping.keys()]

        if request_without_i3:
            # 3. Removing i3 changed nothing, so we can just return the fleet (minus any i3en instances)
            if len(request_without_i3) == len(request_without_i3en):
                return requested_core_fleet

            # 4. We are requesting i3 and other instances. We can remove i3
            else:
                return EmrFleetInstanceTypes.copy_with_different_instances(requested_core_fleet, request_without_i3)

        # 5. We're only requesting i3 instances. Let's try and slot in i4 instances
        if force_i3_instances:
            return requested_core_fleet

        emr_version = AwsEmrVersions.parse_version(emr_release_label)
        if emr_version < SemverVersion(6, 8, 0):
            return requested_core_fleet

        new_instance_fleet = []

        for i3_instance in request_without_i3en:
            i4_instance = i3_instance_mapping[i3_instance.instance_name]

            new_instance_fleet.append(i3_instance.with_priority(3))
            new_instance_fleet.append(i4_instance.copy_config_options_from(i3_instance).with_priority(2))

        from ttd.eldorado.aws.allocation_strategies import AllocationStrategyConfiguration, OnDemandStrategy

        return EmrFleetInstanceTypes(
            instance_types=new_instance_fleet,
            on_demand_weighted_capacity=requested_core_fleet.on_demand_weighted_capacity,
            allocation_strategy=AllocationStrategyConfiguration(on_demand=OnDemandStrategy.Prioritized),
            spot_weighted_capacity=requested_core_fleet.spot_weighted_capacity,
            node_group=requested_core_fleet.node_group,
        )

    @staticmethod
    def create_cluster_task_id(name: str):
        return f"create_cluster_{name}"

    @staticmethod
    def job_flow_id(name: str):
        return f"{{{{ task_instance.xcom_pull('{EmrClusterTask.create_cluster_task_id(name)}', key='{XCOM_RETURN_KEY}') }}}}"

    @staticmethod
    def log_uri(name: str):
        return f"{{{{ task_instance.xcom_pull('{EmrClusterTask.create_cluster_task_id(name)}', key='emr_logs')['log_uri'] }}}}"

    @property
    def cluster_specs(self) -> EmrClusterSpecs:
        return EmrClusterSpecs(
            cluster_name=self.name,
            environment=self.environment,
            core_fleet_instance_type_configs=self.core_fleet_instance_type_configs,
            task_fleet_instance_type_configs=self.task_fleet_instance_type_configs,
        )

    def get_managed_bootstrap_scripts(
        self, region_name: str, disable_vmem_pmem_checks: bool, enable_spark_history_server_stats: bool, disable_ttd_certs: bool
    ) -> List[ScriptBootstrapAction]:
        managed_bootstrap_scripts = []
        # Certs should be enabled by default, but we have some issues downloading bootstrap scripts
        # from this region. Eventually jobs will be moved out of this region anyway.
        if region_name != "eu-central-1":
            if disable_vmem_pmem_checks:
                managed_bootstrap_scripts += [
                    ScriptBootstrapAction(path=MonitoringConfigurationSpark3().NODE_EXPORTER_BOOTSTRAP_PATH),
                    ScriptBootstrapAction(
                        path=MonitoringConfigurationSpark3().GRAPHITE_EXPORTER_BOOTSTRAP_PATH,
                        args=[MonitoringConfigurationSpark3().MONITORING_BASE_PATH],
                    ),
                ]
            if not disable_ttd_certs:
                managed_bootstrap_scripts += [ScriptBootstrapAction(path=MonitoringConfigurationSpark3().IMPORT_TTD_CERTS_PATH, args=[])]

            if enable_spark_history_server_stats:
                managed_bootstrap_scripts += [
                    ScriptBootstrapAction(
                        path=MonitoringConfigurationSpark3().SPARK_HISTORY_STAT_BOOTSTRAP_PATH,
                        args=[MonitoringConfigurationSpark3().SPARK_HISTORY_STAT_SHUTDOWN_PATH]
                    )
                ]

            if self.python_semver is not None:
                emr_major_version = self.emr_release_label.split('emr-')[1].split('.')[0]

                managed_bootstrap_scripts.append(
                    ScriptBootstrapAction(
                        path="s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.3.2/latest/bootstrap/upgrade-python-version.sh",
                        args=[emr_major_version, f"{self.python_semver.major}.{self.python_semver.minor}.{self.python_semver.patch}"]
                    )
                )

            if len(self.whls_to_install) > 0:
                python_version = "" if self.python_semver is None else f".{self.python_semver.minor}"

                managed_bootstrap_scripts.append(
                    ScriptBootstrapAction(
                        path="s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.3.2/latest/bootstrap/install-whl.sh",
                        args=[python_version] + self.whls_to_install
                    )
                )

        return managed_bootstrap_scripts

    def get_managed_configs(
        self, disable_vmem_pmem_checks: bool, enable_prometheus_monitoring: bool, enable_spark_history_server_stats: bool,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes, max_spark_history_logs: Optional[int], custom_java_version: Optional[int]
    ) -> List[EmrConf]:
        managed_configs = [Log4j1AsyncEventQueueConf(), Log4j2AsyncEventQueueConf()]
        # Check if the yarn-site configuration is already made before appending
        if disable_vmem_pmem_checks:
            managed_configs.append(DisableVmemPmemChecksConfiguration())

        if enable_prometheus_monitoring:
            managed_configs.append(MonitoringConfigurationSpark3())

        if enable_spark_history_server_stats:
            managed_configs.append(
                RollingHistoryServerConf(
                    master_fleet_type=master_fleet_instance_type_configs, core_util=.5, max_files_to_retain=max_spark_history_logs
                )
            )
        else:
            # If we aren't collecting history server stats, no need to use more master cores for history server.
            managed_configs.append(RollingHistoryServerConf(max_files_to_retain=max_spark_history_logs))

        if custom_java_version:
            managed_configs += [
                JavaHadoopConf(custom_java_version, self.emr_release_label),
                JavaSparkEnvConf(custom_java_version, self.emr_release_label),
                JavaSparkDefaultsConf(custom_java_version, self.emr_release_label)
            ]

        if self.python_semver is not None:
            managed_configs.append(PysparkConfiguration(f"{self.python_semver.major}.{self.python_semver.minor}"))

        if self.maximize_resource_allocation:
            managed_configs.append(MaximizeResourceAllocationConf())

        return [c for c in managed_configs if c.supported_on(self.emr_release_label)]

    def configure_openlineage(self, config: OpenlineageConfig):
        self.observed_openlineage_configs.append(config)
        is_enabled = any(c.enabled for c in self.observed_openlineage_configs) and config.supports_region(self.region_name)
        robust_used = any(c.transport == OpenlineageTransport.ROBUST for c in self.observed_openlineage_configs)
        bootstrap_scripts = [a for a in self.bootstrap_script_actions]
        if is_enabled and self.region_name != "eu-central-1":
            # Load the initscript from openlineage in. This initscript will also setup the correct termination action
            bootstrap_scripts.append(config.assets_config.get_bootstrap_script_config(self.name, self.environment, robust_used))

        serialized_scripts = list(map(vars, bootstrap_scripts))
        job_flow_overrides = self._setup_tasks.last_airflow_op().job_flow_overrides
        job_flow_overrides["BootstrapActions"] = serialized_scripts
        if is_enabled:
            openlineage_cluster_config = OpenlineageClusterConfiguration(config, self.cluster_specs.cluster_name, self.environment)
            # Setup default config
            job_flow_overrides["Configurations"] = openlineage_cluster_config.merge(job_flow_overrides["Configurations"])

    def add_parallel_body_task(self, body_task: BaseTask) -> SetupTeardownTask:
        super().add_parallel_body_task(body_task)
        from ttd.eldorado.aws.emr_job_task import EmrJobTask

        if isinstance(body_task, EmrJobTask):
            body_task.set_cluster_specs(self.cluster_specs)

            if self.maximize_resource_allocation:
                body_task.maximize_resource_allocation = True

            task_name = tag_utils.replace_tag_invalid_chars(body_task.name)
            cluster_tags = self._setup_tasks.last_airflow_op().job_flow_overrides["Tags"]
            tag_utils.add_task_tag(cluster_tags, task_name)
            self.configure_openlineage(body_task.openlineage_config)
        return self

    def add_sequential_body_task(self, body_task: BaseTask) -> SetupTeardownTask:
        super().add_sequential_body_task(body_task)
        from ttd.eldorado.aws.emr_job_task import EmrJobTask

        if isinstance(body_task, EmrJobTask):
            body_task.set_cluster_specs(self.cluster_specs)

            if self.maximize_resource_allocation:
                body_task.maximize_resource_allocation = True

            task_name = tag_utils.replace_tag_invalid_chars(body_task.name)
            cluster_tags = self._setup_tasks.last_airflow_op().job_flow_overrides["Tags"]
            tag_utils.add_task_tag(cluster_tags, task_name)
            self.configure_openlineage(body_task.openlineage_config)
        return self

    def accept(self, visitor: AbstractVisitor) -> None:
        if typing.TYPE_CHECKING:
            from ttd.eldorado.aws.emr_task_visitor import EmrTaskVisitor

        if isinstance(visitor, EmrTaskVisitor):
            visitor.visit_emr_cluster_task(self)
        else:
            super().accept(visitor)


class ClusterVersionNotSupported(Exception):
    pass
