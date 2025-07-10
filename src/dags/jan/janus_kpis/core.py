import hashlib
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any, Union

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_cluster_specs import EmrClusterSpecs
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import AIFUN, JAN, SlackTeam
from ttd.tasks.base import BaseTask
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnv, TtdEnvFactory, ProdEnv

INTERMEDIATE_STAGE = "intermediate"
ROLLUP_STAGE = "rollup"
EXPORT_STAGE = "export"

CURRENT_DATE = '{{ logical_date.strftime("%Y-%m-%d") }}'
CURRENT_HOUR = '{{ logical_date.strftime("%Y-%m-%dT%H:00:00") }}'
CURRENT_HOUR_DS_CHECK = '{{ logical_date.strftime("%Y-%m-%d %H:00:00") }}'
ONE_HOUR_LOOKBACK = '{{ (logical_date - macros.timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00") }}'
ONE_HOUR_LOOKBACK_DS_CHECK = '{{ (logical_date - macros.timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00") }}'

JANUS_PROJECT_IDS = ["biddercache", "budget-server"]


def get_dag_id(stage, metric) -> str:
    # Used in both the sensor and dag creation code to keep consistency
    return f"janus-kpis-{stage}-{metric}"


def get_cluster_task_name(prefix_to_keep, suffix_to_hash) -> str:
    # Concat prefix with a hashed suffix of length 8
    name_hash = hashlib.md5(suffix_to_hash.encode()).hexdigest()
    suffix = name_hash[:8]
    return f"{prefix_to_keep}-{suffix}"


class _JanusCoreDag:
    STAGE: Optional[str] = None
    JANUS_PROJECT_IDS: Optional[List[str]] = None
    JAR_DEFAULT = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/ds/libs/janusabtestingkpipipeline-assembly.jar"

    def __init__(
        self,
        metric: str,
        task_class: str,
        config_options: List[Tuple[str, str]],
        start_date: datetime,
        job_task_name: Optional[str] = None,
        end_date: Optional[datetime] = None,
        core_fleet_capacity: int = 64,
        emr_release_label: str = "emr-6.7.0",
        enable_slack_alert: bool = False,
        max_active_runs: int = 1,
        schedule_interval: Union[str, timedelta] = timedelta(hours=1),
        run_only_latest: Optional[bool] = None,
        slack_team: SlackTeam = JAN.team,
        retries: int = 3,
        retry_delay: timedelta = timedelta(minutes=1),
        core_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        environment: Optional[TtdEnv] = None,
        jar_override: Optional[str] = None,
        spark_options: Optional[List[Tuple[str, str]]] = None,
        std_master_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        additional_application_configurations: Optional[Dict[str, Any]] = None,
        additional_cluster_tags: Optional[Dict[str, str]] = None,
        additional_dag_tags: Optional[List[str]] = None,
        janus_project_ids: Optional[List[str]] = None,
    ):

        self.dependency: Optional[BaseTask] = None

        if self.STAGE is None:
            raise Exception("cls.STAGE should be set by the inheriting class but isn't")

        self.dag_id = get_dag_id(self.STAGE, metric)
        self.task_class = task_class
        self.jar = jar_override
        self.config_options = config_options
        self.start_date = start_date
        self.end_date = end_date
        self.job_task_name = job_task_name
        self.additional_application_configurations = additional_application_configurations
        self.core_fleet_instance_type_configs = core_fleet_instance_type_configs
        self.dag_tags = ["janus-kpis"]
        self.teams_allowed_to_access = [AIFUN.team.jira_team, JAN.team.jira_team]
        self.emr_release_label = emr_release_label
        self.enable_slack_alert = enable_slack_alert
        self.environment = environment
        self.max_active_runs = max_active_runs
        self.retries = retries
        self.retry_delay = retry_delay
        self.schedule_interval = schedule_interval
        self.run_only_latest = run_only_latest
        self.slack_team = slack_team
        self.spark_options = spark_options
        self.std_master_fleet_instance_type_configs = std_master_fleet_instance_type_configs
        self.cluster_tags = {"Team": self.slack_team.jira_team, "Process": self.dag_id}

        if self.core_fleet_instance_type_configs is None:
            self.core_fleet_instance_type_configs = EmrFleetInstanceTypes(
                instance_types=[
                    R6gd.r6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
                    R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
                    R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
                    R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
                    R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
                ],
                on_demand_weighted_capacity=core_fleet_capacity
            )
        if self.jar is None:
            self.jar = self.JAR_DEFAULT
        if self.std_master_fleet_instance_type_configs is None:
            self.std_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
                instance_types=[M5.m5_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
                on_demand_weighted_capacity=1,
                spot_weighted_capacity=0
            )
        if self.spark_options is None:
            self.spark_options = [("conf", f"spark.sql.shuffle.partitions={core_fleet_capacity}")]

        if self.environment is None:
            env = TtdEnvFactory.get_from_system()
            if isinstance(env, ProdEnv):
                self.environment = env
            else:
                # we want to default to test here even in prodTest environment to prevent writing to prod
                # to test, users can manually pass in prodTest environment into the constructor
                self.environment = TtdEnvFactory.test

        if self.additional_application_configurations is None:
            self.additional_application_configurations = {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}
        if additional_cluster_tags is not None:
            self.cluster_tags.update(**additional_cluster_tags)
        if additional_dag_tags is not None:
            self.dag_tags += additional_dag_tags

        if self.job_task_name is None:
            self.job_task_name = self.STAGE

        self.janus_project_ids: List[str]
        if janus_project_ids is None:
            if self.JANUS_PROJECT_IDS is None:
                self.janus_project_ids = []
            else:
                self.janus_project_ids = self.JANUS_PROJECT_IDS
        else:
            self.janus_project_ids = janus_project_ids

    def get_config_options(self, janus_project_id) -> List[Tuple[str, str]]:
        return self.config_options

    def with_dependency(self, dependency: BaseTask) -> '_JanusCoreDag':
        self.dependency = dependency
        return self

    def get_ttd_dag(self) -> TtdDag:
        return TtdDag(
            dag_id=self.dag_id,
            start_date=self.start_date,
            end_date=self.end_date,
            schedule_interval=self.schedule_interval,
            run_only_latest=self.run_only_latest,
            max_active_runs=self.max_active_runs,
            retries=self.retries,
            retry_delay=self.retry_delay,
            slack_tags=self.slack_team.jira_team,
            enable_slack_alert=self.enable_slack_alert,
            tags=self.dag_tags,
            teams_allowed_to_access=self.teams_allowed_to_access
        )

    def get_cluster_task(self, project_id: str) -> EmrClusterTask:
        return EmrClusterTask(
            name=get_cluster_task_name(project_id, self.dag_id),
            master_fleet_instance_type_configs=self.std_master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=self.core_fleet_instance_type_configs,
            additional_application_configurations=[self.additional_application_configurations],
            cluster_tags=self.cluster_tags,
            emr_release_label=self.emr_release_label,
            enable_prometheus_monitoring=True,
            environment=self.environment,
            instance_configuration_spark_log4j={
                'log4j.rootCategory': 'INFO, console',
                'log4j.appender.console': 'org.apache.log4j.ConsoleAppender',
                'log4j.appender.console.layout': 'org.apache.log4j.PatternLayout',
                'log4j.appender.console.layout.ConversionPattern': '%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n',
                'log4j.appender.console.target': 'System.err',
            }
        )

    def get_emr_job_task(self, project_id: str, cluster_specs: EmrClusterSpecs) -> EmrJobTask:
        return EmrJobTask(
            name=self.job_task_name,
            class_name=self.task_class,
            cluster_specs=cluster_specs,
            executable_path=self.jar,
            eldorado_config_option_pairs_list=self.get_config_options(project_id),
            additional_args_option_pairs_list=self.spark_options,
        )

    def setup_cluster_with_step(self, project_id: str) -> BaseTask:
        cluster = self.get_cluster_task(project_id)
        step = self.get_emr_job_task(project_id, cluster.cluster_specs)
        cluster.add_parallel_body_task(step)
        return cluster

    @property
    def common_root(self) -> BaseTask:
        if self.dependency:
            return self.dependency
        return OpTask(op=DummyOperator(task_id="no_op_start"))

    def create_dag(self) -> DAG:
        ttd_dag = self.get_ttd_dag()
        root = self.common_root
        for project_id in self.janus_project_ids:
            cluster = self.setup_cluster_with_step(project_id)
            root >> cluster
            ttd_dag >> root
        return ttd_dag.airflow_dag


class JanusIntermediateKPIDag(_JanusCoreDag):
    STAGE = INTERMEDIATE_STAGE
    JANUS_PROJECT_IDS = ["all_projects"]  # Intermediate dags don't call to Janus to run per experiment, they run across all projects


class JanusProjectSpecificDag(_JanusCoreDag):

    def get_config_options(self, janus_project_id: str) -> List[Tuple[str, str]]:
        options = super().get_config_options(janus_project_id)
        return options + [("janusProjectId", janus_project_id)]


class JanusExportAssignmentSourceDag(JanusProjectSpecificDag):
    STAGE = INTERMEDIATE_STAGE
    JANUS_PROJECT_IDS = JANUS_PROJECT_IDS

    def __init__(
        self,
        export_name: str,
        start_date: datetime,
        job_task_name: str = "ExportAssignmentSource",  # So we don't reuse some massively long name
        end_date: Optional[datetime] = None,
        export_start_time: str = CURRENT_HOUR,
        core_fleet_capacity: int = 8,
        emr_release_label: str = "emr-6.7.0",
        enable_slack_alert: bool = False,
        max_active_runs: int = 2,
        schedule_interval: Union[str, timedelta] = timedelta(hours=1),
        run_only_latest: Optional[bool] = None,
        slack_team: SlackTeam = JAN.team,
        retries: int = 1,
        retry_delay: timedelta = timedelta(minutes=1),
        core_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        environment: Optional[TtdEnv] = None,
        jar_override: Optional[str] = None,
        spark_options: Optional[List[Tuple[str, str]]] = None,
        std_master_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        additional_application_configurations: Optional[Dict[str, Any]] = None,
        additional_cluster_tags: Optional[Dict[str, str]] = None,
        additional_dag_tags: Optional[List[str]] = None,
        extra_config_options: Optional[List[Tuple[str, str]]] = None,
    ):
        config_options = [
            ("BidRequestJanusAssignmentExportTransformer.FromJanus", "true"),
            ("BidRequestJanusAssignmentExportTransformer.exportStartTime", export_start_time),
        ]
        if extra_config_options is not None:
            config_options += extra_config_options

        super().__init__(
            metric=f"{export_name.lower()}",
            task_class=
            "com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.BidRequestJanusAssignmentExportTransformer",
            config_options=config_options,
            start_date=start_date,
            end_date=end_date,
            job_task_name=job_task_name,
            core_fleet_capacity=core_fleet_capacity,
            emr_release_label=emr_release_label,
            enable_slack_alert=enable_slack_alert,
            max_active_runs=max_active_runs,
            schedule_interval=schedule_interval,
            run_only_latest=run_only_latest,
            slack_team=slack_team,
            retries=retries,
            retry_delay=retry_delay,
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            environment=environment,
            jar_override=jar_override,
            spark_options=spark_options,
            std_master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
            additional_application_configurations=additional_application_configurations,
            additional_cluster_tags=additional_cluster_tags,
            additional_dag_tags=additional_dag_tags,
        )


class JanusRollupKPIDag(JanusProjectSpecificDag):
    STAGE = ROLLUP_STAGE
    JANUS_PROJECT_IDS = JANUS_PROJECT_IDS


class JanusExportKPIDag(JanusProjectSpecificDag):
    STAGE = "export"
    JANUS_PROJECT_IDS = JANUS_PROJECT_IDS

    def __init__(
        self,
        rollup_name: str,
        metric_col: str,
        metric_key_suffix: str,
        sensor_metric: str,
        start_date: datetime,
        job_task_name: str = "ExportKPI",  # So we don't reuse some massively long name
        end_date: Optional[datetime] = None,
        rollup_time: str = CURRENT_HOUR,
        core_fleet_capacity: int = 8,
        emr_release_label: str = "emr-6.7.0",
        enable_slack_alert: bool = False,
        max_active_runs: int = 2,
        schedule_interval: Union[str, timedelta] = timedelta(hours=1),
        run_only_latest: Optional[bool] = None,
        slack_team: SlackTeam = JAN.team,
        retries: int = 3,
        retry_delay: timedelta = timedelta(minutes=1),
        core_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        environment: Optional[TtdEnv] = None,
        jar_override: Optional[str] = None,
        spark_options: Optional[List[Tuple[str, str]]] = None,
        std_master_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
        additional_application_configurations: Optional[Dict[str, Any]] = None,
        additional_cluster_tags: Optional[Dict[str, str]] = None,
        additional_dag_tags: Optional[List[str]] = None,
        extra_config_options: Optional[List[Tuple[str, str]]] = None,
        sensor_timeout: int = 60 * 60 * 10 +
        10,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
    ):
        config_options = [
            ("JanusExporter.rollupTime", rollup_time),
            ("JanusExporter.metricColumn", metric_col),
            ("JanusExporter.metricKeySuffix", metric_key_suffix),
            ("JanusExporter.rollupDataSet", rollup_name),
            ("openlineage.enable", "true"),
        ]
        if extra_config_options is not None:
            config_options += extra_config_options

        super().__init__(
            metric=f"{rollup_name.lower()}_{metric_col.lower()}",
            task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.JanusExporter",
            config_options=config_options,
            start_date=start_date,
            end_date=end_date,
            job_task_name=job_task_name,
            core_fleet_capacity=core_fleet_capacity,
            emr_release_label=emr_release_label,
            enable_slack_alert=enable_slack_alert,
            max_active_runs=max_active_runs,
            schedule_interval=schedule_interval,
            run_only_latest=run_only_latest,
            slack_team=slack_team,
            retries=retries,
            retry_delay=retry_delay,
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            environment=environment,
            jar_override=jar_override,
            spark_options=spark_options,
            std_master_fleet_instance_type_configs=std_master_fleet_instance_type_configs,
            additional_application_configurations=additional_application_configurations,
            additional_cluster_tags=additional_cluster_tags,
            additional_dag_tags=additional_dag_tags,
        )

        self.sensor_metric = sensor_metric
        self.sensor_timeout = sensor_timeout

    def setup_cluster_with_step(self, janus_project_id: str) -> BaseTask:
        cluster_task = super().setup_cluster_with_step(janus_project_id)
        rollup_sensor_task = OpTask(
            op=JanusRollupKPISensor(
                metric=self.sensor_metric,
                janus_project_id=janus_project_id,
                timeout=self.sensor_timeout,  # in seconds
            )
        )
        rollup_sensor_task >> cluster_task
        return rollup_sensor_task


class JanusRollupKPISensor(ExternalTaskSensor):

    def __init__(
        self,
        metric: str,
        janus_project_id: str,
        poke_interval: int = 60 * 10 + 10,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
        timeout: int = 60 * 60 * 10,
        mode: str = "reschedule",
        check_existence: bool = False,
        allowed_states: Optional[List[str]] = None,
        job_task_name: Optional[str] = None,
        **kwargs,
    ):
        if allowed_states is None:
            allowed_states = ["success"]

        external_dag_id = get_dag_id(ROLLUP_STAGE, metric)
        if job_task_name is None:
            job_task_name = ROLLUP_STAGE

        external_task_id = f"{get_cluster_task_name(janus_project_id, external_dag_id)}_watch_task_{job_task_name}"
        super().__init__(
            task_id=f"wait_for_{external_task_id}",
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            allowed_states=allowed_states,
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            check_existence=check_existence,
            **kwargs,
        )
