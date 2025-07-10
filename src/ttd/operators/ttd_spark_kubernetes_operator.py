from datetime import timedelta, datetime, timezone
from functools import cached_property
from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_unique_suffix
from kubernetes.client import CoreV1EventList

from ttd.kubernetes.spark_kubernetes_versions import SparkVersion
from ttd.kubernetes.spark_dynamic_allocation import SparkDynamicAllocation
from ttd.kubernetes.spark_pod_config import SparkExecutorPodConfig, SparkPodConfig
from ttd.mixins.namespaced_mixin import NamespacedMixin
from ttd.openlineage import OpenlineageConfig
from ttd.ttdenv import TtdEnvFactory, TtdEnv


class TtdSparkKubernetesOperator(NamespacedMixin, BaseSensorOperator):
    EL_DORADO_PATH = "s3a://ttd-build-artefacts/eldorado/snapshots/master/latest/el-dorado-assembly.jar"

    API_GROUP = "sparkoperator.k8s.io"
    API_VERSION = "v1beta2"
    API_PLURAL = "sparkapplications"

    SUCCESS_STATES = ["COMPLETED"]
    FAILURE_STATES = ["FAILED", "UNKNOWN", "SUBMISSION_FAILED"]

    def __init__(
        self,
        job_name: str,
        spark_version: SparkVersion,
        class_name: str,
        driver_config: SparkPodConfig,
        executor_config: SparkExecutorPodConfig,
        environment: TtdEnv = TtdEnvFactory.get_from_system(),
        executable_path: str = EL_DORADO_PATH,
        k8s_conn_id: str = "kubernetes_default",
        packages: Optional[list[str]] = None,
        log_events: bool = True,
        java_options: Optional[list[tuple[str, Any]]] = None,
        spark_configuration: Optional[list[tuple[str, Any]]] = None,
        hadoop_configuration: Optional[list[tuple[str, Any]]] = None,
        openlineage_config: OpenlineageConfig = OpenlineageConfig(),
        dynamic_allocation: SparkDynamicAllocation = SparkDynamicAllocation(),
        **kwargs
    ):
        """
        Runs a Spark job inside of Kubernetes. It submits the job to K8s, then waits for it to succeed. The job will
        then have a TTL of 7 days.

        :param job_name: the name of the job. This will not stay the same in Kubernetes - a unique suffix is added to the
        end of each job to differentiate them.
        :param spark_version: the version of Spark to use. Refer to the SparkVersions class for supported versions.
        :param class_name: the name of the Spark class to use.
        :param driver_config: driver-specific configuration.
        :param executor_config: executor-specific configuration.
        :param executable_path: a link/path to the jar file/executable file.
        :param k8s_conn_id: the connection ID used to connect to Kubernetes.
        :param packages: the packages to install for your job.
        :param java_options: the Java options to specify when the job is run, specified in key-pair tuples.
        :param spark_configuration: the Spark configuration options to apply in Spark itself. These are specified in
        key-pair values.
        :param hadoop_configuration: the Hadoop configuration options to apply. These are also specified in key-pair
        values.
        :param timeout_delta: how long to wait until the job is considered as failed.
        """
        super().__init__(task_id=f"{job_name}-submit", job_name=job_name, developer_mode=True, **kwargs)

        self.job_name = job_name
        self.pod_name = add_unique_suffix(name=self.job_name)
        self.spark_version = spark_version
        self.mode = "cluster"
        self.class_name = class_name
        self.application_file = executable_path
        self.conn_id = k8s_conn_id
        self.spark_driver_config = driver_config
        self.spark_executor_config = executor_config
        self.dynamic_allocation = dynamic_allocation

        self.log_events = log_events
        self.environment = environment
        self.openlineage_config = openlineage_config
        self.packages = packages
        self.eldorado_configuration = java_options or []
        self.spark_configuration = spark_configuration or []
        self.hadoop_configuration = [(key.replace("spark.hadoop.", "") if key.startswith("spark.hadoop.") else key, value)
                                     for key, value in (hadoop_configuration or [])]

        self.eldorado_configuration += [("ttd.env", self.environment.execution_env), ("log4j2.formatMsgNoLookups", True)]

        self.developer_mode = True  # TODO - should later be removed.
        self.use_hdfs = False

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> Any:

        # First off, let's determine the namespace.
        # We can't do this earlier, since only the context contains the DAG owner, which we use to determine namespace.
        namespace = self.get_namespace(context)

        logical_date_str = context["logical_date"].format("YYYY-MM-DDTHH.mm.ss")

        # Verify first that there are no existing jobs running through this DAG, so that we don't run duplicate jobs.
        response = self.hook.custom_object_client.list_namespaced_custom_object(
            self.API_GROUP,
            self.API_VERSION,
            namespace,
            self.API_PLURAL,
            label_selector=f"airflow/dag-task-id={self.job_name},airflow/dag-logical-date={logical_date_str}"
        )

        if len(response["items"]) > 0:

            # If a job already exists, let's add the application name and check the status.
            # If an existing job is running, then continue to watch it. Otherwise, create a new one, regardless of success.
            restart = False
            for app in response["items"]:
                try:
                    pod_name = app["metadata"]["name"]

                    status = app["status"]["applicationState"]["state"]
                    if status in self.SUCCESS_STATES or self.FAILURE_STATES:
                        restart = True
                    else:
                        self.pod_name = pod_name
                    break
                except KeyError:
                    continue

            if not restart:
                self.log.info(f"Spark job for {logical_date_str} has been found: {self.pod_name}")
                return super().execute(context)

        # Since we now know the namespace, we can add the OpenLineage options.
        additional_args = self.eldorado_configuration + self._get_openlineage_options(namespace)
        application = self._build_spark_application(context, additional_args)

        self.hook.create_custom_object(self.API_GROUP, self.API_VERSION, self.API_PLURAL, application, namespace)
        self.log.info(f"Created Spark Application: {self.pod_name}")
        return super().execute(context)

    def poke(self, context: Context) -> bool | PokeReturnValue:

        namespace = self.get_namespace(context)

        # Before checking the status of the job, let's see what events have appeared with respect to the application.
        # Thus, if the status of the application is marked as failed, we can throw exceptions there rather than wait
        # until after events are fetched.
        if self.log_events:

            items: CoreV1EventList = self.hook.core_v1_client.list_namespaced_event(
                namespace, field_selector=f"involvedObject.name={self.pod_name}"
            )
            current_time = datetime.now(timezone.utc)

            for event in items.items:

                event_time: datetime = event.last_timestamp
                if current_time - event_time < timedelta(seconds=62):
                    if event.type == "Warning":
                        self.log.warning(f"({event.reason}) {event.message}")
                    else:
                        self.log.info(f"({event.reason}) {event.message}")

        status_response = self.hook.get_custom_object(self.API_GROUP, self.API_VERSION, self.API_PLURAL, self.pod_name, namespace)
        try:
            status = status_response["status"]["applicationState"]["state"]
        except KeyError:  # Status has not yet been added to the Spark Application
            return False

        if status in self.SUCCESS_STATES:
            self.log.info(f"Spark Application has succeeded with status: {status}")
            return True

        if status in self.FAILURE_STATES:
            message = f"Spark Application has failed with status: {status}"
            raise AirflowException(message)

        self.log.info(f"Spark Application is in state: {status}")
        return False

    def _build_spark_application(self, context: Context, additional_args: list[tuple[str, Any]]) -> dict:
        return {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "version": "v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": self.pod_name,
                "labels": self.get_labels(context),
            },
            "spec": {
                "type": "Scala",
                "mode": self.mode,
                "sparkVersion": self.spark_version.spark_version,
                "restartPolicy": {
                    "type": "Never"
                },
                "volumes": self._build_volumes(context),
                "timeToLiveSeconds": timedelta(days=7).total_seconds(),  # 7 days
                "sparkConf": self._build_spark_configuration(),
                "hadoopConf": self._build_hadoop_configuration(),
                "deps": self._build_packages(),
                "mainApplicationFile": self.application_file,
                "mainClass": self.class_name,
                "driver": self.spark_driver_config.to_dict("executor", additional_args, self.get_hdfs_id(context)),
                "executor": self.spark_executor_config.to_dict(additional_args, self.get_hdfs_id(context)),
                "dynamicAllocation": self.dynamic_allocation.to_dict()
            }
        }

    def _build_spark_configuration(self) -> dict[str, Any]:
        return {key: value for key, value in self.spark_configuration}

    def _build_hadoop_configuration(self) -> dict[str, Any]:
        return {key: value for key, value in self.hadoop_configuration}

    def _build_packages(self) -> dict[str, list[str]]:
        return {"packages": self.packages or []}

    def _build_volumes(self, context: Context):
        hdfs_id = self.get_hdfs_id(context)
        volumes = [
            volume if isinstance(volume, dict) else volume.to_dict() for volume in [
                self.spark_driver_config.create_volume(),
                self.spark_executor_config.pod_config.create_volume(),
                self.get_hdfs_volume(hdfs_id)
            ] if volume is not None
        ]

        return volumes

    def _get_openlineage_options(self, namespace: str):

        # TODO - this is specific to Scala, PySpark jobs will differ
        if self.openlineage_config.enabled:
            return self.openlineage_config.get_java_spark_options_dict(self.pod_name, self.class_name, namespace, self.environment, None)
        else:
            return self.openlineage_config.get_eldorado_opts_to_disable_bundled_openlineage()

    def get_labels(self, context: Context):
        labels = super().get_labels(context)
        labels["airflow/try-number"] = str(context["ti"].try_number)

        return labels

    def enable_hdfs(self):
        self.use_hdfs = True

    def get_hdfs_id(self, context: Context) -> Optional[str]:
        return self.xcom_pull(context) if self.use_hdfs else None

    def get_hdfs_volume(self, hdfs_id: Optional[str]):
        return {"name": "hdfs-config", "configMap": {"name": f"{hdfs_id}"}} if hdfs_id else None
