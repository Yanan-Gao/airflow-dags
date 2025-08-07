import logging
from typing import Dict, Any, List, Optional

from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from ttd.hdinsight.ambari_hook import AmbariHook, ClusterComponents
from ttd.hdinsight.hdi_hook import HDIHook, AZ_VERBOSE_LOGS_VAR, WARNING_LEVEL
from ttd.metrics.cluster_lifecycle import ClusterLifecycleMetricPusher


class HDIClusterSensor(BaseSensorOperator):
    """
    Sensor operator for monitoring a HDInsight cluster.

    @param cluster_name: The name of the cluster to be monitored
    @type cluster_name: str
    @param resource_group: The resource group of the cluster to be monitored
    @type resource_group: str
    @param poke_interval: Time in seconds that the sensor should wait in between each try
    @type poke_interval: int
    """

    STEP_NAME = "wait_cluster_creation"

    template_fields = ["cluster_name"]

    def __init__(
        self,
        cluster_name: str,
        cluster_task_name: str,
        resource_group: str,
        components_to_monitor: Optional[List[str]] = None,
        rest_conn_id: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cluster_name = cluster_name
        self.cluster_task_name = cluster_task_name
        self.resource_group = resource_group
        self.rest_conn_id = rest_conn_id
        self.components_to_monitor = components_to_monitor or [ClusterComponents.SPARK2_THRIFTSERVER]
        self.manage_components = Variable.get("AZ_MANAGE_COMPONENTS_VIA_AMBARI", deserialize_json=True, default_var=False)
        self.allocated_cores: Optional[int] = None

    def _set_cores_usage(self, hdi_hook: HDIHook) -> None:
        try:
            cluster = hdi_hook.get_cluster(cluster_name=self.cluster_name, resource_group=self.resource_group)
            cores_used = cluster.properties.quota_info.cores_used
            self.allocated_cores = cores_used
        except Exception as e:
            self.log.error("There was an issue with the retrieval of the allocated resources")
            self.log.error(e, exc_info=True)

    def pre_execute(self, context):
        az_verbose_logs_level = Variable.get(AZ_VERBOSE_LOGS_VAR, default_var=WARNING_LEVEL)
        logging.getLogger("azure").setLevel(az_verbose_logs_level)

    def poke(self, context: Dict[str, Any]) -> bool:  # type: ignore
        hdi_hook = HDIHook()
        ambari_hook = AmbariHook(self.cluster_name, rest_conn_id=self.rest_conn_id)

        state = hdi_hook.get_cluster_state(cluster_name=self.cluster_name, resource_group=self.resource_group)
        self.log.info("Current state:" + str(state))

        if state == "Deleting":
            raise ClusterCreationError("The cluster is currently being removed.")

        if state == "Error":
            from azure.mgmt.hdinsight.models import ClusterGetProperties
            from azure.mgmt.hdinsight.models import Errors
            from ttd.hdinsight.hdi_metrics import send_hdi_metrics

            cluster = hdi_hook.get_cluster(self.cluster_name, self.resource_group)
            properties: ClusterGetProperties = cluster.properties
            errors: List[Errors] = properties.errors
            self.log.error("Cluster creation errors: " + "; ".join(f"[{e.code}] {e.message}" for e in errors))

            self.log.info("Sending fail execution metrics")

            error_code = ",".join(e.code for e in errors)
            error_message = ",".join(e.message for e in errors)

            ClusterLifecycleMetricPusher().cluster_startup_concluded(
                cluster_id=self.cluster_name, context=context, success=False, error_code=error_code, error_message=error_message
            )

            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
                error_code=error_code,
                error_message=error_message,
            )
            raise ClusterCreationError(f"Cluster creation failed: {error_code}. {error_message}")

        if state == "Running" or state == "Operational":
            self._set_cores_usage(hdi_hook)
            return True

        if self.manage_components and ambari_hook.is_ready():
            for component in self.components_to_monitor:
                ambari_hook.check_and_start_component(component_name=component)

        return False

    def execute(self, context: Context) -> Any:
        try:
            super().execute(context)
        except AirflowSensorTimeout as e:
            ClusterLifecycleMetricPusher().cluster_startup_concluded(
                cluster_id=self.cluster_name,
                context=context,
                success=False,
                error_code='STARTUP_TIMEOUT',
                startup_timeout=True,
            )

            raise e

    def post_execute(self, context: Dict[str, Any], result: Any = None) -> None:
        from ttd.hdinsight.hdi_metrics import send_hdi_metrics

        ClusterLifecycleMetricPusher().cluster_startup_concluded(
            cluster_id=self.cluster_name, context=context, success=True, total_cores=self.allocated_cores
        )

        self.log.info("Sending success execution metrics")
        send_hdi_metrics(
            operator=self,
            step_name=self.STEP_NAME,
            context=context,
        )


class ClusterCreationError(AirflowException):
    pass
