import logging
from typing import Dict, Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from azure.core.exceptions import ResourceExistsError, HttpResponseError

from ttd.eldorado.azure.hdi_cluster_config import HdiClusterConfig
from ttd.hdinsight.hdi_hook import HDIHook, AZ_VERBOSE_LOGS_VAR, WARNING_LEVEL
from ttd.metrics.cluster_lifecycle import ClusterLifecycleMetricPusher
from ttd.mixins.retry_mixin import RetryMixin, RetryLimitException


class HDIDeleteClusterOperator(BaseOperator, RetryMixin):
    """
    An operator which deletes an HDInsight cluster.

    @param cluster_name: The name of the cluster to be deleted.
    @type cluster_name: str
    @param cluster_config: Contains resource group, virtual network, storage account, credentials.
    """

    STEP_NAME = "kill_cluster"

    template_fields = ["cluster_name"]

    def __init__(
        self,
        cluster_name: str,
        cluster_config: HdiClusterConfig,
        permanent_cluster: bool = False,
        *args,
        **kwargs,
    ):
        super(HDIDeleteClusterOperator, self).__init__(*args, **kwargs)
        RetryMixin.__init__(self, max_retries=3, retry_interval=2 * 60, exponential_retry=False)
        self.cluster_name = cluster_name
        self.cluster_config = cluster_config
        self.permanent_cluster = permanent_cluster

    def pre_execute(self, context):
        az_verbose_logs_level = Variable.get(AZ_VERBOSE_LOGS_VAR, default_var=WARNING_LEVEL)
        logging.getLogger("azure").setLevel(az_verbose_logs_level)

    def _should_fail(self, err) -> bool:
        if isinstance(err, HttpResponseError):
            if (err.error.code in ('ResourceNotFound', 'NotFound')  # type: ignore
                    or (err.error.code == 'PreconditionFailed' and 'No live cluster exists' in err.error.message)):  # type: ignore
                self.log.warning(
                    f"Ignoring error response as it seems cluster has been deleted. Error code: {err.error.code}, message: {err.error.message}"  # type: ignore
                )
                return False
        elif isinstance(err, RetryLimitException):
            self.log.warning("Failed deleting cluster after max number of attempts.")
            return False
        return True

    def execute(self, context: Dict[str, Any]):  # type: ignore
        from ttd.hdinsight.hdi_metrics import send_hdi_metrics

        hdi_hook = HDIHook()

        if self.permanent_cluster:
            state = "Error"
            try:
                state = hdi_hook.get_cluster_state(self.cluster_name, self.cluster_config.resource_group)
            except AirflowException as e:
                self.log.error("Error while getting the final state of the cluster", e)
            finally:
                if state != "Error" and state != "Deleting":
                    self.log.info(f"The cluster is in good health, leaving it for the next job. Cluster state: {state}")
                    return

        self.log.info("Executing delete cluster operator")
        resource_group = self.cluster_config.resource_group
        maybe_result = self.with_retry(
            lambda: hdi_hook.delete_cluster2(resource_group=resource_group, cluster_name=self.cluster_name),
            lambda ex: isinstance(ex, ResourceExistsError),
        )

        if maybe_result.is_failure:
            err = maybe_result.failed().get()
            if isinstance(err, HttpResponseError):
                self.log.info("Sending fail execution metrics")
                send_hdi_metrics(
                    operator=self,
                    step_name=self.STEP_NAME,
                    context=context,
                    error_code=err.error.code,  # type: ignore
                    error_message=err.error.message,  # type: ignore
                )

                if self._should_fail(err):
                    raise err

        else:
            self.log.debug(f"Result type: {type(maybe_result.get())}")

            for result in maybe_result:
                self.log.info(f"Cluster deletion output: {result}")

            self.log.info("Sending success execution metrics")
            send_hdi_metrics(
                operator=self,
                step_name=self.STEP_NAME,
                context=context,
            )
            ClusterLifecycleMetricPusher().cluster_termination_requested(self.cluster_name, context)

        self.log.info("Finished executing delete cluster operator")
