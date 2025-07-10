from typing import Dict, List, Tuple, Optional
from ttd.cloud_provider import CloudProvider, DatabricksCloudProvider, AwsCloudProvider
from ttd.openlineage.env import OpenlineageEnv
from ttd.openlineage.circuit_breaker import CircuitBreakerConfig
from ttd.openlineage.assets import OpenlineageAssetsConfig
from ttd.openlineage.constants import OPENLINEAGE_PRODUCTION_ENDPOINT, OPENLINEAGE_DEVELOPMENT_ENDPOINT, \
    PROMETHEUS_ENDPOINT
from ttd.ttdenv import TtdEnv, ProdEnv
from enum import Enum


class OpenlineageTransport(Enum):
    HTTP = "http"
    ROBUST = "robust"


class IgnoreOwnershipType(Enum):
    """
    Add values here as needed if ownership needs to be ignored. Value is high level description of ignored job type
    Usage: OpenLineageConfig(ignore_ownership=IgnoreOwnershipType.PDG_SCRUBBING)
    """
    PDG_SCRUBBING = 'pdg_scrubbing'


class OpenlineageConfig:
    enabled: bool
    collect_performance_metrics: bool
    transport: OpenlineageTransport
    transport_timeout: int
    overhead_push_threshold: float
    assets_config: OpenlineageAssetsConfig
    circuit_breaker_config: CircuitBreakerConfig
    input_limit_size: int
    ignore_ownership: Optional[IgnoreOwnershipType]

    def __init__(
        self,
        enabled: bool = True,
        collect_performance_metrics: bool = False,
        transport: OpenlineageTransport = OpenlineageTransport.HTTP,
        transport_timeout: int = 5000,
        overhead_push_threshold: float = 1,
        assets_config: OpenlineageAssetsConfig = OpenlineageAssetsConfig(),
        circuit_breaker_config: CircuitBreakerConfig = CircuitBreakerConfig(),
        input_limit_size: int = 100,
        ignore_ownership: Optional[IgnoreOwnershipType] = None,
    ):
        """
        :param enabled: Whether lineage is collected
        :param collect_performance_metrics: Whether low level query plans are collected.
        :param transport: What lineage transport to use (HTTP or Robust)
        :param transport_timeout: The timeout in ms of the http transit
        :param overhead_push_threshold: The timeout in minutes before overhead metrics are automatically pushed
        :param assets_config: The location of the init script which installs the openlineage fork
        :param circuit_breaker_config: Controls when openlineage code should be cancelled because of memory issues
        :param input_limit_size: Controls the max amount of inputs openlineage analyzes for 1 event
        :param ignore_ownership: Config option to ignore ownership on associated jobs. A set value indicates ownership will be ignored. Set value to job type e.g. "PDG scrubbing"
        """
        self.set_enabled(enabled)
        self.set_transport_type(transport)
        self.set_transport_timeout(transport_timeout)
        self.set_overhead_push_threshold(overhead_push_threshold)
        self.set_assets(assets_config)
        self.set_collect_performance_metrics(collect_performance_metrics)
        self.set_circuit_breaker(circuit_breaker_config)
        self.set_input_limit_size(input_limit_size)
        self.set_ignore_ownership(ignore_ownership)

    @staticmethod
    def supports_region(region: str):
        if region == "eu-central-1":
            return False
        return True

    # Setters
    def set_enabled(self, enabled: bool) -> None:
        """
        :param enabled: Whether lineage is collected
        """
        self.enabled = enabled
        """Whether lineage is collected"""

    def set_transport_type(self, transport: OpenlineageTransport) -> None:
        """
        :param transport: The transport type used for sending events
        """
        self.transport = transport
        """The transport used for sending lineage events"""

    def set_transport_timeout(self, transport_timeout: int) -> None:
        """
        :param transport_timeout: The number of milliseconds before giving up on sending the request via HTTP transport
        """
        self.transport_timeout = transport_timeout
        """The number of milliseconds before giving up on sending the request via HTTP transport"""

    def set_overhead_push_threshold(self, overhead_push_threshold: float) -> None:
        """
        :param overhead_push_threshold: If overhead exceeds this threshold (in minutes), automatically send it
        """
        self.overhead_push_threshold = overhead_push_threshold
        """If overhead exceeds this threshold (in minutes), automatically send it"""

    def set_assets(self, openlineage_assets: OpenlineageAssetsConfig) -> None:
        """
        :param openlineage_assets: The s3 location to the assets that install the openlineage functionality on the relevant cluster
        """
        self.assets_config = openlineage_assets
        """The s3 location to the init script that installs the openlineage fork on the relevant cluster"""

    def set_collect_performance_metrics(self, collect_performance_metrics: bool) -> None:
        """
        :param collect_performance_metrics: Whether low level query plans are collected
        """
        self.collect_performance_metrics = collect_performance_metrics
        """Whether low level query plans are collected"""

    def set_circuit_breaker(self, config: CircuitBreakerConfig) -> None:
        """
        :param config: The config specifying when to kill the openlineage execution
        """
        self.circuit_breaker_config = config
        """The memory and time thresholds that control when openlineage code is killed"""

    def set_input_limit_size(self, input_limit_size: int):
        """
        :param input_limit_size: The max amount of inputs openlineage will analyze for 1 event.
        """
        self.input_limit_size = input_limit_size
        """The max amount of inputs openlineage will analyze for 1 event."""

    def set_ignore_ownership(self, ignore_ownership: Optional[IgnoreOwnershipType]):
        """
        :param ignore_ownership: Config option to ignore ownership on associated jobs. A set value indicates ownership will be ignored. Set value to job type e.g. "PDG scrubbing"
        """
        self.ignore_ownership = ignore_ownership

    # Getters
    def _get_facets_to_ignore(self) -> str:
        facets = ["spark_unknown"]
        if not self.collect_performance_metrics:
            facets.append("spark.logicalPlan")
        return "[" + ";".join(facets) + (";" if len(facets) == 1 else "") + "]"

    def _get_openlineage_env(self, env: TtdEnv) -> OpenlineageEnv:
        if env == ProdEnv():
            return OpenlineageEnv.PROD
        return OpenlineageEnv.TEST

    def _get_openlineage_proxy_endpoint(self, env: OpenlineageEnv) -> str:
        if env == OpenlineageEnv.PROD:
            return OPENLINEAGE_PRODUCTION_ENDPOINT
        return OPENLINEAGE_DEVELOPMENT_ENDPOINT

    def _get_sumo_endpoint(self, cloud_provider: CloudProvider) -> str:
        if cloud_provider == DatabricksCloudProvider():
            return "{% raw %}{{secrets/openlineage/sumoEndpoint}}{% endraw %}"
        return ""

    def get_java_job_name(self, app_name: str, class_name: str) -> str:
        return app_name + "-class-" + class_name

    def get_python_job_name(self, app_name: str, executable_path: str) -> str:
        python_file = executable_path.split("/")[-1]
        return app_name + "-python-" + python_file

    def _get_spark_options(self, job_name: str, namespace: str, cloud_provider: CloudProvider, env: TtdEnv) -> Dict[str, str]:
        openlineage_env = self._get_openlineage_env(env)
        options = {
            "spark.app.name": job_name,
            "spark.openlineage.transport.type": self.transport.value,
            "spark.openlineage.facets.disabled": self._get_facets_to_ignore(),
            "spark.openlineage.namespace": namespace,
            "spark.openlineage.transport.prometheusUrl": PROMETHEUS_ENDPOINT,
            "spark.openlineage.transport.metricsPushTimeout": str(self.overhead_push_threshold),
            "spark.openlineage.transport.clusterService": str(cloud_provider),
            "spark.openlineage.transport.environment": openlineage_env.value,
            "spark.openlineage.job.owners.team": "{{ task.owner }}",
            "spark.openlineage.job.owners.dag": "{{ task.dag_id }}",
            "spark.openlineage.inputSizeLimit": str(self.input_limit_size)
        }
        if self.ignore_ownership:
            options["spark.openlineage.job.owners.ignore_ownership"] = self.ignore_ownership.value
        if cloud_provider == AwsCloudProvider():
            # On aws, we have to pass this arg in ourselves. On databricks, its passed in via the initscript.
            options["spark.extraListeners"] = "io.openlineage.spark.agent.OpenLineageSparkListener"
        if cloud_provider == DatabricksCloudProvider():
            sumo_url = self._get_sumo_endpoint(cloud_provider)
            options["spark.openlineage.transport.sumoUrl"] = sumo_url
            options["spark.openlineage.circuitBreaker.sumoUrl"] = sumo_url
        if self.transport == OpenlineageTransport.ROBUST:
            # This argument should only be set to true on providers that support HDFS.
            if cloud_provider == AwsCloudProvider():
                options["spark.openlineage.transport.usesHdfs"] = "true"
            else:
                options["spark.openlineage.transport.usesHdfs"] = "false"
        if self.transport == OpenlineageTransport.HTTP:
            http_options = {
                "spark.openlineage.transport.url": self._get_openlineage_proxy_endpoint(openlineage_env),
                "spark.openlineage.transport.endpoint": "api/v1/lineage",
                "spark.openlineage.transport.clusterService": str(cloud_provider),
                "spark.openlineage.transport.timeoutInMillis": str(self.transport_timeout),
                "spark.openlineage.transport.appName": job_name
            }
            options.update(http_options)
        options.update(self.circuit_breaker_config.to_dict(job_name, openlineage_env, cloud_provider))

        return options

    def get_cluster_spark_defaults_options(self, namespace: str, cloud_provider: CloudProvider, env: TtdEnv) -> Dict[str, str]:
        options = self._get_spark_options("default_app_name", namespace, cloud_provider, env)
        non_cluster_option_keys = ["spark.app.name", "spark.openlineage.circuitBreaker.appName", "spark.extraListeners"]
        return {k: v for (k, v) in options.items() if k not in non_cluster_option_keys}

    def _get_unique_spark_options(self, job_name: str, namespace: str, cloud_provider: CloudProvider, env: TtdEnv) -> Dict[str, str]:
        # We only want to set options that are non defaults on the individual steps.
        default_options = OpenlineageConfig().get_cluster_spark_defaults_options(namespace, cloud_provider, env)
        return {
            k: v
            for (k, v) in self._get_spark_options(job_name, namespace, cloud_provider, env).items()
            if k not in default_options or default_options[k] != v
        }

    def get_eldorado_opts_to_disable_bundled_openlineage(self) -> List[Tuple[str, str]]:
        return [("openlineage.enable", "false")]

    def get_java_spark_options_dict(self, app_name: str, class_name: str, namespace: str, env: TtdEnv,
                                    provider: CloudProvider) -> Dict[str, str]:
        job_name = self.get_java_job_name(app_name, class_name)
        if provider == DatabricksCloudProvider():
            return self._get_spark_options(job_name, namespace, provider, env)
        return self._get_unique_spark_options(job_name, namespace, provider, env)

    def get_java_spark_options_list(self, app_name: str, class_name: str, namespace: str, env: TtdEnv,
                                    provider: CloudProvider) -> List[Tuple[str, str]]:
        return [("conf", f"{k}={v}") for k, v in self.get_java_spark_options_dict(app_name, class_name, namespace, env, provider).items()]

    def get_pyspark_options_dict(self, app_name: str, executable_path: str, namespace: str, env: TtdEnv,
                                 provider: CloudProvider) -> Dict[str, str]:
        job_name = self.get_python_job_name(app_name, executable_path)
        if provider == DatabricksCloudProvider():
            return self._get_spark_options(job_name, namespace, provider, env)
        return self._get_unique_spark_options(job_name, namespace, provider, env)

    def get_pyspark_options_list(self, app_name: str, executable_path: str, namespace: str, env: TtdEnv,
                                 provider: CloudProvider) -> List[Tuple[str, str]]:
        return [("conf", f"{k}={v}") for k, v in self.get_pyspark_options_dict(app_name, executable_path, namespace, env, provider).items()]

    def _get_robust_upload_params(self, job_name: str, cloud_provider: CloudProvider, env: TtdEnv) -> List[str]:
        if cloud_provider == DatabricksCloudProvider():
            return ["--src_dir_path", "/openlineage_events", "--env", self._get_openlineage_env(env).value, "--job_name", job_name]
        elif cloud_provider == AwsCloudProvider():
            return [job_name, self._get_openlineage_env(env).value]
        return []

    def get_robust_upload_params_for_java_task(self, app_name: str, class_name: str, cloud_provider: CloudProvider,
                                               env: TtdEnv) -> List[str]:
        return self._get_robust_upload_params(self.get_java_job_name(app_name, class_name), cloud_provider, env)

    def get_robust_upload_params_for_pyspark_task(self, app_name: str, executable_path: str, cloud_provider: CloudProvider,
                                                  env: TtdEnv) -> List[str]:
        return self._get_robust_upload_params(self.get_python_job_name(app_name, executable_path), cloud_provider, env)
