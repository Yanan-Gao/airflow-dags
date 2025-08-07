from airflow.utils.context import Context
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.ttdenv import TtdEnvFactory
from ttd.kubernetes.pod_resources import PodResources
from typing import Optional, List, Dict, Any
from enum import Enum
import os

PYTHONUNBUFFERED_VAL = "0"
TENZING_AUTH_CONFIG_AF_AUTH_CONTEXT = "airflow"
ENV = str(TtdEnvFactory.get_from_system())


class ApiActionEnum(Enum):
    PUT = "put"
    DELETE = "delete"


class KserveClientEnvEnum(Enum):
    PROD = "prod"
    PROD_TEST = "prodTest"
    NON_PROD = "nonProd"


class TenzingClusterEnum(Enum):
    GeneralUseProduction = "general-use-production"
    GeneralUseNonProduction = "general-use-non-production"
    Or2EksP01 = "or2-eks-p-01"
    Or2EksKpop01 = "or2-eks-kpop-01"


class TenzingBaseOperator(TtdKubernetesPodOperator):
    """
        :param tenzing_unique_id: Unique id of resource
        :param tenzing_namespace: Namespace where to PUT/DELETE from resource
        :param tenzing_api_action: Which API action to perform on resource (can be PUT or DELETE) -- note that if a resource
        doesn't exist the first PUT will create it.
        :param task_name: Airflow task property - name of task
        :param task_id: Airflow task property - id of task
        :param operator_namespace: Namespace where task is to be spun up
        !!! User must provide exactly ONE of the following three parameters: !!!
        -->
        :param tenzing_endpoint_configuration_spec: Json representing your endpoint configuration (this is identical to
        the endpoint definition you would provide in the model-serving-endpoints repo. See AIFUN docs for more info)
        :param tenzing_team_assets_configuration_spec: Json representing a team asset spec (identical to team definitions
        in the model-serving-endpoints repo. See AIFUN documentation for more information.)
        :param tenzing_resource_assets: List of JSON that represents the resource.
        <--
        :param tenzing_resource_is_ephemeral: Whether you plan to keep your resource as an ephemeral resource. Used for AIFUN metrics collection.
        :param tenzing_read_cluster: Whether you want the client to read from a specific cluster. Will default to the active
        cluster. Provided for flexibility but not recommended unless there is a specific reason to do this.
        :parm tenzing_write_cluster: Whether you want the client to write to a specific cluster. Will default to the active
        cluster. Provided for flexibility but not recommended unless there is a specific reason to do this.
        :param tenzing_kserve_client_env_config: Kserve Client env - can be prod, dev, or prodTest. Example use case:
        you want to hit the dev clusters from the prod environment. If you don't set this field in the prod Airflow env,
        it will default to hitting the prod clusters. Note that the Airflow prodtest environment will default to a dev Kserve config.
        :param tenzing_kserve_client_metric_labels: Any extra label value pairs to attach to the metrics that are generated
        by the script that runs in the operator. The script generates the following metrics:
        - kserve_af_tenzing_operator_api_call_succeeded_cnt
        - kserve_af_tenzing_operator_api_call_failed_cnt
        Both are queryable in Grafana and have some metadata already attached. They are metrics that are pushed on each DAG
        run.
        The rest of the parameters are Airflow specific -- we have provided defaults to them here.
        """

    def __init__(
        self,
        tenzing_unique_id: str,
        tenzing_namespace: str,
        tenzing_api_action: ApiActionEnum,
        task_name: str,
        task_id: str,
        operator_namespace="tenzing",
        tenzing_endpoint_configuration_spec: Optional[Dict[str, Any]] = None,
        tenzing_team_assets_configuration_spec: Optional[Dict[str, str]] = None,
        tenzing_resource_assets: Optional[List[Dict[str, Any]]] = None,
        tenzing_resource_is_ephemeral: bool = False,
        tenzing_read_cluster: Optional[TenzingClusterEnum] = None,
        tenzing_write_cluster: Optional[TenzingClusterEnum] = None,
        tenzing_kserve_client_env_config: Optional[KserveClientEnvEnum] = None,
        tenzing_kserve_client_metric_labels: Optional[Dict[str, str]] = None,
        pod_resources: Optional[PodResources
                                ] = PodResources(limit_cpu="500m", limit_memory="512Mi", request_cpu="100m", request_memory="512Mi"),
        random_name_suffix: bool = True,
        get_logs: bool = True,
        image_pull_policy: str = "Always",
        startup_timeout_seconds: int = 500,
        log_events_on_failure: bool = True,
        *args,
        **kwargs
    ):
        self._endpoint_configuration_spec = tenzing_endpoint_configuration_spec
        self._team_configuration_spec = tenzing_team_assets_configuration_spec
        self._resource_assets = tenzing_resource_assets
        self.arguments = [
            "tenzing_kserve_client/airflow/run_kserve_client/script.py",
            "--namespace",
            tenzing_namespace,
            "--unique_id",
            tenzing_unique_id,
            "--api_action",
            tenzing_api_action.value,
            "--ephemeral",
            str(tenzing_resource_is_ephemeral),
        ]
        # if PUT, need the json of the assets (or the file definition object)
        if tenzing_api_action == ApiActionEnum.PUT:
            self._update_resource_assets_args()

        # users can specify a particular cluster to read/write from
        if tenzing_read_cluster is not None:
            self.arguments.append("--read_cluster")
            self.arguments.append(tenzing_read_cluster.value)
        if tenzing_write_cluster is not None:
            self.arguments.append("--write_cluster")
            self.arguments.append(tenzing_write_cluster.value)
        # users can specify the env of the kserve client config
        if tenzing_kserve_client_env_config is not None:
            self.arguments.append("--kserve_client_config_env")
            self.arguments.append(tenzing_kserve_client_env_config.value)
        # users can specify extra metric labels/values to annotate runner metrics with
        if tenzing_kserve_client_metric_labels is not None:
            self.arguments.append("--metric_labels")
            self.arguments.append(str(tenzing_kserve_client_metric_labels))

        super().__init__(
            name=task_name,
            task_id=task_id,
            image="production.docker.adsrvr.org/ttd-base/scrum-aifun/tenzing_kserve_client:0.1.165",
            namespace=operator_namespace,
            resources=pod_resources,
            cmds=["python"],
            arguments=self.arguments,
            random_name_suffix=random_name_suffix,
            get_logs=get_logs,
            image_pull_policy=image_pull_policy,
            startup_timeout_seconds=startup_timeout_seconds,
            log_events_on_failure=log_events_on_failure,
            *args,
            **kwargs
        )

    def _update_resource_assets_args(self):
        values = [
            self._endpoint_configuration_spec,
            self._resource_assets,
            self._team_configuration_spec,
        ]
        if sum(v is not None for v in values) != 1:
            raise ValueError(
                "Exactly one of the following must be provided: an EndpointFileDefinition, a TeamFileDefinition, or a list of json resource assets. Only one can be provided per operator."
            )

        if self._resource_assets is not None:
            self.arguments.append("--resource_assets_json")
            self.arguments.append(str(self._resource_assets))
        if self._endpoint_configuration_spec is not None:
            self.arguments.append("--endpoint_spec_definition")
            self.arguments.append(str(self._endpoint_configuration_spec))
        if self._team_configuration_spec is not None:
            self.arguments.append("--team_spec_definition")
            self.arguments.append(str(self._team_configuration_spec))

    def execute(self, context: Context):
        owner = context["task"].owner.lower()
        dag_id = context["dag"].dag_id.lower()
        self.env_vars = [
            V1EnvVar(name="PYTHONUNBUFFERED", value=PYTHONUNBUFFERED_VAL),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AUTH_CONTEXT",
                value=TENZING_AUTH_CONFIG_AF_AUTH_CONTEXT,
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__DAG_OWNER",
                value=owner,
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__DAG_ID",
                value=dag_id,
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__TASK_ID",
                value=context["task"].task_id.lower(),
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__RUN_ID",
                value=context["task_instance"].run_id,
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__TRY_NUMBER",
                value=str(context["task_instance"].try_number),
            ),
            V1EnvVar(name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__ENV", value=ENV),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__AIRFLOW_IDENTITY__AIRFLOW_INSTANCE",
                value=os.environ["AIRFLOW__WEBSERVER__INSTANCE_NAME"],
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__USER_CREDENTIALS__USERNAME",
                value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name="vault-secrets", key="svc_tenzing_af_user")),
            ),
            V1EnvVar(
                name="TENZING_CORE_AUTH_CONFIG__USER_CREDENTIALS__PASSWORD",
                value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name="vault-secrets", key="svc_tenzing_af_pass")),
            ),
        ]
        self.arguments.append("--team")
        self.arguments.append(owner)
        self.arguments.append("--dag_id")
        self.arguments.append(dag_id)

        return super().execute(context)
