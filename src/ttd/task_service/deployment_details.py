import logging
from dataclasses import dataclass
from typing import Optional, Dict

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from airflow.exceptions import AirflowException
from kubernetes import config
from kubernetes.client import CoreV1Api, ApiException


@dataclass
class DeploymentDetails:
    current_version: str
    deployment_id: str
    deployment_time: str
    docker_image: str
    secret_name: str


def _namespace() -> str:
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
        return f.read()


def _k8s_client() -> CoreV1Api:
    config.load_incluster_config()
    return CoreV1Api()


def _cleanse_branch_name(branch_name: str) -> str:
    branch_name = branch_name.lower()

    branch_name = branch_name[:253]

    return branch_name


def _get_config_map_name(branch_name: Optional[str] = None, is_china: bool = False) -> str:
    config_map_name = 'task-service-deployment-details'

    if is_china:
        config_map_name += '-cn'

    if branch_name is not None:
        branch_name = _cleanse_branch_name(branch_name)
        config_map_name += f'-{branch_name}'

    return config_map_name


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5, max=10),
    retry=retry_if_exception_type(ApiException),
    reraise=True
)
def _read_configmap(name: str) -> Dict[str, str]:
    client = _k8s_client()
    namespace = _namespace()

    logging.info(f"Reading config map: {name}")
    cm = client.read_namespaced_config_map(name=name, namespace=namespace)

    return cm.data


def retrieve_deployment_details_from_k8s(branch_name: Optional[str] = None, is_china: bool = False) -> DeploymentDetails:
    config_map_name = _get_config_map_name(branch_name, is_china)

    try:
        data = _read_configmap(config_map_name)
    except ApiException as e:
        if e.status == 404:
            raise DeploymentDetailsConfigMapNotFoundError("We couldn't find the deployment details configmap.")
        else:
            raise e

    try:
        details = DeploymentDetails(
            current_version=data['CURRENT_VERSION'],
            deployment_id=data['DEPLOYMENT_ID'],
            deployment_time=data['DEPLOYMENT_TIME'],
            docker_image=data['DOCKER_IMAGE'],
            secret_name=data['SECRET_NAME'],
        )
    except KeyError:
        raise ConfigMapInvalidError("There seems to some values missing from the deployment details configmap.")

    return details


class DeploymentDetailsConfigMapNotFoundError(AirflowException):
    pass


class ConfigMapInvalidError(AirflowException):
    pass
