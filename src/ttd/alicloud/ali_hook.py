import json
from typing import List, Optional, Callable, TypeVar

from Tea.model import TeaModel
from airflow.hooks.base import BaseHook
from alibabacloud_emr20160408.client import Client as AliEmrClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_emr20160408 import models as emr_models
from alibabacloud_tea_util.models import RuntimeOptions

from ttd.mixins.retry_mixin import RetryMixin


class Defaults:
    __slots__ = ()
    NET_TYPE_VPC = "vpc"
    CLUSTER_TYPE_HADOOP = "HADOOP"
    CHARGE_TYPE_POST_PAID = "PostPaid"
    CN4_REGION_ID = "cn-shanghai"
    ACCESS_KEY_SECRET = "access_key_secret"
    ACCESS_KEY_ID = "access_key_id"
    MASTER_HOST_NAME = "emr-header-1"
    ENHANCED_SSD = "ESSD"
    DESTINATION_RESOURCE = "InstanceType"
    STATUS_AVAILABLE = "Available"
    WITH_STOCK = "WithStock"
    CN4_REGION_DEFAULT_VPC_ID = "vpc-uf6vehvy72nxyvfqbb1v6"
    VSWITCH_PREFIX = "Production-PrivateWAN"


class AliEMRHook(BaseHook, RetryMixin):

    def __init__(self, conn_id="alicloud_emr_access_key"):
        self.conn_id = conn_id
        self.client = self.get_conn()
        RetryMixin.__init__(self, max_retries=2, retry_interval=30, exponential_retry=True)

    def get_conn(self) -> AliEmrClient:
        connection = self.get_connection(self.conn_id)
        extra = json.loads(connection.get_extra())

        config = open_api_models.Config(
            access_key_id=extra[Defaults.ACCESS_KEY_ID],
            access_key_secret=connection.get_password(),
        )

        config.endpoint = "emr.aliyuncs.com"
        return AliEmrClient(config)

    Request = TypeVar("Request", bound=TeaModel)
    Response = TypeVar("Response", bound=TeaModel)

    def execute_api_call(
        self,
        api_method_with_options: Callable[[Request, RuntimeOptions], Response],
        request_model: Request,
        runtime_options: RuntimeOptions = RuntimeOptions(read_timeout=30 * 1000, autoretry=True)
    ) -> Response:
        maybe_response = self.with_retry(
            lambda: api_method_with_options(request_model, runtime_options),
            lambda ex: self.should_retry(ex),
        )
        return maybe_response.get()

    def should_retry(self, ex: Exception) -> bool:
        import traceback
        import Tea.exceptions
        traceback.print_exception(type(ex), ex, ex.__traceback__)

        if not isinstance(ex, Tea.exceptions.UnretryableException):
            self.log.info("Not of type UnretryableException. Won't retry")
            return False

        inner_ex = ex.inner_exception

        import requests
        if isinstance(inner_ex, requests.exceptions.ReadTimeout):
            self.log.info("Inner exception is a ReadTimeout. Should retry")
            return True

        self.log.info("Inner exception is not a ReadTimeout. Won't retry")
        return False

    def get_clusters(self,
                     region_id: str = Defaults.CN4_REGION_ID,
                     is_non_terminated_only: bool = True) -> List[emr_models.ListClustersResponseBodyClustersClusterInfo]:
        clusters = []
        page_number = 1
        page_size = 50
        status_list = [
            "CREATING",
            "RUNNING",
            "IDLE",
            "RELEASE_FAILED",
            "WAIT_FOR_PAY",
            "ABNORMAL",
        ] if is_non_terminated_only else []

        while True:
            list_clusters_request = emr_models.ListClustersRequest(
                region_id=region_id,
                page_number=page_number,
                page_size=page_size,
                status_list=status_list,
            )
            response = self.execute_api_call(self.client.list_clusters_with_options, list_clusters_request)
            clusters_response = response.body.clusters

            if clusters_response.cluster_info is None or len(clusters_response.cluster_info) == 0:
                break

            for cluster in clusters_response.cluster_info:
                clusters.append(cluster)

            page_number += 1

        return clusters

    def create_cluster(self, create_cluster_request: emr_models.CreateClusterV2Request):
        options = RuntimeOptions(read_timeout=30 * 1000, autoretry=True)
        response = self.execute_api_call(self.client.create_cluster_v2with_options, create_cluster_request, options)
        return response.body.cluster_id

    def delete_cluster(self, region_id: str, cluster_id: str):
        delete_cluster_request = emr_models.ReleaseClusterRequest(region_id=region_id, id=cluster_id)
        self.execute_api_call(self.client.release_cluster_with_options, delete_cluster_request)

    def describe_cluster(
        self, cluster_id: str, region_id: Optional[str] = Defaults.CN4_REGION_ID
    ) -> emr_models.DescribeClusterV2ResponseBody:
        request = emr_models.DescribeClusterV2Request(region_id=region_id, id=cluster_id)
        return self.execute_api_call(self.client.describe_cluster_v2with_options, request).body

    def get_cluster_state(self, region_id: str, cluster_id: str) -> str:
        return self.describe_cluster(region_id=region_id, cluster_id=cluster_id).cluster_info.status

    def get_livy_endpoint(self, region_id: str, cluster_id: str) -> str:
        get_cluster_master_request = emr_models.ListClusterHostRequest(
            region_id=region_id,
            cluster_id=cluster_id,
            host_name=Defaults.MASTER_HOST_NAME,
        )

        response = self.execute_api_call(self.client.list_cluster_host_with_options, get_cluster_master_request)
        private_ip = response.body.host_list.host[0].private_ip
        return f"http://{private_ip}:8998/batches"
