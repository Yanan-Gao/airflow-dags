import json
import logging
from ttd.alicloud.ali_hook import Defaults
from airflow.hooks.base_hook import BaseHook
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_ecs20140526.client import Client as AliEcsClient


class AliECSHook(BaseHook):

    def __init__(self, conn_id="alicloud_emr_access_key"):
        self.conn_id = conn_id
        self.client = self.get_conn()

    def get_conn(self) -> AliEcsClient:
        connection = self.get_connection(self.conn_id)
        extra = json.loads(connection.get_extra())
        config = open_api_models.Config(
            access_key_id=extra[Defaults.ACCESS_KEY_ID],
            access_key_secret=connection.get_password(),
        )

        config.endpoint = "ecs.aliyuncs.com"
        return AliEcsClient(config)

    def describe_available_resource(
        self,
        region_id: str,
        instance_type: str,
        destination_resource: str = "InstanceType",
        instance_charge_type: str = "PostPaid",
    ):
        describe_available_resource_request = (
            ecs_models.DescribeAvailableResourceRequest(
                region_id=region_id,
                instance_charge_type=instance_charge_type,
                destination_resource=destination_resource,
                system_disk_category="ESSD",
                instance_type=instance_type,
            )
        )
        runtime = util_models.RuntimeOptions()
        try:
            response = self.client.describe_available_resource_with_options(describe_available_resource_request, runtime)
        except Exception as e:
            UtilClient.assert_as_string(e)
            logging.error(f"describe_available_resource failed: {e}")
        if response.status_code != 200:
            logging.error("describe_available_resource failed!")
            return []
        return response.body.available_zones.available_zone
