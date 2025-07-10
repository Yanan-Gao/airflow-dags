import json
import logging
from ttd.alicloud.ali_hook import Defaults
from airflow.hooks.base_hook import BaseHook
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient
from alibabacloud_vpc20160428 import models as vpc_models
from alibabacloud_vpc20160428.client import Client as AliVpcClient


class AliVPCHook(BaseHook):

    def __init__(self, conn_id="alicloud_emr_access_key"):
        self.conn_id = conn_id
        self.client = self.get_conn()

    def get_conn(self) -> AliVpcClient:
        connection = self.get_connection(self.conn_id)
        extra = json.loads(connection.get_extra())
        config = open_api_models.Config(
            access_key_id=extra[Defaults.ACCESS_KEY_ID],
            access_key_secret=connection.get_password(),
        )

        config.endpoint = "vpc.aliyuncs.com"
        return AliVpcClient(config)

    def describe_vswitches(self, region_id: str, zone_id: str, vpc_id: str):
        describe_vswitches_request = vpc_models.DescribeVSwitchesRequest(
            region_id=region_id,
            zone_id=zone_id,
            vpc_id=vpc_id,
        )
        runtime = util_models.RuntimeOptions()
        try:
            response = self.client.describe_vswitches_with_options(describe_vswitches_request, runtime)
        except Exception as e:
            UtilClient.assert_as_string(e)
            logging.error(f"describe_available_resource failed: {e}")

            return []
        return response.body.v_switches.v_switch
