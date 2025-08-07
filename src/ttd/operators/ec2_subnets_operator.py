import logging
import random
from collections import defaultdict

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.utils.xcom import XCOM_RETURN_KEY
from typing import List, Set, Dict, Any

from ttd.ec2.ec2_subnet import ProductionAccount, DevComputeAccount
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ttdenv import TtdEnvFactory


class EC2SubnetsOperator(BaseOperator):

    def __init__(
        self,
        name: str,
        instance_types: List[EmrInstanceType],
        region_name: str = "us-east-1",
        is_random: bool = False,
        *args,
        **kwargs,
    ):
        self.task_id = f"select_subnets_{name}"
        self.instance_types = instance_types
        self.region_name = region_name
        self.is_random = is_random

        super().__init__(task_id=self.task_id, *args, **kwargs)  # type: ignore

    def execute(self, context: Dict[str, Any]) -> List[str]:  # type: ignore
        common_zones = self.get_common_zones()
        logging.info(f"Common zones: {common_zones}")

        if len(common_zones) == 0:
            raise EC2SubnetsOperatorError("No common zones found")

        vpc = DevComputeAccount.default_vpc if TtdEnvFactory.get_from_system() == TtdEnvFactory.dev \
            else ProductionAccount.default_vpc

        common_subnets = [subnet for zone in common_zones if (subnet := vpc.get_subnet_from_availability_zone(zone)) is not None]
        logging.info(f"Common subnets: {common_subnets}")

        if self.is_random:
            random_subnet = random.choice(common_subnets)
            logging.info(f"Random subnet: {random_subnet}")
            return random_subnet
        else:
            return common_subnets

    @property
    def ec2_subnets_template(self) -> str:
        return f'{{{{ task_instance.xcom_pull(task_ids="{self.task_id}", key="{XCOM_RETURN_KEY}") }}}}'

    def get_common_zones(self) -> Set[str]:
        if not self.instance_types:
            return set()

        ec2 = EC2Hook(api_type="client_type").conn

        response = ec2.describe_instance_type_offerings(
            LocationType="availability-zone",
            Filters=[{
                "Name": "instance-type",
                "Values": [instance_type.instance_name for instance_type in self.instance_types],
            }],
        )
        instance_type_offerings = response["InstanceTypeOfferings"]

        locations_grouped_by_instance_type = defaultdict(set)
        for offering in instance_type_offerings:
            locations_grouped_by_instance_type[offering["InstanceType"]].add(offering["Location"])

        common_zones = set.intersection(*locations_grouped_by_instance_type.values())
        return common_zones


class EC2SubnetsOperatorError(Exception):
    pass
