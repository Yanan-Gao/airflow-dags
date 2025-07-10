from abc import ABCMeta
from dataclasses import dataclass
from typing import List, Dict, Optional

from ttd.monads.maybe import Maybe, Nothing, Just


@dataclass
class AwsSubnet:
    az: str
    id: str
    name: str = ""


@dataclass
class AwsSecurityGroup:
    id: str
    name: str = ""


@dataclass
class EmrSecurityGroups:
    emr_managed_master_sg: AwsSecurityGroup
    emr_managed_slave_sg: AwsSecurityGroup
    service_access_sg: Maybe[AwsSecurityGroup]


class EmrVpc:

    def __init__(self, id: str, subnets: List[AwsSubnet], security_groups: EmrSecurityGroups):
        self.security_groups = security_groups
        self.id = id
        self.subnets: List[AwsSubnet] = subnets
        self.az_to_subnet: Dict[str, AwsSubnet] = {subnet.az: subnet for subnet in self.subnets}

    def __getattr__(self, item) -> AwsSubnet:
        subnet = self.az_to_subnet.get(item, None)
        if subnet is None:
            raise AttributeError
        return subnet

    def get_subnet_from_availability_zone(self, availability_zone: str) -> None | str:
        subnet_id = self.az_to_subnet.get(availability_zone)
        return subnet_id.id if subnet_id is not None else None

    def all(self) -> List[str]:
        return [subnet.id for subnet in self.subnets]


class AwsAccount(metaclass=ABCMeta):
    default_vpc: EmrVpc

    ec2_ssh_key_name: Optional[str]

    logs_uri: str


class DevComputeAccount(AwsAccount):
    vai_vpc: EmrVpc = EmrVpc(
        "vpc-0b14c83d9dc04f8f5", [
            AwsSubnet("us-east-1a", "subnet-06dc5a0e47ca95531"),
            AwsSubnet("us-east-1b", "subnet-00d893617f146b29c"),
            AwsSubnet("us-east-1c", "subnet-0d182e7abce64c20c"),
            AwsSubnet("us-east-1d", "subnet-0d718c880837c4963"),
            AwsSubnet("us-east-1e", "subnet-05fd1a87caa3f4a66"),
            AwsSubnet("us-east-1f", "subnet-00abd465b23268c45")
        ],
        EmrSecurityGroups(
            emr_managed_master_sg=AwsSecurityGroup("sg-05a8b3b2fbf951f94", "ElasticMapReduce-Slave-Private"),
            emr_managed_slave_sg=AwsSecurityGroup("sg-0a1e9e5bda7b05b99", "ElasticMapReduce-Master-Private"),
            service_access_sg=Just(AwsSecurityGroup("sg-0adb08144afdeed37", "ElasticMapReduce-ServiceAccess"))
        )
    )
    default_vpc = vai_vpc

    ec2_ssh_key_name: Optional[str] = None

    vai_logs_uri: str = "s3://ttd-bigdata-logs-useast1-503911722519/emr"
    logs_uri = vai_logs_uri


class ProductionAccount(AwsAccount):
    useast_emr_vpc = EmrVpc(
        "vpc-6688a602", [
            AwsSubnet("us-east-1a", "subnet-f2cb63aa"),
            AwsSubnet("us-east-1b", "subnet-0972a2bbd44d3eb6d"),
            AwsSubnet("us-east-1d", "subnet-7a86170c"),
            AwsSubnet("us-east-1e", "subnet-62cd6b48"),
            AwsSubnet("us-east-1f", "subnet-00471751ad92cbb5f")
        ],
        EmrSecurityGroups(
            emr_managed_master_sg=AwsSecurityGroup("sg-f50aa18d", "USEAST_EMR_SSH"),
            emr_managed_slave_sg=AwsSecurityGroup("sg-f50aa18d", "USEAST_EMR_SSH"),
            service_access_sg=Nothing()
        )
    )
    default_vpc = useast_emr_vpc

    ec2_ssh_key_name = "TheTradeDeskDeveloper_USEAST"

    useast_logs_uri: str = "s3://ttd-bigdata-logs-useast1-003576902480/emr"
    logs_uri = useast_logs_uri


class EmrSubnets:
    """
    EC2 Subnets

    Contains both Public and Private EC2 subnets
    Can be used to specify creation of a EMR cluster in a specific subnet/AZ(availability zone)
    """

    class Public:

        @classmethod
        def useast_emr_1a(cls) -> str:
            return "subnet-f2cb63aa"

        @classmethod
        def useast_emr_1b(cls) -> str:
            return "subnet-0972a2bbd44d3eb6d"

        @classmethod
        def useast_emr_1d(cls) -> str:
            return "subnet-7a86170c"

        @classmethod
        def useast_emr_1e(cls) -> str:
            return "subnet-62cd6b48"

        @classmethod
        def useast_emr_1f(cls) -> str:
            return "subnet-00471751ad92cbb5f"

        @classmethod
        def all(cls) -> List[str]:
            return [
                cls.useast_emr_1a(),
                cls.useast_emr_1b(),
                cls.useast_emr_1d(),
                cls.useast_emr_1e(),
                cls.useast_emr_1f(),
            ]

    class Private:

        @classmethod
        def useast_emr_1a(cls) -> str:
            return "subnet-00d4f85a"

    class PublicUSWest2:

        @classmethod
        def uswest_prod_1a(cls) -> str:
            return "subnet-01f511a711ddbf1a2"

        @classmethod
        def uswest_prod_1b(cls) -> str:
            return "subnet-02ab8c84190043301"

        @classmethod
        def uswest_prod_1c(cls) -> str:
            return "subnet-01eb5305059bbff53"

        @classmethod
        def all(cls) -> List[str]:
            return [cls.uswest_prod_1a(), cls.uswest_prod_1b(), cls.uswest_prod_1c()]

    class PrivateUSWest2:

        @classmethod
        def uswest_prod_1a(cls) -> str:
            return "subnet-0a179a8e66f957134"

        @classmethod
        def uswest_prod_1b(cls) -> str:
            return "subnet-0134632c6e178872f"

        @classmethod
        def uswest_prod_1c(cls) -> str:
            return "subnet-0ecde5acbc879f072"

        @classmethod
        def all(cls) -> List[str]:
            return [cls.uswest_prod_1a(), cls.uswest_prod_1b(), cls.uswest_prod_1c()]

    class PrivateAPNortheast1:

        @classmethod
        def ap_northeast_prod_1c(cls) -> str:
            return "subnet-00946ba4dd0614b24"

        @classmethod
        def ap_northeast_prod_1d(cls) -> str:
            return "subnet-02b17ae742ddd2116"

        @classmethod
        def all(cls) -> List[str]:
            return [cls.ap_northeast_prod_1c(), cls.ap_northeast_prod_1d()]

    class PrivateAPSoutheast1:

        @classmethod
        def ap_southeast_prod_1a(cls) -> str:
            return "subnet-035b4e6b4c6eb90cb"

        @classmethod
        def ap_southeast_prod_1b(cls) -> str:
            return "subnet-080cd358226b273ce"

        @classmethod
        def ap_southeast_prod_1c(cls) -> str:
            return "subnet-074309d2f8d23367a"

        @classmethod
        def all(cls) -> List[str]:
            return [
                cls.ap_southeast_prod_1a(),
                cls.ap_southeast_prod_1b(),
                cls.ap_southeast_prod_1c(),
            ]

    class PrivateEUCentral1:

        @classmethod
        def eu_central_prod_1a(cls) -> str:
            return "subnet-0219390730669e6d9"

        @classmethod
        def eu_central_prod_1b(cls) -> str:
            return "subnet-040fc9942d218764a"

        @classmethod
        def eu_central_prod_1c(cls) -> str:
            return "subnet-0a27c22d54ea4e86d"

        @classmethod
        def all(cls) -> List[str]:
            return [
                cls.eu_central_prod_1a(),
                cls.eu_central_prod_1b(),
                cls.eu_central_prod_1c(),
            ]

    class PrivateEUWest1:

        @classmethod
        def eu_west_prod_1a(cls) -> str:
            return "subnet-0f39ba052d5f7f5e5"

        @classmethod
        def eu_west_prod_1b(cls) -> str:
            return "subnet-0630bc13ba712e33f"

        @classmethod
        def eu_west_prod_1c(cls) -> str:
            return "subnet-01b343d57fe2fb5c4"

        @classmethod
        def all(cls) -> List[str]:
            return [cls.eu_west_prod_1a(), cls.eu_west_prod_1b(), cls.eu_west_prod_1c()]
