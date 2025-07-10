from typing import Dict, Any

from ttd.eldorado.aws.cluster_configs.base import EmrConf
from ttd.openlineage import OpenlineageConfig
from ttd.ttdenv import TtdEnv
from ttd.cloud_provider import AwsCloudProvider


class OpenlineageClusterConfiguration(EmrConf):
    options: Dict[str, str]

    def __init__(self, openlineage_config: OpenlineageConfig, cluster_name: str, env: TtdEnv):
        self.options = openlineage_config.get_cluster_spark_defaults_options(cluster_name, AwsCloudProvider(), env)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "Classification": "spark-defaults",
            "Properties": self.options,
        }
