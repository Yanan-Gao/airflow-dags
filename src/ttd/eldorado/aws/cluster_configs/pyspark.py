from typing import Dict, Any

from ttd.eldorado.aws.cluster_configs.base import EmrConf


class PysparkConfiguration(EmrConf):

    def __init__(self, version: str) -> None:
        super().__init__()

        self.version = version

    def to_dict(self) -> Dict[str, Any]:
        return {
            "Classification":
            "spark-env",
            "Configurations": [{
                "Classification": "export",
                "Configurations": [],
                "Properties": {
                    "PYSPARK_PYTHON": f"/usr/local/bin/python{self.version}"
                }
            }],
        }
