from abc import ABC, abstractmethod
from typing import Dict, Any, List


class EmrConf(ABC):

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        return {}

    def supported_on(self, emr_release_label: str):
        return True

    def merge(self, other_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # in reality, it can be recursive construction
        # i.e. {'Classification': 'spark-env', 'Configurations': [{'Classification': 'export', 'Configurations': [], 'Properties': {'PYSPARK_PYTHON': '/usr/local/bin/python3.10'}}]}
        # which means that copy should be recursive as well, but current version is only works with simple stuff.
        config = self.to_dict()
        new_configs = []
        updated_existing_conf = False
        for configuration in other_configs:
            if configuration["Classification"] == config["Classification"]:
                if "Properties" in config:
                    configuration["Properties"].update(config["Properties"])
                updated_existing_conf = True
            new_configs.append(configuration)
        if not updated_existing_conf:
            new_configs.append(config)
        return new_configs
