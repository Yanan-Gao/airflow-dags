from abc import ABC, abstractmethod
from typing import Dict, List, TypedDict, NotRequired

EmrConfiguration = TypedDict(
    'EmrConfiguration',
    {
        'Classification': str,
        'Configurations': NotRequired[List['EmrConfiguration']],
        'Properties': NotRequired[Dict[str, str]]
    },
)


class EmrConf(ABC):

    @abstractmethod
    def to_dict(self) -> EmrConfiguration:
        pass

    def supported_on(self, emr_release_label: str):
        return True

    def merge(self, other_configs: List[EmrConfiguration]) -> List[EmrConfiguration]:
        conf: EmrConfiguration = self.to_dict()
        return EmrConf._merge(conf, other_configs)

    @staticmethod
    def _merge(config: EmrConfiguration, other_configs: List[EmrConfiguration]) -> List[EmrConfiguration]:
        # in reality, it can be recursive construction
        # i.e.
        # {
        #   'Classification': 'spark-env',
        #   'Configurations': [{
        #       'Classification': 'export',
        #       'Configurations': [],
        #       'Properties': {
        #          'PYSPARK_PYTHON': '/usr/local/bin/python3.10'
        #       }
        #    }]
        # }
        # {
        #    "Classification": "spark-env",
        #    "Configurations": [{
        #         "Classification": "export",
        #         "Configurations": [],
        #         "Properties": {
        #             "JAVA_HOME": self.java_location
        #         }
        #    }],
        # }
        # which means that copy should be recursive as well, but current version is only works with simple stuff.
        found_matching_classification = False
        for other_config in other_configs:
            if other_config["Classification"] == config["Classification"]:
                if "Properties" in config:
                    other_config["Properties"].update(config["Properties"])
                if "Configurations" in config:
                    for internal_config in config["Configurations"]:
                        EmrConf._merge(internal_config, other_config["Configurations"])
                found_matching_classification = True
        if not found_matching_classification:
            other_configs.append(config)
        return other_configs
