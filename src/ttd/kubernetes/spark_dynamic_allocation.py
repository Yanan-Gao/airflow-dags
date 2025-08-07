from dataclasses import dataclass


@dataclass
class SparkDynamicAllocation:

    enabled: bool = False
    min_executors: int = 0
    max_executors: int = 100

    def to_dict(self):
        return {"enabled": self.enabled, "minExecutors": self.min_executors, "maxExecutors": self.max_executors}
