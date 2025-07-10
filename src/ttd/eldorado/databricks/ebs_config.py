from typing import Dict, Union, Optional


class DatabricksEbsConfiguration:
    ebs_volume_count: int
    ebs_volume_size_gb: int
    ebs_volume_iops: Optional[int] = None
    ebs_volume_throughput: Optional[int] = None
    ebs_volume_type: str = "GENERAL_PURPOSE_SSD"

    def __init__(
        self,
        ebs_volume_count: int = 0,
        ebs_volume_size_gb: int = 0,
        ebs_volume_throughput=None,
        ebs_volume_iops=None,
    ):
        self.ebs_volume_count = ebs_volume_count
        self.ebs_volume_size_gb = ebs_volume_size_gb
        self.ebs_volume_throughput = ebs_volume_throughput
        self.ebs_volume_iops = ebs_volume_iops

    def to_dict(self) -> Dict[str, Union[str, int]]:
        if self.ebs_volume_count > 0:
            base = {
                "ebs_volume_type": self.ebs_volume_type,
                "ebs_volume_count": self.ebs_volume_count,
                "ebs_volume_size": self.ebs_volume_size_gb
            }

            if self.ebs_volume_iops is not None:
                base["ebs_volume_iops"] = self.ebs_volume_iops

            if self.ebs_volume_throughput is not None:
                base["ebs_volume_throughput"] = self.ebs_volume_throughput

            return base
        return {
            "ebs_volume_count": 0,
        }
