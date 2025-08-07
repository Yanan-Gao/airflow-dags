from typing import Dict, Any, Optional
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration


class DatabricksAwsConfiguration:
    availability: str = "ON_DEMAND"
    zone_id: str = "auto"
    ebs_config: DatabricksEbsConfiguration
    instance_profile_arn: Optional[str]

    def __init__(self, ebs_config: DatabricksEbsConfiguration, instance_profile_arn: Optional[str]):
        self.ebs_config = ebs_config
        self.instance_profile_arn = instance_profile_arn

    def to_dict(self) -> Dict[str, Any]:
        aws_attributes = {
            "availability": self.availability,
            "zone_id": self.zone_id,
        }
        # Add the ebs config in
        aws_attributes.update(self.ebs_config.to_dict())
        if self.instance_profile_arn is not None:
            aws_attributes['instance_profile_arn'] = self.instance_profile_arn
        return aws_attributes
