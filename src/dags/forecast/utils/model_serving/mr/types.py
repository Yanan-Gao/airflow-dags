"""Home for all the classes and type aliases"""
from typing import Optional, Sequence, TypedDict


class MlFlowModelSource:
    """Defines a model version in MlFlow"""

    template_fields: Sequence[str] = ("name", "_version")

    def __init__(self, name: str, version: str | int):
        self.name = name
        self._version = version

    @property
    def version(self) -> int:
        """
        Returns the model version cast to int
        We do it in this way as this property shouldn't be called until after templating
        """
        return int(self._version)


ModelSource = str | MlFlowModelSource


class AutoScaleSettings(TypedDict):
    """
    KServe AutoScale Settings
    """

    cpuTargetUtilizationPercentage: str  # Between 0 and 100


class CpuMemorySettings(TypedDict):
    """
    KServe CPU and Memory Settings
    """

    cpu: str
    memory: str


class ResourceSettings(TypedDict):
    """
    KServe Resource Settings
    """

    limits: CpuMemorySettings
    requests: CpuMemorySettings


class ScalingSettings(TypedDict, total=False):
    """
    KServe Scaling Settings
    It defines the vertical and horizontal scaling
    """

    autoScale: AutoScaleSettings
    minReplicas: int
    maxReplicas: int
    resources: ResourceSettings


class ModelServingEndpointDetails:
    """Used to store all the information required to spin up an model serving endpoint"""

    template_fields: Sequence[str] = (
        "team_name",
        "endpoint_group",
        "endpoint",
        "model_format",
        "model_source",
    )

    def __init__(
        self,
        team_name: str,
        endpoint_group: str,
        endpoint: str,
        model_format: str,
        model_source: ModelSource,
        scaling_settings: None | ScalingSettings = None,
        allow_endpoint_overwrite: bool = False,  # Will error if False and we try to overwrite an endpoint
        versions_to_keep: Optional[None] = None,  # When None, we won't delete old endpoints
    ):
        if versions_to_keep is not None and not isinstance(model_source, MlFlowModelSource):
            raise ValueError("Must provide mlflow model source when deleting old endpoints")
        self.team_name = team_name
        self.endpoint_group = endpoint_group
        self.endpoint = endpoint
        self.model_format = model_format
        self.model_source = model_source
        self.scaling_settings = scaling_settings
        self.allow_endpoint_overwrite = allow_endpoint_overwrite
        self.versions_to_keep = versions_to_keep
