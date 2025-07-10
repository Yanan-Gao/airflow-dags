from typing import Dict

from ttd.cloud_provider import CloudProvider
from ttd.openlineage import OpenlineageEnv
from ttd.openlineage.constants import PROMETHEUS_ENDPOINT


class CircuitBreakerConfig:
    freeMemoryPercentage: float
    checkHeartbeatIntervalMs: int
    gcCpuPercentage: float
    timeoutInSeconds: int

    def __init__(self, freeMemoryPercentage: float = 20, checkHeartbeatIntervalMs=1000, gcCpuPercentage=10, timeoutInSeconds=30):
        self.freeMemoryPercentage = freeMemoryPercentage
        self.checkHeartbeatIntervalMs = checkHeartbeatIntervalMs
        self.gcCpuPercentage = gcCpuPercentage
        self.timeoutInSeconds = timeoutInSeconds

    def to_dict(self, job_name: str, env: OpenlineageEnv, cloud_provider: CloudProvider) -> Dict[str, str]:
        return {
            "spark.openlineage.circuitBreaker.type": "ttdCircuitBreaker",
            "spark.openlineage.circuitBreaker.memoryThreshold": str(self.freeMemoryPercentage),
            "spark.openlineage.circuitBreaker.gcCpuThreshold": str(self.gcCpuPercentage),
            "spark.openlineage.circuitBreaker.circuitCheckIntervalInMillis": str(self.checkHeartbeatIntervalMs),
            "spark.openlineage.circuitBreaker.timeoutInSeconds": str(self.timeoutInSeconds),
            "spark.openlineage.circuitBreaker.appName": job_name,
            "spark.openlineage.circuitBreaker.prometheusUrl": PROMETHEUS_ENDPOINT,
            "spark.openlineage.circuitBreaker.clusterService": str(cloud_provider),
            "spark.openlineage.circuitBreaker.environment": env.value,
        }
