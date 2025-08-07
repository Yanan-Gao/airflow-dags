from datetime import datetime

from ttd.cloud_provider import CloudProviders
from ttd.cluster_service import ClusterServices


class CloudJobConfig:

    def __init__(
        self,
        start_date: datetime,
        capacity: int,
        parallelism: int,
        max_result_size_gb: int = 2,
        base_disk_space: int = 75,
        output_files: int = 200,
        metrics_base_disk_space: int = 10,
        disks_per_hdi_vm: int = 0,
        hdi_permanent_cluster: bool = False,
        metrics_disks_per_hdi_vm: int = 0,
    ):
        self.start_date = start_date
        self.provider = CloudProviders.aws
        self.cluster_service = ClusterServices.AwsEmr
        self.capacity = capacity
        self.parallelism = parallelism
        self.max_result_size_gb = max_result_size_gb
        self.base_disk_space = base_disk_space
        self.output_files = output_files
        self.metrics_base_disk_space = metrics_base_disk_space
        self.disks_per_hdi_vm = disks_per_hdi_vm
        self.hdi_permanent_cluster = hdi_permanent_cluster
        self.metrics_disks_per_hdi_vm = metrics_disks_per_hdi_vm


class LogNames:
    blockedByPreBidReasons = "BlockedByPreBidReasons"
    referrerCacheAvailableCategories = "ReferrerCacheAvailableCategories"
    referrerCacheAvailableRequestData = "ReferrerCacheAvailableRequestData"
    referrerCacheProviderRequestData = "ReferrerCacheProviderRequestData"
    adGroupContextualEntityPerformance = "AdGroupContextualEntityPerformance"
