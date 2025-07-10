from ttd.cloud_provider import CloudProvider, CloudProviderMapper
from ttd.cluster_service import ClusterService


class CloudJobConfigOld:

    def __init__(
        self,
        provider: str,
        capacity: int,
        base_disk_space: int = 75,
        output_files: int = 200,
    ):
        self.provider = provider
        self.capacity = capacity
        self.base_disk_space = base_disk_space
        self.output_files = output_files


class CloudJobConfig:

    def __init__(
        self,
        provider: CloudProvider,
        capacity: int,
        metrics_capacity: int,
        base_disk_space: int = 75,
        output_files: int = 200,
        metrics_base_disk_space: int = 10,
        cluster_service: ClusterService = None,
        disks_per_hdi_vm: int = 0,
        hdi_permanent_cluster: bool = False,
        metrics_disks_per_hdi_vm: int = 0,
    ):
        self.provider = provider
        self.capacity = capacity
        self.metrics_capacity = metrics_capacity
        self.base_disk_space = base_disk_space
        self.output_files = output_files
        self.metrics_base_disk_space = metrics_base_disk_space
        self.disks_per_hdi_vm = disks_per_hdi_vm
        self.hdi_permanent_cluster = hdi_permanent_cluster
        self.metrics_disks_per_hdi_vm = metrics_disks_per_hdi_vm
        self.cluster_service = cluster_service if cluster_service is not None else CloudProviderMapper.get_cluster_service(self.provider)


class MetricCloudJobConfig(CloudJobConfig):

    def __init__(self, provider: CloudProvider, capacity: int, base_disk_space: int = 10):
        super(MetricCloudJobConfig, self).__init__(provider, capacity, base_disk_space)


class LogNames:
    bidfeedback = "BidFeedback"
    bidfeedbackverticaload = "BidFeedbackVerticaLoad"
    bidrequest = "BidRequest"
    clicktracker = "ClickTracker"
    clicktrackerverticaload = "ClickTrackerVerticaLoad"
    controlbidrequest = "ControlBidRequest"
    conversiontracker = "ConversionTracker"
    conversiontrackerverticaload = "ConversionTrackerVerticaLoad"
    eventtrackerverticaload = "EventTrackerVerticaLoad"
    videoevent = "VideoEvent"
    attributedeventverticaload = "AttributedEventVerticaLoad"
    attributedeventresultverticaload = "AttributedEventResultVerticaLoad"
    attributedeventdataelementverticaload = "AttributedEventDataElementVerticaLoad"
