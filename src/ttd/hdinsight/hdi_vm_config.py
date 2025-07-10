from ttd.azure_vm.hdi_instance_types import HDIInstanceType


class HDIVMConfig:
    """
    HDInsight virtual machines configuration

    @param headnode_type: Type of the head nodes instances
    @param workernode_type: Type of the worker nodes instances
    @param num_workernode: The number of worker nodes
    @param disks_per_node: number of disks per worker node
    """

    def __init__(
        self,
        headnode_type: HDIInstanceType,
        workernode_type: HDIInstanceType,
        num_workernode: int,
        disks_per_node: int = 0,
    ):
        self.headnode_type = headnode_type
        self.workernode_type = workernode_type
        self.num_workernode = num_workernode
        self.disks_per_node = disks_per_node

        # Default non-customisable value for any type of HDI cluster
        self.num_headnode = 2
