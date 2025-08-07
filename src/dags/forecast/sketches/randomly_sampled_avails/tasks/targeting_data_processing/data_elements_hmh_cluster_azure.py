from dags.forecast.sketches.randomly_sampled_avails.constants import STANDARD_CLUSTER_TAGS
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig

from ttd.slack.slack_groups import FORECAST

_NAME = "td-agg-az-cluster"
_ADDITIONAL_CLUSTER_TAGS = {'Team': FORECAST.team.jira_team}
_VM_CONFIG = HDIVMConfig(
    headnode_type=HDIInstanceTypes.Standard_D14_v2(),
    workernode_type=HDIInstanceTypes.Standard_D14_v2(),
    num_workernode=83,
    disks_per_node=0,
)


class DataElementsHMHClusterAzure(HDIClusterTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            vm_config=_VM_CONFIG,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            cluster_tags={
                **STANDARD_CLUSTER_TAGS,
                **_ADDITIONAL_CLUSTER_TAGS
            },
            enable_openlineage=False
        )
