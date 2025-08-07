from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConf, EmrConfiguration


class MaximizeResourceAllocationConf(EmrConf):

    def to_dict(self) -> EmrConfiguration:
        return {"Classification": "spark", "Configurations": [], "Properties": {"maximizeResourceAllocation": "true"}}
