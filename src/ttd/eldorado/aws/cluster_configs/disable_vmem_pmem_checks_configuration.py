from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConf, EmrConfiguration


class DisableVmemPmemChecksConfiguration(EmrConf):
    # According to best practices of managing memory on EMR even if you configure your executor numbers/memory/overheads
    # correctly, the container could go slightly over the limit and get killed by YARN. Therefore, virtual and physical
    # memory checks needs to be disabled
    # see https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    def to_dict(self) -> EmrConfiguration:
        return {
            "Classification": "yarn-site",
            "Properties": {
                "yarn.nodemanager.pmem-check-enabled": "false",
                "yarn.nodemanager.vmem-check-enabled": "false",
            },
            "Configurations": [],
        }
