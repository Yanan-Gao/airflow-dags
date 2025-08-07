from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConf, EmrConfiguration
from abc import ABC


class JavaEmrConf(EmrConf, ABC):

    def __init__(self, java_version: int, emr_version: str):
        self.java_version = java_version
        self.emr_version = emr_version

        set_emr_version = AwsEmrVersions.parse_version(emr_version)
        min_emr_version = AwsEmrVersions.parse_version(AwsEmrVersions.AWS_EMR_SPARK_3_4)
        max_emr_version = AwsEmrVersions.parse_version(AwsEmrVersions.AWS_EMR_SPARK_3_5)
        corretto_emr_version = AwsEmrVersions.parse_version(AwsEmrVersions.AWS_EMR_SPARK_3_5_0)

        if set_emr_version >= corretto_emr_version:
            self.min_version = corretto_emr_version
            self.max_version = max_emr_version
            self.java_location = f"/usr/lib/jvm/java-{self.java_version}-amazon-corretto.x86_64"
        else:
            self.min_version = min_emr_version
            self.max_version = corretto_emr_version
            self.java_location = f"/usr/lib/jvm/jre-{self.java_version}"

    def supported_on(self, emr_release_label: str):
        curr_version = AwsEmrVersions.parse_version(emr_release_label)
        return self.max_version > curr_version >= self.min_version


class JavaHadoopConf(JavaEmrConf):

    def to_dict(self) -> EmrConfiguration:
        return {
            "Classification": "hadoop-env",
            "Configurations": [{
                "Classification": "export",
                "Configurations": [],
                "Properties": {
                    "JAVA_HOME": self.java_location
                }
            }],
        }


class JavaSparkEnvConf(JavaEmrConf):

    def to_dict(self) -> EmrConfiguration:
        return {
            "Classification": "spark-env",
            "Configurations": [{
                "Classification": "export",
                "Configurations": [],
                "Properties": {
                    "JAVA_HOME": self.java_location
                }
            }],
        }


class JavaSparkDefaultsConf(JavaEmrConf):

    def to_dict(self) -> EmrConfiguration:
        return {"Classification": "spark-defaults", "Properties": {"spark.executorEnv.JAVA_HOME": self.java_location}}
