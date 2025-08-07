import unittest
from typing import List

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.cluster_configs.emr_conf import EmrConfiguration
from ttd.eldorado.aws.cluster_configs.java_configuration import JavaSparkEnvConf
from ttd.eldorado.aws.cluster_configs.pyspark import PysparkConfiguration


class EmrConfTest(unittest.TestCase):

    def test_emr_conf_merge__can_merge_same_classificaiton(self):
        configs = [JavaSparkEnvConf(17, AwsEmrVersions.AWS_EMR_SPARK_3_5), PysparkConfiguration("3.10")]

        other_configs: List[EmrConfiguration] = []

        for config in configs:
            other_configs = config.merge(other_configs)

        self.assertTrue(other_configs[0]['Configurations'][0]['Properties']['PYSPARK_PYTHON'] == '/usr/local/bin/python3.10')
        self.assertTrue(other_configs[0]['Configurations'][0]['Properties']['JAVA_HOME'] == '/usr/lib/jvm/java-17-amazon-corretto.x86_64')
