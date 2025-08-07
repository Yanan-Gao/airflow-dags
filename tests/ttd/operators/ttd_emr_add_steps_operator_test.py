import unittest

from ttd.monads.maybe import Just, Nothing
from ttd.operators.ttd_emr_add_steps_operator import TtdEmrAddStepsOperator


class TtdEmrAddStepsOperatorTest(unittest.TestCase):

    def test_get_class_from_step(self):
        expected_class = "com.Foo"
        good_step = {
            "Name": "test",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar":
                "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--conf",
                    "spark.sql.adaptive.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.coalescePartitions.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.skewJoin.enabled=true",
                    "--class",
                    expected_class,
                    "--executor-memory",
                    "138G",
                    "--executor-cores",
                    "5",
                    "--conf",
                    "spark.executor.memoryOverhead=3686m",
                    "--driver-java-options=-Dttd.env=prodTest",
                    "s3://foo/bar/baz.jar",
                    "",
                ],
            },
        }

        result_class = TtdEmrAddStepsOperator.get_class_from_step(good_step)
        self.assertEqual(result_class, Just(expected_class))

        no_class_step = {
            "Name": "test",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar":
                "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--conf",
                    "spark.sql.adaptive.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.coalescePartitions.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.skewJoin.enabled=true",
                    "--executor-memory",
                    "138G",
                    "--executor-cores",
                    "5",
                    "--conf",
                    "spark.executor.memoryOverhead=3686m",
                    "--driver-java-options=-Dttd.env=prodTest",
                    "s3://foo/bar/baz.jar",
                    "",
                ],
            },
        }
        result_class = TtdEmrAddStepsOperator.get_class_from_step(no_class_step)
        self.assertEqual(result_class, Nothing())

    def test_get_jar_from_step(self):
        expected_jar = "s3://foo/bar/baz.jar"
        good_step = {
            "Name": "test",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar":
                "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--conf",
                    "spark.sql.adaptive.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.coalescePartitions.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.skewJoin.enabled=true",
                    "--class",
                    "com.Foo",
                    "--executor-memory",
                    "138G",
                    "--executor-cores",
                    "5",
                    "--conf",
                    "spark.executor.memoryOverhead=3686m",
                    "--driver-java-options=-Dttd.env=prodTest",
                    expected_jar,
                    "",
                ],
            },
        }
        result_jar = TtdEmrAddStepsOperator.get_jar_from_step(good_step)
        self.assertEqual(result_jar, Just(expected_jar))

        no_jar_step = {
            "Name": "test",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar":
                "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--conf",
                    "spark.sql.adaptive.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.coalescePartitions.enabled=true",
                    "--conf",
                    "spark.sql.adaptive.skewJoin.enabled=true",
                    "--class",
                    "com.Foo",
                    "--executor-memory",
                    "138G",
                    "--executor-cores",
                    "5",
                    "--conf",
                    "spark.executor.memoryOverhead=3686m",
                    "--driver-java-options=-Dttd.env=prodTest",
                    "",
                ],
            },
        }
        result_jar = TtdEmrAddStepsOperator.get_jar_from_step(no_jar_step)
        self.assertEqual(result_jar, Nothing())
