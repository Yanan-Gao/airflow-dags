from dataclasses import dataclass


@dataclass
class SparkVersion:

    spark_version: str
    hadoop_version: str


class SparkVersions:
    """
    Spark versions that are supported by Spark on K8s.
    """

    # TODO - Hadoop only publishes specific versions on Docker hub.
    # So if we want versions that match Spark version support, we'd need our own builds.
    # Tags: https://hub.docker.com/r/apache/hadoop/tags
    v3_3_1 = SparkVersion("3.3.1", "3.3.6")  # Actual Hadoop version: 3.3.2
    v3_2_1 = SparkVersion("3.2.1", "3.3.6")  # Actual Hadoop version: 3.3.1
