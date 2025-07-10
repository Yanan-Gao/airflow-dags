from __future__ import annotations
from ttd.semver import SemverVersion


class AwsEmrVersions:
    """
    Version of EMR designated to each Spark major.minor version will be updated automatically upon
    release of the new EMR version with updated Spark patch version.

    For example AWS_EMR_SPARK_3_3 points to "emr-6.11.1", which contains Spark 3.3.2. Upon release new version of EMR with Spark 3.3.3,
    it will be put for AWS_EMR_SPARK_3_3.
    """

    SPARK_3_SUPPORT_MAJOR_VERSION_THRESHOLD: int = 6
    SPARK_3_SUPPORT_MINOR_VERSION_THRESHOLD: int = 1

    AWS_EMR_SPARK_3_2: str = "emr-6.7.0"
    "AWS EMR version emr-6.7.0 (Spark 3.2.1), will change when EMR version updates"

    AWS_EMR_SPARK_3_2_1: str = "emr-6.7.0"
    "AWS EMR version emr-6.7.0 (Spark 3.2.1)"

    AWS_EMR_SPARK_3_3: str = "emr-6.11.1"
    "AWS EMR version emr-6.11.1 (Spark 3.3.2), will change when EMR version updates"

    AWS_EMR_SPARK_3_3_1: str = "emr-6.10.1"
    "AWS EMR version emr-6.10.1 (Spark 3.3.1)"

    AWS_EMR_SPARK_3_3_2: str = "emr-6.11.1"
    "AWS EMR version emr-6.11.1 (Spark 3.3.2)"

    AWS_EMR_SPARK_3_4: str = "emr-6.14.0"
    "AWS EMR version emr-6.14.0 (Spark 3.4.1), will change when EMR version updates"

    AWS_EMR_SPARK_3_4_0: str = "emr-6.12.0"
    "AWS EMR version emr-6.12.0 (Spark 3.4.0)"

    AWS_EMR_SPARK_3_5: str = "emr-7.9.0"
    "AWS EMR version emr-7.9.0 (Spark 3.5.5), will change when EMR version updates"

    AWS_EMR_SPARK_3_5_0: str = "emr-7.0.0"
    "AWS EMR version emr-7.0.0 (Spark 3.5.0)"

    AWS_EMR_SPARK_3_5_2: str = "emr-7.5.0"
    "AWS EMR version emr-7.5.0 (Spark 3.5.2)"

    AWS_EMR_SPARK_3_5_5: str = "emr-7.9.0"
    "AWS EMR version emr-7.9.0 (Spark 3.5.5)"

    @staticmethod
    def parse_version(emr_version) -> SemverVersion:
        emr_version_prefix = "emr-"
        version_separator = "."
        major_version_str, minor_version_str, patch = emr_version.lstrip(emr_version_prefix).split(version_separator)
        major_version = int(major_version_str)
        minor_version = int(minor_version_str)
        patch_version = int(patch)
        return SemverVersion(major_version, minor_version, patch_version)

    @staticmethod
    def is_spark_3(emr_version: str) -> bool:
        v = AwsEmrVersions.parse_version(emr_version)
        if v.major > AwsEmrVersions.SPARK_3_SUPPORT_MAJOR_VERSION_THRESHOLD:
            return True
        elif v.major == AwsEmrVersions.SPARK_3_SUPPORT_MAJOR_VERSION_THRESHOLD and v.minor >= AwsEmrVersions.SPARK_3_SUPPORT_MINOR_VERSION_THRESHOLD:
            return True
        else:
            return False
