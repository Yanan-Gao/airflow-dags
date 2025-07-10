class AliCloudEmrVersions:
    SPARK_3_SUPPORT_MAJOR_VERSION_THRESHOLD: int = 5

    ALICLOUD_EMR_SPARK_3_1: str = "EMR-5.6.0"
    "Kept only for the history, don't use!"

    ALICLOUD_EMR_SPARK_3_2: str = "EMR-5.6.0"
    "ALI EMR version EMR-5.6.0 (Spark 3.2.1), might change if EMR version updates"

    ALICLOUD_EMR_SPARK_3_2_1: str = "EMR-5.6.0"
    "ALI EMR version EMR-5.6.0 (Spark 3.2.1)"

    @staticmethod
    def is_spark_3(emr_version: str) -> bool:
        emr_version_prefix = "EMR-"
        major_version = int(emr_version.lstrip(emr_version_prefix)[0])
        return major_version >= AliCloudEmrVersions.SPARK_3_SUPPORT_MAJOR_VERSION_THRESHOLD
