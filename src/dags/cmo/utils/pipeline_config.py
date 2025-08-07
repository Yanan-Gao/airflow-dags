from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ttdenv import TtdEnv, TtdEnvFactory


class PipelineConfig:

    def __init__(
        self,
        jar: str = None,
        env: TtdEnv = TtdEnvFactory.get_from_system(),
    ):
        self.env = env

        # Spark 3 configuration
        self.jar = jar or 's3://ttd-build-artefacts/tv-data/prod/latest/jars/ltv-data-jobs.jar'
        self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5

    # Creating these as functions rather than properties so we don't pass by reference and update the original. We want a new version each time we use it
    def get_cluster_additional_configurations(self):
        return [{"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}]

    # TODO: We've been putting this on all of our jobs. we should investigate if this has much use when using maxmizeResourceAllocation
    def get_step_additional_configurations(self):
        return [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer")]
