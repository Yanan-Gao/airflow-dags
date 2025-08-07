from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

eldorado_jar: str = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-ctxmp-assembly.jar"

# EMR spark version should match eldorado spark version
aws_emr_version: str = AwsEmrVersions.AWS_EMR_SPARK_3_4
