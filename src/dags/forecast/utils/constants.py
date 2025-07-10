from typing import Final

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions

###########################################
#  Aerospike Configuration
###########################################
AEROSPIKE_HOSTS: Final = '{{macros.ttd_extras.resolve_consul_url("aerospike-use-ramv.aerospike.service.useast.consul", port=3000, limit=1)}}'
RAM_NAMESPACE: Final = 'ttd-ramv'
METADATA_SET_NAME_PROD: Final = 'metadata'
METADATA_SET_NAME_TEST: Final = 'metadata_t'

ETL_FORECAST_JOB_JAR_PATH_PROD: Final = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
ETL_FORECAST_JOB_JAR_PATH_TEST: Final = "s3://ttd-build-artefacts/etl-based-forecasts/mergerequests/$BRANCH_NAME$/jobs/jars/latest/etl-forecast-jobs.jar"

SPARK_3_EMR_RELEASE_LABEL: Final = AwsEmrVersions.AWS_EMR_SPARK_3_3

FORECAST_MODELS_CHARTER_SLACK_CHANNEL: Final = "#dev-forecast-models-charter-alerts"
