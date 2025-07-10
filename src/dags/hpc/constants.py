from ttd.hdinsight.script_action_spec import ScriptActionSpec
from ttd.slack.slack_groups import hpc
from ttd.ttdenv import TtdEnvFactory

job_environment = TtdEnvFactory.get_from_system()

# Defaults
DEFAULT_CLUSTER_TAGS = {
    'Team': hpc.jira_team,
}
HPC_AWS_EL_DORADO_JAR_URL = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-hpc-assembly.jar'
HPC_AZURE_EL_DORADO_JAR_URL = 'abfs://ttd-build-artefacts@ttdeldorado.dfs.core.windows.net/eldorado/release-spark-3/main-spark-3/latest/eldorado-hpc-assembly.jar'
HPC_ALI_EL_DORADO_JAR_URL = 'oss://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-hpc-assembly.jar'

# Variables
DMP_ROOT = 'ttd-datamarketplace'

OSS_COUNTS_BUCKET = "ttd-counts"

DEFAULT_AWS_REGION = 'us-east-1'
SQS_DATAIMPORT_INTERMEDIATE_QUEUE = 'sns-s3-useast-dataimport-intermediate-collected-queue'
SQS_DATAIMPORT_INTERMEDIATEAGGREGATED_QUEUE = 'sns-s3-useast-logs-2-dataimportintermediateaggregated-collected-queue'

RECEIVED_COUNTS_DATASET_VERSION = 5
RECEIVED_COUNTS_WRITE_PATH = f"counts/{job_environment.dataset_write_env}/targetingdatareceivedcounts/v={RECEIVED_COUNTS_DATASET_VERSION}/"
RECEIVED_COUNTS_AZURE_WRITE_PATH = f"counts/{job_environment.dataset_write_env}/targetingdatareceivedcountsazure/v={RECEIVED_COUNTS_DATASET_VERSION}/"

RECEIVED_COUNTS_BY_ID_TYPE_DATASET_VERSION = 1
RECEIVED_COUNTS_BY_ID_TYPE_WRITE_PATH = f"counts/prod/targetingdatareceivedcountsbyuiidtype/v={RECEIVED_COUNTS_BY_ID_TYPE_DATASET_VERSION}/"
RECEIVED_COUNTS_BY_ID_TYPE_AZURE_WRITE_PATH = f"counts/prod/targetingdatareceivedcountsbyuiidtypeazure/v={RECEIVED_COUNTS_BY_ID_TYPE_DATASET_VERSION}/"

TARGETING_DATA_STAGING_S3 = "s3://thetradedesk-useast-qubole/warehouse/thetradedesk.db/sib-daily-sketches-targeting-data-ids-staging-hmh/"
TARGETING_DATA_STAGING_AZURE = "wasbs://ttd-datamarketplace@eastusttdlogs.blob.core.windows.net/counts/prod/sib-daily-sketches-targeting-data-ids-staging-hmh/"
TARGETING_DATA_FINAL_S3 = "s3://thetradedesk-useast-qubole/warehouse/thetradedesk.db/sib-daily-sketches-targeting-data-ids/"
TARGETING_DATA_FINAL_AZURE = "wasbs://ttd-datamarketplace@eastusttdlogs.blob.core.windows.net/counts/prod/sib-daily-sketches-targeting-data-ids/"
TTD_COUNTS_AZURE = "wasbs://ttd-counts@ttdexportdata.blob.core.windows.net/counts/"

HOTCACHE_COUNTS_DATASET_VERSION = 1
HOTCACHE_COUNTS_WRITE_PATH = f'wasbs://ttd-datamarketplace@eastusttdlogs.blob.core.windows.net/counts/{job_environment.dataset_write_env}/hotcachecounts/v={HOTCACHE_COUNTS_DATASET_VERSION}'
HOTCACHE_COUNTS_CHINA_WRITE_PATH_PREFIX = f'counts/{job_environment.dataset_write_env}/hotcachecounts/v={HOTCACHE_COUNTS_DATASET_VERSION}'

COLD_STORAGE_SCAN_CLASS_NAME = 'com.thetradedesk.jobs.receivedcounts.coldstoragescan.ColdStorageScan'

# Log Workflow
LOGWORKFLOW_CONNECTION = 'lwdb'
LOGWORKFLOW_SANDBOX_CONNECTION = 'sandbox-lwdb'
LOGWORKFLOW_DB = 'LogWorkflow'

# Aerospike Variables
COLD_STORAGE_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", 4333)}}'
INTERMEDIATE_COLD_STORAGE_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("aerospike-ny3-scs.aerospike.service.ny3.consul", 3000)}}'
CHINA_COLD_STORAGE_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("aerospike-cn-cold.aerospike.service.cn4.consul", 3000, limit=1)}}'
CHINA_COLD_STORAGE_NAMESPACE = 'ttd-coldstorage-cn'

VAD_HC_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-ip.aerospike.service.vad.consul", 3000)}}'
CA2_HC_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-user.aerospike.service.ca2.consul", 3000)}}'
NY1_HC_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-user.aerospike.service.ny1.consul", 3000)}}'
VA6_HC_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-user.aerospike.service.va6.consul", 3000)}}'
CN4_HC_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-user.aerospike.service.cn4.consul", 3000)}}'

COUNTS_AEROSPIKE_NAMESPACE = 'ttd-sgsk-hmh'
AWS_COUNTS_AEROSPIKE_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-sgsk-hmh.aerospike.service.useast.consul", 3000, limit=1)}}'
AZURE_COUNTS_AEROSPIKE_ADDRESS = '{{macros.ttd_extras.resolve_consul_url("ttd-sgsk-hmh.aerospike.service.va9.consul", 4333, limit=1)}}'
COUNTS_AEROSPIKE_TTL = '-1'

# XD variables
XD_GRAPH_LOOKBACK_DAYS = 12
XD_GRAPH_BUCKET = 'thetradedesk-useast-data-import'
XD_GRAPH_PREFIX_IAV2 = 'sxd-etl/universal/iav2graph/'
XD_GRAPH_DATE_KEY_IAV2 = 'xd_graph_date_iav2'

# Pipeline Cadences
RECEIVED_COUNTS_CADENCE_IN_HOURS = 6
TARGETING_DATA_USER_EXTRA_DURATION_IN_HOURS = 3  # TDU runs after the ColdStorageScan job, so goes beyond the 6 hour duration

DOWNLOAD_AEROSPIKE_CERT_AZURE_CLUSTER_SCRIPT_ACTION = [
    ScriptActionSpec(
        action_name="download_public_certs",
        script_uri=
        "https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.0/latest/azure-scripts/download_from_url.sh",
        parameters=[
            "'https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v0-spark-2.4.0/latest/azure-scripts/ttd-internal-root-ca-truststore.jks?{{conn.azure_ttd_build_artefacts_sas.get_password()}}'",
            "/tmp/ttd-internal-root-ca-truststore.jks"
        ]
    )
]

# Cardinality service
CARDINALITY_SERVICE_PROD_HOST = "cardinality-service.gen.adsrvr.org"
CARDINALITY_SERVICE_PROD_PORT = 443
CARDINALITY_SERVICE_TEST_HOST = "cardinality-service-test.gen.adsrvr.org"
CARDINALITY_SERVICE_TEST_PORT = 443

# Granite
# https://gitlab.adsrvr.org/thetradedesk/teams/hpc/granite/-/blob/master/java-client-example/src/main/java/com/thetradedesk/hpc/granite/client/example/Example.java
GRANITE_PROD_HOSTS = "10.124.128.36,10.124.134.36,10.124.164.36"
GRANITE_CANARY_HOSTS = "10.127.144.74,10.127.148.74,10.124.164.34"
