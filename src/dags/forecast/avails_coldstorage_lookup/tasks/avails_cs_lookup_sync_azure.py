import math

from dags.forecast.avails_coldstorage_lookup.constants import SHORT_DAG_NAME, DAG_NAME
from dags.forecast.avails_coldstorage_lookup.utils import xcom_pull_from_template
from datasources.datasources import Datasources
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.ec2.cluster_params import calc_cluster_params

from ttd.el_dorado.v2.hdi import HDIJobTask


def modify_s3_link(link: str) -> str:
    # Check if the link starts with 's3a://'
    if link.startswith('s3a://'):
        # Replace 's3a://' with 's3://'
        return link.replace('s3a://', 's3://', 1)
    # If the link does not start with 's3a://', return the original link
    return link


_QUOBLE_ROOT = 'wasbs://thetradedesk-useast-qubole@eastusttdlogs.blob.core.windows.net/warehouse.external/thetradedesk.db/ttd'
_EXECUTABLE_PATH = 'abfs://ttd-build-artefacts@ttdartefacts.blob.core.windows.net/etl-based-forecasts/master/latest/jars/etl-forecast-jobs.jar'  # spark3: azure etlbased jar

_CLUSTER_PARAMS = calc_cluster_params(
    instances=10, vcores=HDIInstanceTypes.Standard_D32A_v4().cores, memory=HDIInstanceTypes.Standard_D32A_v4().memory, parallelism_factor=4
)
_MEMORY_TOLERANCE = 0.9985
_MACHINE_TYPE = HDIInstanceTypes.Standard_D32A_v4()
_INSTANCES = 10
_USABLE_MEMORY_PER_INSTANCE = math.floor((_MACHINE_TYPE.memory - 8) * _MEMORY_TOLERANCE
                                         )  # 119GB -- Saving 8GB for the OS and some tolerance
_TOTAL_MEMORY = _INSTANCES * _USABLE_MEMORY_PER_INSTANCE  # in GB
_TOTAL_CORES = _INSTANCES * _MACHINE_TYPE.cores

_EXECUTOR_CORES = 5
_EXECUTOR_MEMORY = 30  # GB
_NUM_EXECUTORS = _TOTAL_MEMORY // _EXECUTOR_MEMORY

_AEROSPIKE_HOST = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", limit=1)}}'  # ColdStorage

_JAVA_OPTIONS_LIST_SYNC_DATA_AZURE = [
    ('outputLocation', modify_s3_link(Datasources.coldstorage.avails_targetingdata.get_root_path())),
    ('ttdGraphLocation', modify_s3_link(Datasources.common.ttd_graph.get_root_path())),
    ('aerospikeHostName', _AEROSPIKE_HOST),
    ('aerospikePort', 4333),
    ('globalQpsLimit', 100_000),
    ('threadPoolSize', 32),
    ('lookupPartitionNum', _EXECUTOR_CORES * _NUM_EXECUTORS),
    ('bucketSizeSec', 10),
    ('maxResultQueueSize', 1024),
    ('cacheType', 2),
    ('cacheLookbackInHour', 24),
    ('cacheLocation', _QUOBLE_ROOT + '/coldstoragelookupcache'),
    ('xDVendorIds', '10,11'),
    ('cloudServiceId', '4'),
    ('ttd.azure.enable', 'true'),
    ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'),
    ('ttd.defaultcloudprovider', 'azure'),
    # ('ttd.env', 'test')
]
# Spark parameters docs: https://spark.apache.org/docs/latest/configuration.html#runtime-environment
_SPARK_OPTIONS_LIST = [
    # ('num-executors', _NUM_EXECUTORS),
    ('spark.executor.cores', _EXECUTOR_CORES),
    ('spark.executor.memory', f'{_EXECUTOR_MEMORY * 1000}m'),
    ('spark.executor.memoryOverhead', f'{_EXECUTOR_MEMORY // 10 * 1000}m'),
    ('spark.driver.memoryOverhead', '4044m'),
    ('spark.driver.memory', '16405m'),
    ('spark.default.parallelism', _EXECUTOR_CORES * _NUM_EXECUTORS),
    ('spark.sql.shuffle.partitions', _EXECUTOR_CORES * _NUM_EXECUTORS),
    ('spark.speculation', 'false'),
    ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'),
    ('spark.sql.files.ignoreCorruptFiles', 'true'),
    ("yarn.nodemanager.resource.memory-mb", "64000"),
    ("yarn.nodemanager.resource.cpu-vcores", 10)
]

_CLASS_NAME = 'com.thetradedesk.etlforecastjobs.coldstorage.AvailsColdStorageLookupSyncDataAzure'


class AvailsCsLookupSyncAzure(HDIJobTask):
    """
    Class to encapsulate HDIJobTask creation for Avails ColdStorage sync.

    More info: https://atlassian.thetradedesk.com/confluence/display/EN/Universal+Forecasting+Walmart+Data+Sovereignty
    """

    def __init__(self, iteration_number):

        super().__init__(
            name=SHORT_DAG_NAME + f'_{iteration_number}_cs_sync_step_azure',
            jar_path=_EXECUTABLE_PATH,
            class_name=_CLASS_NAME,
            configure_cluster_automatically=True,
            additional_args_option_pairs_list=_SPARK_OPTIONS_LIST + [
                (
                    'spark.executor.extraJavaOptions',
                    '-Djavax.net.ssl.trustStorePassword={{ conn.aerospike_truststore_password.get_password() }} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks -server -XX:+UseParallelGC'
                ),
            ],
            eldorado_config_option_pairs_list=self._build_eldorado_config_option_pairs_list(DAG_NAME, iteration_number),
        )

    @staticmethod
    def _build_eldorado_config_option_pairs_list(dag_name, iteration_number=None):
        task_id = 'initialize_run_hour'
        return [('hour', xcom_pull_from_template(dag_name, task_id, iteration_number))] + _JAVA_OPTIONS_LIST_SYNC_DATA_AZURE + [
            ('javax.net.ssl.trustStorePassword', "{{ conn.aerospike_truststore_password.get_password() }}")
        ]
