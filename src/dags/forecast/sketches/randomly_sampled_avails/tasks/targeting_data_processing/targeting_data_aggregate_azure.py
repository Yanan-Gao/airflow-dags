from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY, \
    GLOBAL_JVM_SETTINGS_LIST, AZURE_JAR_PATH, GLOBAL_SPARK_OPTIONS_LIST_AZURE
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.hdi import HDIJobTask

_NAME = "TargetingDataAggregateAzure"
_CLASSNAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.dataelementshmh.TargetingDataAggregateAzure"
_SPARK_ADDITIONAL_OPTIONS = [("yarn.nodemanager.resource.memory-mb", "128000"), ("yarn.nodemanager.resource.cpu-vcores", "32"),
                             ("spark.executor.instances", 48), ("spark.executor.memory", "96000m"), ("spark.executor.cores", 28),
                             ("spark.executor.memoryOverhead", "4000m"), ("spark.driver.memory", "96000m"), ("spark.driver.cores", 28),
                             ("spark.driver.memoryOverhead", "4000m"), ("spark.driver.maxResultSize", "6G"), ("spark.task.cpus", "28"),
                             ("spark.default.parallelism", 9000), ("spark.sql.shuffle.partitions", 9000),
                             ("spark.network.timeout", "1200s"), ("spark.shuffle.registration.timeout", 90000),
                             ("spark.speculation", "false"), ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
                             ("spark.executor.extraJavaOptions", "-server -XX:+UseParallelGC"),
                             ("spark.sql.files.ignoreCorruptFiles", "false")]
_JVM_CONFIG_OPTIONS = [
    ('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}'),
    ("dataElementsHmhCoalescePartitions", "9000"),
    ("ttd.defaultcloudprovider", "azure"),
]
_TEST_JVM_SETTINGS = [('ttd.env', 'test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class TargetingDataAggregateAzure(HDIJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASSNAME,
            jar_path=AZURE_JAR_PATH,
            additional_args_option_pairs_list=
            [*GLOBAL_SPARK_OPTIONS_LIST_AZURE, *_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST, *_JVM_CONFIG_OPTIONS, *get_test_or_default_value(_TEST_JVM_SETTINGS, [])],
            configure_cluster_automatically=False
        )
