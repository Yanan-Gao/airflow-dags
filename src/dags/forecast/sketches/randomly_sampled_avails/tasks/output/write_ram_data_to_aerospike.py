from datetime import timedelta

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY, \
    LAST_GOOD_ISO_WEEKDAYS_KEY, RAM_GENERATION_ISO_WEEKDAY_KEY, INACTIVE_AEROSPIKE_SET_KEY_AEROSPIKE, RAM_NAMESPACE, \
    AEROSPIKE_HOSTS, AEROSPIKE_USE_BUFFERED_WRITES, AEROSPIKE_TRANSACTION_RATE, \
    AEROSPIKE_MAX_ASYNC_CONNECTIONS_PER_NODE, ETL_BASED_FORECASTS_JAR_PATH, GLOBAL_SPARK_OPTIONS_LIST, \
    GLOBAL_JVM_SETTINGS_LIST
from ttd.eldorado.aws.emr_job_task import EmrJobTask

_NAME = "WriteRamDataToAerospike"
_CLASSNAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.WriteRamDataToAerospike"
_SPARK_ADDITIONAL_OPTIONS = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("executor-memory", "27000M"),
                             ("conf", "spark.driver.memory=27000M"), ("conf", "spark.executor.cores=4"), ("conf", "spark.driver.cores=4"),
                             ("conf", "spark.driver.maxResultSize=3G"), ("conf", "spark.network.timeout=1200s"),
                             ("conf", "spark.yarn.maxAppAttempts=1"), ("conf", "spark.sql.shuffle.partitions=3000")]
_JVM_CONFIG_OPTIONS = [
    ('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}'),
    ('ramGenerationIsoWeekday', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_ISO_WEEKDAY_KEY}") }}}}'),
    ('partitionWeekdaysAndDates', f'{{{{ task_instance.xcom_pull(key="{LAST_GOOD_ISO_WEEKDAYS_KEY}") }}}}'),
    ('aerospikeSet', f'{{{{ task_instance.xcom_pull(key="{INACTIVE_AEROSPIKE_SET_KEY_AEROSPIKE}") }}}}'),
    ('aerospikeNamespace', RAM_NAMESPACE),
    ('aerospikeHosts', AEROSPIKE_HOSTS),
    ('aerospikeUseBufferedWrites', AEROSPIKE_USE_BUFFERED_WRITES),
    # Below are experimentally chosen parameters for optimal BufferedWrites operation
    ('aerospikeTransactionRate', AEROSPIKE_TRANSACTION_RATE),
    ('aerospikeMaxAsyncConnectionsPerNode', AEROSPIKE_MAX_ASYNC_CONNECTIONS_PER_NODE),
]


class WriteRamDataToAerospike(EmrJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASSNAME,
            executable_path=ETL_BASED_FORECASTS_JAR_PATH,
            additional_args_option_pairs_list=[*GLOBAL_SPARK_OPTIONS_LIST, *_SPARK_ADDITIONAL_OPTIONS],
            eldorado_config_option_pairs_list=[*GLOBAL_JVM_SETTINGS_LIST, *_JVM_CONFIG_OPTIONS],
            timeout_timedelta=timedelta(hours=14)
        )
