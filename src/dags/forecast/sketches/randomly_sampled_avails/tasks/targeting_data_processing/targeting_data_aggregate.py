from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY, \
    ETL_BASED_FORECASTS_JAR_PATH, \
    GLOBAL_SPARK_OPTIONS_LIST, GLOBAL_JVM_SETTINGS_LIST
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.aws.emr_job_task import EmrJobTask

_NAME = "TargetingDataAggregate"
_CLASSNAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.dataelementshmh.TargetingDataAggregate"
_SPARK_ADDITIONAL_OPTIONS = [("executor-memory", "115000M"), ("executor-cores", "15"),
                             ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                             ("conf", "spark.driver.memory=115000M"), ("conf", "spark.driver.cores=15"), ("conf", "spark.task.cpus=15"),
                             ("conf", "spark.driver.maxResultSize=6G"), ("conf", "spark.network.timeout=1200s"),
                             ("conf", "spark.shuffle.registration.timeout=90000"), ('conf', 'spark.sql.shuffle.partitions=3000')]
_JVM_CONFIG_OPTIONS = [('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}'),
                       ('dataElementsHmhCoalescePartitions', '2000'), ('numSlices', 2)]
_TEST_JVM_SETTINGS = [('ttd.env', 'test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class TargetingDataAggregate(EmrJobTask):

    def __init__(self, slice_idx):
        super().__init__(
            name=_NAME + f'_{str(slice_idx)}',
            class_name=_CLASSNAME,
            executable_path=ETL_BASED_FORECASTS_JAR_PATH,
            additional_args_option_pairs_list=
            [*GLOBAL_SPARK_OPTIONS_LIST, *_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST, *_JVM_CONFIG_OPTIONS, ('sliceIdx', slice_idx), *get_test_or_default_value(_TEST_JVM_SETTINGS, [])]
        )
