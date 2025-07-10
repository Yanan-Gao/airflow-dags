from datetime import timedelta
from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY, \
    ETL_BASED_FORECASTS_JAR_PATH, GLOBAL_SPARK_OPTIONS_LIST, GLOBAL_JVM_SETTINGS_LIST
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.aws.emr_job_task import EmrJobTask

_NAME = "CreateContainmentPointerRecords"
_CLASSNAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.containment.CreateContainmentPointerRecords"
_SPARK_ADDITIONAL_OPTIONS = [('conf', 'spark.sql.shuffle.partitions=3000')]
_JVM_CONFIG_OPTIONS = [
    ('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}'),
    ('bloomFalsePositive', '0.000001'),
    ('ttd.azure.enable', "true"),
]
_TEST_JVM_SETTINGS = [('ttd.env', 'test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class CreateContainmentPointerRecords(EmrJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASSNAME,
            executable_path=ETL_BASED_FORECASTS_JAR_PATH,
            additional_args_option_pairs_list=
            [*GLOBAL_SPARK_OPTIONS_LIST, *_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST, *_JVM_CONFIG_OPTIONS, *get_test_or_default_value(_TEST_JVM_SETTINGS, [])],
            timeout_timedelta=timedelta(hours=6)
        )
