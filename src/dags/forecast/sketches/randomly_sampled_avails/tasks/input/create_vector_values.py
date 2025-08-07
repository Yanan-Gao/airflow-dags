from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import ETL_BASED_FORECASTS_JAR_PATH, \
    GLOBAL_SPARK_OPTIONS_LIST, GLOBAL_JVM_SETTINGS_LIST, RAM_GENERATION_TIMESTAMP_KEY
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.aws.emr_job_task import EmrJobTask

_NAME = "CreateVectorValues"
_CLASS_NAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.CreateVectorValues"
_JVM_ADDITIONAL_SETTINGS = [('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}')]
_SPARK_ADDITIONAL_OPTIONS = [('conf', 'spark.sql.shuffle.partitions=100')]
_TEST_JVM_SETTINGS = [('ttd.ds.AdhocVectorValuesDataSet.prod.s3root', 's3a://ttd-identity/datapipeline/ram/test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class CreateVectorValues(EmrJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASS_NAME,
            executable_path=ETL_BASED_FORECASTS_JAR_PATH,
            additional_args_option_pairs_list=
            [*GLOBAL_SPARK_OPTIONS_LIST, *_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST, *_JVM_ADDITIONAL_SETTINGS, *get_test_or_default_value(_TEST_JVM_SETTINGS, [])]
        )
