from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import ETL_BASED_FORECASTS_JAR_PATH, \
    GLOBAL_SPARK_OPTIONS_LIST, GLOBAL_JVM_SETTINGS_LIST, HMH_P, RAM_GENERATION_TIMESTAMP_KEY, HMH_R
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.aws.emr_job_task import EmrJobTask

_NAME = "BidFeedbackVectorsHMH"
_CLASSNAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.BidFeedbackVectorsHyperMinHash"
_SPARK_ADDITIONAL_OPTIONS = [('conf', 'spark.sql.shuffle.partitions=6000')]
_JVM_CONFIG_OPTIONS = [('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}'),
                       ('hmhP', str(HMH_P)), ('hmhR', str(HMH_R))]
_TEST_JVM_SETTINGS = [('ttd.ds.AdhocMappedBidFeedbackWithHmhDataSet.prod.s3root', 's3a://ttd-identity/datapipeline/ram/test'),
                      ('ttd.ds.AdhocBidFeedbackVectorValuesDataSet.prod.s3root', 's3a://ttd-identity/datapipeline/ram/test'),
                      ('ttd.ds.RamDailyVectorsDataSet.prod.s3root', 's3a://ttd-identity/datapipeline/ram/test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class BidFeedbackVectorsHMH(EmrJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASSNAME,
            executable_path=ETL_BASED_FORECASTS_JAR_PATH,
            additional_args_option_pairs_list=
            [*GLOBAL_SPARK_OPTIONS_LIST, *_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST, *_JVM_CONFIG_OPTIONS, *get_test_or_default_value(_TEST_JVM_SETTINGS, [])]
        )
