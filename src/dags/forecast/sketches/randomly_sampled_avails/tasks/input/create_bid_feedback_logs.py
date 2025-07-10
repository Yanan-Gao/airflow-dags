from datetime import timedelta
from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import RAM_GENERATION_TIMESTAMP_KEY, \
    ETL_BASED_FORECASTS_JAR_PATH, GLOBAL_JVM_SETTINGS_LIST
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.aws.emr_job_task import EmrJobTask

_NAME = "CreateBidFeedbackLogs"
_CLASS_NAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.CreateBidFeedbackLogs"
_SPARK_ADDITIONAL_OPTIONS = [("executor-memory", "100000M"), ("executor-cores", "5"),
                             ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=80000M"),
                             ("conf", "spark.driver.cores=16"), ("conf", "spark.driver.maxResultSize=12G"),
                             ("conf", "spark.network.timeout=300"), ("conf", "spark.shuffle.registration.timeout=90000"),
                             ('conf', 'spark.sql.shuffle.partitions=6000')]

_JVM_ADDITIONAL_SETTINGS = [('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}')]
_TEST_JVM_SETTINGS = [('ttd.ds.AdhocVectorValuesDataSet.prod.s3root', 's3a://ttd-identity/datapipeline/ram/test'),
                      ('ttd.ds.AdhocBidFeedbackLogsDataSet.prod.s3root', 's3a://ttd-identity/datapipeline/ram/test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class CreateBidFeedbackLogs(EmrJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASS_NAME,
            executable_path=ETL_BASED_FORECASTS_JAR_PATH,
            # Temporarily increase timeout of the step, see https://thetradedesk.slack.com/archives/CRUTE7UAE/p1675758944328559
            # & FORECAST-2219 for more context
            timeout_timedelta=timedelta(hours=8),
            additional_args_option_pairs_list=[*_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST, *_JVM_ADDITIONAL_SETTINGS, *get_test_or_default_value(_TEST_JVM_SETTINGS, [])]
        )
