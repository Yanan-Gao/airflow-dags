from typing import List, Tuple

from dags.forecast.sketches.randomly_sampled_avails.constants import GLOBAL_JVM_SETTINGS_LIST_AZURE, \
    RAM_GENERATION_TIMESTAMP_KEY, \
    AZURE_JAR_PATH, GLOBAL_SPARK_OPTIONS_LIST_AZURE
from dags.forecast.sketches.randomly_sampled_avails.utils import get_test_or_default_value
from ttd.eldorado.hdi import HDIJobTask

_NAME = "StoreDataElementPartialAggsToHdfsAzure"
_CLASS_NAME = "com.thetradedesk.etlforecastjobs.universalforecasting.ram.dataelementshmh.StoreDataElementPartialAggsToHdfsAzure"
_SPARK_ADDITIONAL_OPTIONS: List[Tuple[str, str]] = []
_JVM_ADDITIONAL_SETTINGS = [
    ('ramGenerationTimestamp', f'{{{{ task_instance.xcom_pull(key="{RAM_GENERATION_TIMESTAMP_KEY}") }}}}'),
    ('bloomFalsePositive', '0.000001'),
    ("ttd.defaultcloudprovider", "azure"),
]
_TEST_JVM_SETTINGS = [('ttd.env', 'test')]
_TEST_SPARK_OPTIONS: List[Tuple[str, str]] = []


class StoreDataElementPartialAggsToHDFSAzure(HDIJobTask):

    def __init__(self):
        super().__init__(
            name=_NAME,
            class_name=_CLASS_NAME,
            additional_args_option_pairs_list=
            [*GLOBAL_SPARK_OPTIONS_LIST_AZURE, *_SPARK_ADDITIONAL_OPTIONS, *get_test_or_default_value(_TEST_SPARK_OPTIONS, [])],
            eldorado_config_option_pairs_list=
            [*GLOBAL_JVM_SETTINGS_LIST_AZURE, *_JVM_ADDITIONAL_SETTINGS, *get_test_or_default_value(_TEST_JVM_SETTINGS, [])],
            jar_path=AZURE_JAR_PATH,
            configure_cluster_automatically=True
        )
