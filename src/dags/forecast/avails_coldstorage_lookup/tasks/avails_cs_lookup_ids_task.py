from datetime import timedelta

from dags.forecast.avails_coldstorage_lookup.constants import SHORT_DAG_NAME, DAG_NAME
from dags.forecast.avails_coldstorage_lookup.utils import xcom_pull_from_template
from datasources.datasources import Datasources
from ttd.ec2.cluster_params import calc_cluster_params
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.emr import EmrJobTask

_CLASS_NAME = 'com.thetradedesk.etlforecastjobs.coldstorage.AvailsColdStorageLookupSyncIds'
_CLUSTER_PARAMS = calc_cluster_params(instances=10, vcores=R5.r5_8xlarge().cores, memory=R5.r5_8xlarge().memory, parallelism_factor=4)
_EXECUTABLE_PATH = 's3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar'
# Spark parameters docs: https://spark.apache.org/docs/latest/configuration.html#runtime-environment
_SPARK_OPTION_LIST = [
    ('executor-memory', f'{_CLUSTER_PARAMS.executor_memory_with_unit}'),
    ('executor-cores', f'{_CLUSTER_PARAMS.executor_cores}'),
    ('conf', f'num-executors={_CLUSTER_PARAMS.executor_instances}'),
    ('conf', f'spark.executor.memoryOverhead={_CLUSTER_PARAMS.executor_memory_overhead_with_unit}'),
    ('conf', f'spark.driver.memoryOverhead={_CLUSTER_PARAMS.executor_memory_overhead_with_unit}'),
    ('conf', f'spark.driver.memory={_CLUSTER_PARAMS.executor_memory_with_unit}'),
    ('conf', f'spark.default.parallelism={_CLUSTER_PARAMS.parallelism}'),
    ('conf', f'spark.sql.shuffle.partitions={_CLUSTER_PARAMS.parallelism}'),
    ('conf', 'spark.speculation=false'),
    ('conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer'),
    ('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC'),
    ('conf', 'spark.sql.files.ignoreCorruptFiles=true'),
]
_JAVA_OPTION_LIST = [
    ('availsLocation', Datasources.coldstorage.avails_sampled.get_dataset_path()),
]


class AvailsLookupIdsTask(EmrJobTask):
    """
    Class to encapsulate the creation of the EmrJobTask that will read from AWS and write TDIDs in AWS.
    """

    def __init__(self, iteration_number):
        super().__init__(
            name=SHORT_DAG_NAME + f'_{iteration_number}_ids_sync_step_aws',
            executable_path=_EXECUTABLE_PATH,
            class_name=_CLASS_NAME,
            configure_cluster_automatically=False,
            additional_args_option_pairs_list=_SPARK_OPTION_LIST,
            eldorado_config_option_pairs_list=self._build_eldorado_config_option_pairs_list(DAG_NAME, iteration_number),
            timeout_timedelta=timedelta(hours=3),
            action_on_failure='CONTINUE'
        )

    @staticmethod
    def _build_eldorado_config_option_pairs_list(dag_name, iteration_number=None):
        task_id = 'initialize_run_hour'
        return [('hour', xcom_pull_from_template(dag_name, task_id, iteration_number))] + _JAVA_OPTION_LIST
