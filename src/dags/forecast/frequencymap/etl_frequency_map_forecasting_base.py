from dataclasses import dataclass
from enum import Enum
from typing import Final

from airflow import DAG
from datetime import datetime, timedelta

from dags.forecast.utils.cluster_settings_library import ClusterSettings, InstanceTypeCore, InstanceTypeMaster
from dags.forecast.frequencymap.cluster_setup import get_cluster
from dags.forecast.utils.avails import get_wait_avails_operator
from dags.forecast.aerospike_set_utils import create_get_inactive_set_version_and_lock_task, \
    create_activate_and_unlock_set_version_task
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from dags.forecast.utils.team import TEAM_NAME
from dags.forecast.utils.constants import AEROSPIKE_HOSTS, RAM_NAMESPACE, METADATA_SET_NAME_PROD, \
    METADATA_SET_NAME_TEST, ETL_FORECAST_JOB_JAR_PATH_PROD, ETL_FORECAST_JOB_JAR_PATH_TEST, SPARK_3_EMR_RELEASE_LABEL, \
    FORECAST_MODELS_CHARTER_SLACK_CHANNEL

ttd_env = TtdEnvFactory.get_from_system()

###########################################
#   job settings
###########################################
is_prod = ttd_env == TtdEnvFactory.prod
JOB_NAME_PREFIX: Final = "etl-frequency-map-forecasting-"
AEROSPIKE_TRACK_TABLE_TASK_PREFIX: Final = "update_aerospike_track_table_"
BASE_CLASS_PATH: Final = "com.thetradedesk.etlforecastjobs.universalforecasting.frequencymap."
MAPPING_JOB_CLASS: Final = "generation.MappingGeneratingJob"
FREQUENCY_MAPPING_JOB_CLASS: Final = "generation.FrequencyMapGeneratingJob"
EXPORT_JOB_CLASS = "exporting.FrequencyMapExportingJob"

###########################################
#   Testing/Debug Settings
###########################################
use_test_jar = True
mr_branch_name = "siq-FORECAST-6201-fmap-targeting-data-cleanup"
TEST_DATASET_NAME: Final = "tst_0"

###########################################
#   Configure Aerospike
###########################################
NEXT_SET_KEY_AEROSPIKE: Final = 'next_set_aerospike'
NEXT_SET_BASE_TASK_ID: Final = 'get_next_aerospike_set_'
AEROSPIKE_GEN_KEY: Final = 'aerospike_gen'

###########################################
#   Configure supported types
###########################################
vector = "vector"


@dataclass(frozen=True)
class IdTypeToSet:
    id_type: str
    set_name: str


class ClusterSettingsFactory(Enum):
    id_mapping = ClusterSettings("id-mapping")
    frequency_map = ClusterSettings("frequency-maps")
    validation_export = ClusterSettings(
        "validation-export", 12000, 5 * 96, InstanceTypeMaster.INSTANCE_TYPE_MASTER_FLEET_R5_16X,
        InstanceTypeCore.MEMORY_OPTIMIZED_FLEET_SETTINGS, "195G"
    )


class FrequencyMapDag:

    def __init__(
        self,
        job_start_date: datetime,
        job_schedule_interval: str,
        avail_stream: str,
        id_types_with_production_aerospike_set_roots: list[IdTypeToSet],
        time_ranges: list[int],
        include_people_household_curve: bool = False,
        version: str = "",
        export_enabled: bool = True,
        graph_name: str = None,
    ):
        self.job_start_date = job_start_date
        self.job_schedule_interval = job_schedule_interval
        self.avail_stream = avail_stream
        self.id_types_with_production_aerospike_set_roots = id_types_with_production_aerospike_set_roots
        self.time_ranges = time_ranges
        self.include_people_household_curve = include_people_household_curve
        self.job_name = JOB_NAME_PREFIX + avail_stream + ("" if graph_name is None else "-" + graph_name) + "-" + version
        self.export_enabled = export_enabled
        self.graph_name = graph_name
        self.mapping_type = vector
        self.metadata_set_name = METADATA_SET_NAME_PROD if is_prod else METADATA_SET_NAME_TEST
        self.jar_name = ETL_FORECAST_JOB_JAR_PATH_PROD if (is_prod or not use_test_jar) \
            else ETL_FORECAST_JOB_JAR_PATH_TEST.replace("$BRANCH_NAME$", mr_branch_name)
        self.dag = self.create_dag()

    def create_dag(self) -> DAG:
        forecasting_fmap_dag_pipeline = TtdDag(
            dag_id=self.job_name,
            start_date=self.job_start_date,
            schedule_interval=self.job_schedule_interval,
            retries=1,
            retry_delay=timedelta(minutes=2),
            tags=[TEAM_NAME],
            slack_tags=TEAM_NAME,
            slack_channel=FORECAST_MODELS_CHARTER_SLACK_CHANNEL,
            default_args={"owner": TEAM_NAME},
        )

        # Define all the required functions
        def get_job_aerospike_set_through_xcom(job_aerospike_set_root: str) -> str:
            if job_aerospike_set_root is None:
                return TEST_DATASET_NAME
            xcom_pull_set_number_params = [
                f"dag_id = '{self.job_name}'", f"task_ids = '{NEXT_SET_BASE_TASK_ID + job_aerospike_set_root}'",
                f"key = '{NEXT_SET_KEY_AEROSPIKE}'"
            ]
            xcom_pull_set_number_call = f"task_instance.xcom_pull({', '.join(xcom_pull_set_number_params)})"
            return job_aerospike_set_root + "_{{" + xcom_pull_set_number_call + "}}"

        def get_next_aerospike_set(job_aerospike_set_root: str) -> OpTask:
            return create_get_inactive_set_version_and_lock_task(
                dag=dag,
                task_id=NEXT_SET_BASE_TASK_ID + job_aerospike_set_root,
                aerospike_hosts=AEROSPIKE_HOSTS,
                namespace=RAM_NAMESPACE,
                metadata_set_name=self.metadata_set_name,
                set_key=job_aerospike_set_root,
                inactive_xcom_set_number_key=NEXT_SET_KEY_AEROSPIKE,
                aerospike_gen_xcom_key=AEROSPIKE_GEN_KEY,
                inactive_xcom_set_key=None
            )

        def update_aerospike_track_table(job_aerospike_set_root: str) -> OpTask:
            return create_activate_and_unlock_set_version_task(
                dag=dag,
                task_id=AEROSPIKE_TRACK_TABLE_TASK_PREFIX + job_aerospike_set_root,
                aerospike_hosts=AEROSPIKE_HOSTS,
                inactive_get_task_id=NEXT_SET_BASE_TASK_ID + job_aerospike_set_root,
                job_name=self.job_name,
                namespace=RAM_NAMESPACE,
                metadata_set_name=self.metadata_set_name,
                set_key=job_aerospike_set_root,
                inactive_xcom_set_number_key=NEXT_SET_KEY_AEROSPIKE,
                aerospike_gen_xcom_key=AEROSPIKE_GEN_KEY
            )

        def mapping(id_type: str) -> EmrClusterTask:
            return get_cluster(
                date='{{ ds }}',
                avail_stream=self.avail_stream,
                id_type=id_type,
                mapping_types=self.mapping_type,
                jar_path=self.jar_name,
                emr_release_label=SPARK_3_EMR_RELEASE_LABEL,
                timeout=4,
                class_name=BASE_CLASS_PATH + MAPPING_JOB_CLASS,
                cluster_settings=ClusterSettingsFactory.id_mapping.value,
                additional_config=[('graphName', self.graph_name)]
            )

        def frequency_map(id_type: str) -> EmrClusterTask:
            return get_cluster(
                date='{{ ds }}',
                avail_stream=self.avail_stream,
                id_type=id_type,
                mapping_types=self.mapping_type,
                jar_path=self.jar_name,
                emr_release_label=SPARK_3_EMR_RELEASE_LABEL,
                timeout=8,
                class_name=BASE_CLASS_PATH + FREQUENCY_MAPPING_JOB_CLASS,
                cluster_settings=ClusterSettingsFactory.frequency_map.value,
                additional_config=[('graphName', self.graph_name), ('timeRanges', ','.join(map(str, tuple(self.time_ranges))))]
            )

        def validate_and_export(id_type_set_name: IdTypeToSet) -> EmrClusterTask:
            return get_cluster(
                date='{{ ds }}',
                avail_stream=self.avail_stream,
                id_type=id_type_set_name.id_type,
                timeout=3,
                class_name=BASE_CLASS_PATH + EXPORT_JOB_CLASS,
                jar_path=self.jar_name,
                emr_release_label=SPARK_3_EMR_RELEASE_LABEL,
                cluster_settings=ClusterSettingsFactory.validation_export.value,
                additional_config=[
                    ('fmapValidationOnly', 'false' if self.export_enabled else 'true'), ('fmapAvailStream', self.avail_stream),
                    ('fmapIdType',
                     id_type_set_name.id_type), ('fmapIncludePhc', 'true' if self.include_people_household_curve else 'false'),
                    ('aerospikeHosts', AEROSPIKE_HOSTS), ('aerospikeNamespace', RAM_NAMESPACE),
                    ('aerospikeSet', get_job_aerospike_set_through_xcom(id_type_set_name.set_name) if is_prod else TEST_DATASET_NAME),
                    ('graphName', self.graph_name), ('fmapTimeRanges', ','.join(map(str, tuple(self.time_ranges))))
                ]
            )

        # Dag Graph Creation
        dag = forecasting_fmap_dag_pipeline.airflow_dag
        wait_avails_ready = get_wait_avails_operator(self.avail_stream, dag, self.graph_name)
        forecasting_fmap_dag_pipeline >> wait_avails_ready

        for idt in self.id_types_with_production_aerospike_set_roots:
            id_mapping_task = mapping(idt.id_type)
            fmap_task = frequency_map(idt.id_type)
            next_set_task = get_next_aerospike_set(idt.set_name)
            validate_export_task = validate_and_export(idt)
            update_set_task = update_aerospike_track_table(idt.set_name)
            wait_avails_ready >> id_mapping_task >> fmap_task >> next_set_task >> validate_export_task >> update_set_task
        return dag
