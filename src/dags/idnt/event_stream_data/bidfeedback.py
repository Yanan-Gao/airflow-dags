"""
Processes general Bidfeedback and EU Bidfeedback.
"""
from airflow import DAG
from datetime import datetime, timedelta
from dags.idnt.datasets.dataset_adapter import DatasetAdapter
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Tags, Executables, RunTimes, Directories
from dags.idnt.vendors.vendor_datasets import VendorDatasets
from datasources.sources.common_datasources import CommonDatasources
from ttd.datasets.date_dataset import DateDataset
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask
from ttd.tasks.base import BaseTask
from typing import List, Optional

# Get the execution environment and set the output paths accordingly (prod or test)
ttd_env = Tags.environment()
path_change = Directories.test_dir_no_prod()


class BaseBidfeedback():
    """Established the base parts of the bidfeedback pipeline: check that input data is ready then generate the cluster
    to process the bidfeedback.

    Attributes:
        dag (TtdDag): The DAG we should add the pipeline to.
        name (str): Name of the specific bidfeedback
        source_datasets (List[DateDataset]): Datasets that the bidfeedback gen relies on
        input_path_base (str): The input dataset to generate bidfeedback from.
        output_path_base (str): Path for the generated bidfeedback.
        stats_path (str): Output path for stats
        additional_task_configs (dict[str, str]): Additional configs for the ETL task
    """

    def __init__(self, dag: TtdDag):
        self.dag = dag

    name: str
    source_datasets: List[DateDataset]
    input_path_base: str
    output_path_base: str
    stats_path: str
    additional_task_configs: dict[str, str]

    def get_run_cluster(self) -> EmrClusterTask:
        """Generates a cluster for running bidfeedback ETL processing.

        Returns:
            EmrClusterTask: The EMR cluster task with the BidFeedbackEtl task added.
        """
        cluster = IdentityClusters.get_cluster(f'etl-{self.name}', self.dag, 4000, ComputeType.STORAGE, use_delta=False)
        run_time = RunTimes.previous_full_day
        base_configs = dict([('LOCAL', 'false'), ('START_DATE', run_time), ('END_DATE', run_time),
                             ('INPUT_PATH_BASE', self.input_path_base), ('INPUT_PARTITIONS', 60000),
                             ('OUTPUT_PATH_BASE', self.output_path_base), ('STATS_PATH', self.stats_path)])

        cluster.add_sequential_body_task(
            IdentityClusters.task(
                class_name='com.thetradedesk.etl.misc.BidFeedbackEtl',
                eldorado_configs=IdentityClusters.dict_to_configs(base_configs | self.additional_task_configs),
                executable_path=Executables.etl_repo_executable
            )
        )
        return cluster

    def build_pipeline(self, upstream_task: Optional[BaseTask] = None) -> None:
        check_bidfeedback_data = DagHelpers.check_datasets(datasets=self.source_datasets, suffix=self.name)

        if (first_step := upstream_task) is None:
            first_step = self.dag

        first_step >> check_bidfeedback_data >> self.get_run_cluster()


class Bidfeedback(BaseBidfeedback):
    name: str = 'bidfeedback'
    input_datasource: str = CommonDatasources.rtb_bidfeedback_v5
    source_datasets: List[DateDataset] = [input_datasource]
    input_path_base: str = DatasetAdapter(input_datasource).location
    output_path_base: str = f's3://ttd-identity/datapipeline{path_change}/sources/bidfeedback'
    stats_path: str = f's3://ttd-identity/adbrain-stats/etl{path_change}/bidfeedback'
    additional_task_configs: dict[str, str] = dict([('NUM_PARTITIONS', '2000'), ('SAMPLING_LIMIT', '60'),
                                                    ('MAX_DEGREE_OF_PARALLELISM', '1'), ('FORCE_PROCESSING', 'true')])


class BidfeedbackEu(BaseBidfeedback):
    name: str = 'bidfeedback-eu'
    source_datasets: List[DateDataset] = VendorDatasets.get_bidfeedback_eu_raw_data()
    input_path_base: str = 's3://thetradedesk-useast-logs-2/unmaskedandunhashedbidfeedback/collected/'
    output_path_base: str = f's3://ttd-identity/datapipeline{path_change}/sources/bidfeedback_new'
    stats_path: str = f's3://ttd-identity/adbrain-stats/etl{path_change}/bidfeedback_new'
    additional_task_configs: dict[str, str] = dict([
        ('OUTPUT_PATH_BASE_EU_DATA', f's3://ttd-identity/datapipeline{path_change}/sources/bidfeedback_eu_v2'),
        ('FRAUD_PATH_BASE', 's3a://thetradedesk-useast-logs-2/bidfeedback/cleanselineblocked/'), ('READ_NEW_CSV_FORMAT', 'true'),
        ('SPLIT_EU_DATA', 'true'), ('NUM_PARTITIONS', '400')
    ])


# This DAG has been migrated to event-stream-data but remains specified here for Airflow history.
bidfeedback_dag = DagHelpers.identity_dag(
    dag_id='etl-bidfeedback',
    start_date=datetime(2024, 10, 10),
    schedule_interval='15 0 * * *',
    retry_delay=timedelta(minutes=30),
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=4)
)

Bidfeedback(bidfeedback_dag).build_pipeline()

BidfeedbackEu(bidfeedback_dag).build_pipeline()

final_dag: DAG = bidfeedback_dag.airflow_dag
