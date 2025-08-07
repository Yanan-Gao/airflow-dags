from datetime import datetime, timedelta

from airflow import DAG

from dags.forecast.avails_coldstorage_lookup.constants import DAG_NAME
from dags.forecast.avails_coldstorage_lookup.tasks.avails_cs_lookup_emr_cluster_task import AvailsCsLookupEmrClusterTask
from dags.forecast.avails_coldstorage_lookup.tasks.avails_cs_lookup_hdi_cluster_task import \
    AvailsCsIdsLookupHdiClusterTask
from dags.forecast.avails_coldstorage_lookup.tasks.avails_cs_lookup_ids_task import AvailsLookupIdsTask
from dags.forecast.avails_coldstorage_lookup.tasks.avails_cs_lookup_sync_aws import AvailsCsLookupSyncAws
from dags.forecast.avails_coldstorage_lookup.tasks.avails_cs_lookup_sync_azure import AvailsCsLookupSyncAzure
from dags.forecast.avails_coldstorage_lookup.tasks.copy_ids_dataset import CopyIdsDatasetTask
from dags.forecast.avails_coldstorage_lookup.tasks.data_existence_check_task import DataExistenceCheckTask
from dags.forecast.avails_coldstorage_lookup.tasks.initialize_run_hour_task import InitializeRunHourTask
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import FORECAST

_DAG_START_DATE = datetime(2024, 9, 16, 0, 0)
_NUM_OF_HOURS_TO_PROCESS = 4
_DAG_SCHEDULE_INTERVAL = timedelta(hours=_NUM_OF_HOURS_TO_PROCESS)


class AvailsCsLookupDAG:
    """
    Class to encapsulate an Airflow DAG for Avails ColdStorage sync.

    More info: https://atlassian.thetradedesk.com/confluence/display/EN/Universal+Forecasting+Walmart+Data+Sovereignty
    """

    def __init__(self):
        self.create_dag()
        self.create_tasks()
        self.set_dependencies()

    def create_dag(self):
        self.dag = TtdDag(
            dag_id=DAG_NAME,
            start_date=_DAG_START_DATE,
            schedule_interval=_DAG_SCHEDULE_INTERVAL,
            tags=['SampledAvailsColdStorageSync', 'FORECAST'],
            max_active_runs=1,
            slack_channel='#dev-forecasting-alarms',
            slack_tags=FORECAST.team.sub_team,
            default_args={'owner': 'FORECAST'},
            run_only_latest=True
        )

    def create_tasks(self):
        self.initialize_run_hour_task = InitializeRunHourTask(_NUM_OF_HOURS_TO_PROCESS)
        self.emr_cluster_task = AvailsCsLookupEmrClusterTask()
        self.hdi_cluster_task = AvailsCsIdsLookupHdiClusterTask()

        self.data_check_tasks = []
        self.aws_sync_ids_tasks = []
        self.copy_ids_tasks = []
        self.lookup_ids_tasks = []
        self.azure_sync_ids_tasks = []
        for i in range(_NUM_OF_HOURS_TO_PROCESS):
            self.data_check_tasks.append(DataExistenceCheckTask(iteration_number=i))
            self.copy_ids_tasks.append(CopyIdsDatasetTask(iteration_number=i))
            self.lookup_ids_tasks.append(AvailsLookupIdsTask(iteration_number=i))
            self.aws_sync_ids_tasks.append(AvailsCsLookupSyncAws(iteration_number=i))
            self.azure_sync_ids_tasks.append(AvailsCsLookupSyncAzure(iteration_number=i))

    def set_dependencies(self):
        self.lookup_ids_tasks[0] >> self.copy_ids_tasks[0] >> self.azure_sync_ids_tasks[0]
        self.hdi_cluster_task.add_parallel_body_task(self.azure_sync_ids_tasks[0])
        self.emr_cluster_task.add_parallel_body_task(self.lookup_ids_tasks[0])
        for i in range(1, _NUM_OF_HOURS_TO_PROCESS):
            self.data_check_tasks[i - 1] >> self.data_check_tasks[i]
            self.lookup_ids_tasks[i - 1] >> self.lookup_ids_tasks[i]
            self.lookup_ids_tasks[i] >> self.copy_ids_tasks[i]
            self.azure_sync_ids_tasks[i - 1] >> self.azure_sync_ids_tasks[i]
            self.copy_ids_tasks[i] >> self.azure_sync_ids_tasks[i]
            self.hdi_cluster_task.add_parallel_body_task(self.azure_sync_ids_tasks[i])
            self.emr_cluster_task.add_parallel_body_task(self.lookup_ids_tasks[i])

        self.dag >> self.initialize_run_hour_task
        self.initialize_run_hour_task >> self.data_check_tasks[0]
        self.data_check_tasks[_NUM_OF_HOURS_TO_PROCESS - 1] >> self.emr_cluster_task
        self.data_check_tasks[_NUM_OF_HOURS_TO_PROCESS - 1] >> self.hdi_cluster_task


ttd_dag = AvailsCsLookupDAG().dag
dag: DAG = ttd_dag.airflow_dag
