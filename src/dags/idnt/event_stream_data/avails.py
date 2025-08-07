"""
Processes avails v2 and v3 on an hourly basis, then consolidates all hourly avails into daily aggregates.

Runs 6 hours of processing every 6 hours, processing hours from 6 to 12 hours ago.
Once the last hours of the previous day are processed, kicks off a daily agg process.

E.g:
06:00 (data interval end) run processes [18, 24) for last day and daily agg last day
12:00 (data interval end) run processes [0, 6) for current day
18:00 (data interval end) run processes [6, 12) for current day
24:00 (data interval end) run processes [12, 18) for current day

data interval end is the time of processing.
"""
from airflow import DAG
from abc import abstractmethod
from datetime import datetime
from typing import List, Any, Tuple, Optional

from airflow.operators.python import BranchPythonOperator

from dags.idnt.identity_clusters import ComputeType, IdentityClusters
from dags.idnt.identity_clusters_aliyun import IdentityClustersAliyun
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables, RunTimes, Tags
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.datasets.date_dataset import DateDataset
from ttd.datasets.hour_dataset import HourDataset
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.tasks.op import OpTask
from ttd.tasks.chain import ChainOfTasks
from ttd.spark import ParallelTasks
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.ttdenv import TtdEnvFactory
from ttd.cloud_provider import CloudProvider, CloudProviders


class BaseAvails:
    """
    Outlines what information we need for the avails processing since it is the same for both v2 and v3.
    Note: Once we deprecate avails v2, we won't need this class hierarchy.

    Attributes:
        name (str): Name of the avails.
        source_dataset (HourDataset): The datasets that the hourly runs depend on.
        hourly_datasets (List[DateDataset]): The datasets that the daily aggregate depends on.
        dag (TtdDag): The DAG to add the avails tasks to.
        hour_group_size (int): Determines the number of hours processed per cluster.
    """

    name: str
    dataset_cloud_provider: CloudProvider = CloudProviders.aws
    source_dataset: HourDataset
    hourly_datasets: List[DateDataset]

    ttd_env = Tags.environment()
    test_folder = "prod" if str(ttd_env) == "prod" else "test"
    test_folder_without_prod = "" if str(ttd_env) == "prod" else "/test"

    cluster_factory: Any = IdentityClusters

    def __init__(self, dag: TtdDag, hour_group_size: int):
        self.dag = dag
        self.hour_group_size = hour_group_size

    @abstractmethod
    def get_daily_cluster(self) -> EmrClusterTask | AliCloudClusterTask:
        """
        Get the cluster for the daily aggregation.

        Returns:
            EmrClusterTask: The EMR cluster task with the appropriate jobs already added.
        """
        pass

    @abstractmethod
    def get_intraday_cluster(self, start_hour: int, end_hour: int) -> EmrClusterTask | AliCloudClusterTask:
        """
        Get the cluster that will be used for a group of hourly aggregations.
        For each range outlined above, we create a cluster that will run each hour's aggregation.

        Args:
            start_hour (int): The first hour in the range that this cluster is responsible.
            end_hour (int): The last hour in the range.

        Returns:
            EmrClusterTask: The EMR cluster task does not have the jobs added yet.
        """
        pass

    @abstractmethod
    def get_hourly_task(self, date: str, hour: int) -> EmrJobTask:
        """
        Get hourly task for the specified date and hour.

        Args:
            input_run_date (str): Specified only if it differs from date.

        Returns:
            EmrClusterTask: The EMR cluster task does not have the jobs added yet.
        """
        pass

    def get_hourly_and_daily_aggs(self) -> OpTask:
        """
            Function that structures all the hourly and daily aggregation builds.

            Returns:
                OpTask: The final daily aggregation cluster.
        """
        # All avails hourly processes will check the upstream data source for each hour.
        # The daily aggregation will also check each aggregated hour before beginning to process that data.
        data_checks = []

        for hour_lag in range(0, self.hour_group_size):
            full_lookback = self.hour_group_size + hour_lag + 1
            check_input_avails = DagHelpers.check_datasets(
                datasets=[self.source_dataset.with_check_type("hour")],
                ds_date=RunTimes.current_interval_x_hour_ago(x=full_lookback, format='%Y-%m-%d %H:00:00'),
                timeout_hours=6,
                suffix=f"_lag{full_lookback:>02}",
                cloud_provider=self.dataset_cloud_provider
            )
            data_checks.append(check_input_avails)

        check_input_avails = ChainOfTasks(
            task_id=f"{self.name}_check_lagging_datasets",
            tasks=data_checks,
        ).as_taskgroup(f"{self.name}_data_checks")

        check_hourly_agg = DagHelpers.check_datasets(
            datasets=self.hourly_datasets, timeout_hours=5, cloud_provider=self.dataset_cloud_provider
        )

        daily_cluster = self.get_daily_cluster()

        intraday_clusters = []

        for cluster_num in range(0, 24 // self.hour_group_size):
            # using python convention of being non-inclusive end range
            # cluster0 (at 0am data_interval_start, 6am processing) -> [18, 24)
            # cluster1 (at 6am data_interval_start, 12am processing) -> [0, 6)
            # this -1 offsets the processing so that we use a simple lambda h: h // hour_group_size in branching
            # we want cluster 0 to process be aligned to start interval 0am, which means we finish up yesterday's data, hence -1
            start_hour = ((cluster_num - 1) * self.hour_group_size) % 24
            end_hour = start_hour + self.hour_group_size

            intraday_cluster = self.get_intraday_cluster(start_hour, end_hour)

            # if the processing time is on a new day then we're processing last day's data
            # cases:
            # last_hour  6 -> dagrun at 12:00
            # last_hour 12 -> dagrun at 18:00
            # last_hour 18 -> dagrun at 00:00 (24) (next day)
            # last_hour 24 -> dagrun at 06:00 (30) (next day)
            dag_run_hour = end_hour + self.hour_group_size
            if dag_run_hour >= 24:
                runDate = RunTimes.previous_full_day_raw
            else:
                runDate = RunTimes.current_full_day_raw

            for hour in range(start_hour, end_hour):
                intraday_cluster.add_parallel_body_task(self.get_hourly_task(runDate, hour))

            self.cluster_factory.set_step_concurrency(intraday_cluster)

            # if we're processing last hours then check all hours are there and do daily agg
            if end_hour == 24:
                intraday_cluster >> check_hourly_agg >> daily_cluster

            intraday_clusters.append(intraday_cluster)

        intraday_branch = OpTask(
            op=BranchPythonOperator(
                task_id=f"pick_{self.name}_intraday_cluster",
                python_callable=DagHelpers.branch_on_multiple_hour,
                # keep in mind that current_interval_start is 0am for dag executed at 4am,
                # so this will evaluate to first cluster, meaning hours [20, 23)
                op_kwargs=dict(
                    ds=RunTimes.current_interval_start,
                    hour_to_index=lambda h: h // self.hour_group_size,
                    task_map=dict(enumerate(intraday_clusters))
                ),
            )
        )

        intraday_clusters_task = ParallelTasks(f"{self.name}_intraday_clusters", intraday_clusters).as_taskgroup(f"{self.name}_intraday")

        self.dag >> check_input_avails >> intraday_branch >> intraday_clusters_task

        return daily_cluster


class AvailsV2(BaseAvails):
    name: str = "availsv2"
    source_dataset: DateDataset = AvailsDatasources.proto_avails
    hourly_datasets: List[DateDataset] = [IdentityDatasets.identity_avails_v2, IdentityDatasets.identity_avails_v2_idless]

    output_ided_path_base = f's3://ttd-identity/datapipeline{BaseAvails.test_folder_without_prod}/sources/avails-idnt-v2'
    output_idless_path_base = f's3://ttd-identity/datapipeline{BaseAvails.test_folder_without_prod}/sources/avails-idnt-idless-v2'
    stat_path_base = f's3://ttd-identity/adbrain-stats{BaseAvails.test_folder_without_prod}/etl/avails-idnt-v2'

    def get_daily_cluster(self) -> EmrClusterTask:
        daily_cluster = self.cluster_factory.get_cluster("identityAvailsV2_DailyCluster", self.dag, 3500, ComputeType.STORAGE)
        daily_cluster.add_sequential_body_task(
            self.cluster_factory.task("jobs.ingestion.availsdailyagg.AvailsDailyAgg", runDate_arg="Date", timeout_hours=6)
        )
        daily_cluster.add_sequential_body_task(
            self.cluster_factory.task("jobs.ingestion.availsdailyagg.AggToIdMapping", runDate_arg="Date")
        )
        return daily_cluster

    def get_intraday_cluster(self, start_hour: int, end_hour: int) -> EmrClusterTask:
        cluster_name = f"identityAvailsV2_IntradayCluster_{start_hour:>02}_{end_hour:>02}"
        return self.cluster_factory.get_cluster(
            cluster_name,
            self.dag,
            10000,
            ComputeType.MEMORY,
            use_delta=False,
            parallelism_multiplier=1.0,
            emr_release_label=Executables.emr_version_6
        )

    def get_hourly_task(self, date: str, hour: int) -> EmrJobTask:
        date_in_input_format = f"""{{{{ macros.datetime.strptime({date}, '%Y-%m-%d').strftime('%Y/%m/%d') }}}}"""
        input_hour = f"{date_in_input_format}/{hour:>02}"
        date_formatted = f"""{{{{ {date} }}}}"""
        output_hour = f"{date_formatted}/{hour:>02}"
        return self.cluster_factory.task(
            task_name_suffix=f"_hour_{hour}",
            class_name="com.thetradedesk.identity.avails.etl.AvailsEtl",
            executable_path=Executables.avails_etl_v2_executable,
            runDate_arg="DATE",  # Not used
            extra_args=[
                # driver
                ('conf', 'spark.driver.cores=8'),
                ('conf', 'spark.driver.memory=80g'),
                ('conf', 'spark.driver.memoryOverhead=2048'),
                # executor
                ('conf', 'spark.executor.cores=7'),
                ('conf', 'spark.executor.memory=64g'),
                ('conf', 'spark.executor.memoryOverhead=8192'),
                ('conf', 'spark.sql.shuffle.partitions=6300'),
                # ('conf', 'spark.default.parallelism=6300'),
                ('conf', 'spark.dynamicAllocation.enabled=false'),
                ('conf', 'spark.executor.instances=450'),
            ],
            eldorado_configs=[
                ('LOCAL', 'false'),
                ('INPUT_PATH', f's3://thetradedesk-useast-partners-avails/tapad/{input_hour}'),
                ('OUTPUT_IDED_PATH', f'{AvailsV2.output_ided_path_base}/{output_hour}'),
                ('OUTPUT_IDLESS_PATH', f'{AvailsV2.output_idless_path_base}/{output_hour}'),
                ('STATS_PATH', f'{AvailsV2.stat_path_base}/{output_hour}'),
            ],
            action_on_failure="CONTINUE",
            timeout_hours=8
        )


class BaseAvailsV3(BaseAvails):
    name: str = "availsv3"
    executable_path: str
    spark_extra_args: Optional[List[Tuple[str, str]]] = None

    # AIFun's version of unsampled avails hourly agg ready
    source_dataset: HourDataset
    hourly_datasets: List[DateDataset]

    hourly_agg_avails_source_path: str
    base_output_path: str
    temp_output_path: str
    stats_output_path: str
    ip_hashing_enabled: str
    maxmind_base_path: str
    override_location_tables_path: str
    supply_vendor_id_white_list: str
    useExtraIdColumns: str

    daily_cluster_cores: int
    intraday_cluster_cores: int

    def get_daily_cluster(self) -> EmrClusterTask | AliCloudClusterTask:
        daily_cluster = self.cluster_factory.get_cluster(
            "identityAvailsV3_DailyCluster", self.dag, self.daily_cluster_cores, ComputeType.MEMORY, use_delta=False
        )
        daily_cluster.add_sequential_body_task(
            self.cluster_factory.task(
                class_name="jobs.identity.avails.v3.availspipeline.AvailsPipelineDailyAgg",
                timeout_hours=3,
                eldorado_configs=self.get_eldorado_configs(),
                executable_path=self.executable_path,
                extra_args=self.spark_extra_args,
            )
        )
        return daily_cluster

    def get_intraday_cluster(self, start_hour: int, end_hour: int) -> EmrClusterTask | AliCloudClusterTask:
        cluster_name = f"identityAvailsV3_IntradayCluster_{start_hour:>02}_{end_hour:>02}"
        return self.cluster_factory.get_cluster(cluster_name, self.dag, self.intraday_cluster_cores, ComputeType.MEMORY)

    def get_hourly_task(self, date: str, hour: int) -> EmrJobTask | AliCloudJobTask:
        date_formatted = f"""{{{{ {date} }}}}"""
        hour_to_process = f"date={date_formatted}/hour={hour:>02}"
        return self.cluster_factory.task(
            class_name="jobs.identity.avails.v3.availspipeline.AvailsPipelineHourlyAgg",
            timeout_hours=6,
            runDate=date_formatted,
            eldorado_configs=self.get_eldorado_configs() + [('jobs.identity.avails.v3.availspipeline.runDateHour', hour_to_process)],
            action_on_failure="CONTINUE",
            task_name_suffix=f"hour_{hour:>02}",
            executable_path=self.executable_path,
            extra_args=self.spark_extra_args,
        )

    def get_eldorado_configs(self) -> List[Tuple[str, str]]:
        return [
            ('jobs.identity.avails.v3.availspipeline.hourlyAggAvailsSourcePath', self.hourly_agg_avails_source_path),
            ('jobs.identity.avails.v3.availspipeline.baseOutputPath', self.base_output_path),
            ('jobs.identity.avails.v3.availspipeline.tempOutputPath', self.temp_output_path),
            ('jobs.identity.avails.v3.availspipeline.statsOutputPath', self.stats_output_path),
            ('jobs.identity.avails.v3.availspipeline.ipHashing.enabled', self.ip_hashing_enabled),
            ('jobs.identity.avails.v3.availspipeline.maxmindBasePath', self.maxmind_base_path),
            ('jobs.identity.avails.v3.availspipeline.overrideLocationTablesPath', self.override_location_tables_path),
            ('jobs.identity.avails.v3.availspipeline.supplyVendorIdWhiteList', self.supply_vendor_id_white_list),
            ('jobs.identity.avails.v3.availspipeline.useExtraIdColumns', self.useExtraIdColumns),
        ]


identity_executable_path = Executables.identity_repo_executable
identity_executable_path_aliyun = Executables.identity_repo_executable_aliyun

test_start_date = datetime(2025, 5, 26, 0, 3)
test_end_date = datetime(2025, 5, 26, 0, 3)


class AvailsV3(BaseAvailsV3):
    executable_path = identity_executable_path

    source_dataset: HourDataset = AvailsDatasources.identity_agg_hourly_dataset
    hourly_datasets: List[DateDataset] = [IdentityDatasets.avails_v3_hourly_agg]

    hourly_agg_avails_source_path = "s3://thetradedesk-useast-avails/datasets/withPII/prod/identity-avails-agg-hourly-v2-delta"
    base_output_path = f"s3://ttd-identity/data/{BaseAvails.test_folder}/events/avails-pipeline-daily-agg"
    temp_output_path = f"s3://ttd-identity/datapipeline/{BaseAvails.test_folder}/availspipeline/avails-pipeline-daily-agg"
    stats_output_path = f"s3://ttd-identity/adbrain-stats{BaseAvails.test_folder_without_prod}/avails-pipeline-daily-agg"
    ip_hashing_enabled = "true"
    maxmind_base_path = "s3://ttd-identity/external/maxmind"
    override_location_tables_path = ""
    supply_vendor_id_white_list = ""
    useExtraIdColumns = "false"

    daily_cluster_cores: int = 3100
    intraday_cluster_cores: int = 10000


class AvailsV3China(BaseAvailsV3):
    executable_path = identity_executable_path_aliyun

    dataset_cloud_provider: CloudProvider = CloudProviders.ali
    source_dataset: HourDataset = AvailsDatasources.identity_agg_hourly_dataset.with_region("Ali_China_East_2"
                                                                                            ).with_cloud(CloudProviders.ali)
    hourly_datasets: List[DateDataset] = [IdentityDatasets.avails_v3_hourly_agg.with_cloud(CloudProviders.ali)]

    hourly_agg_avails_source_path = "oss://thetradedesk-cn-avails/datasets/withPII/prod/identity-avails-agg-hourly-v2-delta"
    base_output_path = f"oss://ttd-identity/data/{BaseAvails.test_folder}/events/avails-pipeline-daily-agg"
    temp_output_path = f"oss://ttd-identity/datapipeline/{BaseAvails.test_folder}/availspipeline/avails-pipeline-daily-agg"
    stats_output_path = f"oss://ttd-identity/adbrain-stats{BaseAvails.test_folder_without_prod}/avails-pipeline-daily-agg"
    ip_hashing_enabled = "false"
    maxmind_base_path = "oss://ttd-identity/external/maxmind"
    override_location_tables_path = "oss://ttd-identity/thetradedesk.db/provisioning/staticexpandedgeoelements/v=1/date=20250422"
    supply_vendor_id_white_list = "131:YouKu,133:IQiYi,172:Xunfei,194:HuanTV,253:Yex,290:Zhihu,300:Fliggy,320:Dynamic"
    useExtraIdColumns = "true"

    cluster_factory: Any = IdentityClustersAliyun
    daily_cluster_cores: int = 256
    intraday_cluster_cores: int = 128


# if you change this, make sure to change the schedule interval too!
hour_group_size = 6

dag_kwargs = {
    "schedule_interval": "3 0,6,12,18 * * *",
    # we can catchup a full day in one go
    "max_active_runs": 24 // hour_group_size if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 1,
    # max_active_tasks need to increase to allow parallel runs, with all the emr overheads setting this to 3x the num hours
    "max_active_tasks": 72,
    "doc_md": __doc__,
    "run_only_latest": None if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False,
}

# This DAG has been migrated to event-stream-data but remains specified here for Airflow history.
dag = DagHelpers.identity_dag(
    dag_id="avails-v3-hourly-and-daily-agg",
    start_date=datetime(2025, 2, 17, 0, 3) if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else test_start_date,
    end_date=None if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else test_end_date,
    **dag_kwargs
)

AvailsV2(dag, hour_group_size).get_hourly_and_daily_aggs()

AvailsV3(dag, hour_group_size).get_hourly_and_daily_aggs()

final_dag: DAG = dag.airflow_dag

# for China graphs
dag_china = DagHelpers.identity_dag(
    dag_id="avails-v3-hourly-and-daily-agg-china",
    start_date=datetime(2025, 5, 11, 6, 3) if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else test_start_date,
    end_date=None if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else test_end_date,
    tags=['CHINA'],
    slack_channel=Tags.slack_channel_alarms_testing,
    **dag_kwargs
)

AvailsV3China(dag_china, hour_group_size).get_hourly_and_daily_aggs()
final_dag_china: DAG = dag_china.airflow_dag
