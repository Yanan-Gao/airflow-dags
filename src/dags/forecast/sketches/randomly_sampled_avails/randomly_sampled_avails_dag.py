from datetime import datetime

from dags.forecast.aerospike_set_utils import create_get_inactive_set_version_and_lock_task, \
    create_activate_and_unlock_set_version_task
from dags.forecast.sketches.randomly_sampled_avails.constants import DAG_NAME, AEROSPIKE_HOSTS, METADATA_SET_NAME, \
    RAM_NAMESPACE, RAM_SET_NAME, AEROSPIKE_GEN_KEY, INACTIVE_AEROSPIKE_SET_KEY_AEROSPIKE, \
    INACTIVE_AEROSPIKE_SET_NUMBER_KEY_AEROSPIKE, LOOKUP_INACTIVE_AEROSPIKE_TASK_ID, UPDATE_AEROSPIKE_SET_TASK_ID
from dags.forecast.sketches.randomly_sampled_avails.tasks.avails_processing.avails_vectors_hmh import AvailsVectorsHMH
from dags.forecast.sketches.randomly_sampled_avails.tasks.avails_processing.avails_vectors_hmh_cluster import \
    AvailsVectorsHMHCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.bid_feedback_processing.bid_feedback_vectors_hmh import \
    BidFeedbackVectorsHMH
from dags.forecast.sketches.randomly_sampled_avails.tasks.bid_feedback_processing.bid_feedback_vectors_hmh_cluster import \
    BidFeedbackVectorsHMHCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.append_ram_daily_containment_records import \
    AppendRamDailyContainmentRecords
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.copy_dailyavails_vectors_sample_filtered_data import \
    CopyDailyAvailsVectorsSampleFilteredData
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.create_containment_pointer_records import \
    CreateContainmentPointerRecords
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.create_containment_records import \
    CreateContainmentRecords
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.create_containment_records_azure import \
    CreateContainmentRecordsAzure
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.create_containment_records_cluster import \
    CreateContainmentRecordsCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.create_containment_records_cluster_azure import \
    CreateContainmentRecordsClusterAzure
from dags.forecast.sketches.randomly_sampled_avails.tasks.containment.filter_containment_samples import \
    FilterContainmentSamples
from dags.forecast.sketches.randomly_sampled_avails.tasks.input.create_bid_feedback_logs import CreateBidFeedbackLogs
from dags.forecast.sketches.randomly_sampled_avails.tasks.input.create_vector_values import CreateVectorValues
from dags.forecast.sketches.randomly_sampled_avails.tasks.input.prepare_input_cluster import PrepareInputsCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.input.set_timestamps_and_iso_weekday import \
    SetTimestampsAndIsoWeekday

from dags.forecast.sketches.randomly_sampled_avails.tasks.output.write_ram_data_to_aerospike import \
    WriteRamDataToAerospike
from dags.forecast.sketches.randomly_sampled_avails.tasks.output.write_to_aerospike_cluster import \
    WriteToAerospikeCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.post_processing.post_processing_cluster import \
    PostProcessingCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.post_processing.prepare_data_for_export import \
    PrepareDataForExport
from dags.forecast.sketches.randomly_sampled_avails.tasks.post_processing.validate_and_collect_metrics import \
    ValidateAndCollectDataMetrics
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.map_avails import MapAvails
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.map_avails_cluster import MapAvailsCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.setup_stage_avails import SetupStageAvails
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.setup_stage_avails_table_cluster import \
    SetupStageAvailsTableCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.stage_targeting_data_table import \
    StageTargetingDataTable
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.stage_targeting_data_table_cluster import \
    StageTargetingDataTableCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.stage_xdevice_targeting_data_table import \
    StageXDeviceTargetingDataTable
from dags.forecast.sketches.randomly_sampled_avails.tasks.pre_processing.stage_xdevice_targeting_data_table_cluster import \
    StageXDeviceTargetingDataTableCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.append_targeting_data_ram_daily_vectors import \
    AppendTargetingDataRamDailyVectors
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.append_xdevice_targeting_data_ram_daily_vectors import \
    AppendXDeviceTargetingDataRamDailyVectors
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.copy_tdid_hmh_aggregate import \
    CopyTdidAndHMHData
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.data_elements_hmh_cluster import \
    DataElementsHMHCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.data_elements_hmh_cluster_azure import \
    DataElementsHMHClusterAzure
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.store_data_element_partial_aggs_to_hdfs import \
    StoreDataElementPartialAggsToHDFS
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.store_data_element_partial_aggs_to_hdfs_azure import \
    StoreDataElementPartialAggsToHDFSAzure
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.targeting_data_aggregate import \
    TargetingDataAggregate
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.targeting_data_aggregate_azure import \
    TargetingDataAggregateAzure
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.targeting_data_aggregate_merge import \
    TargetingDataAggregateMerge
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.targeting_data_aggregate_merge_cluster import \
    TargetingDataAggregateMergeCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.targeting_data_partial_agg_cluster import \
    TargetingDataPartialAggCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.tdid_hmh_aggregate import \
    TdidHMHAggregate
from dags.forecast.sketches.randomly_sampled_avails.tasks.targeting_data_processing.xdevice_targeting_data_aggregate_azure import \
    XDeviceTargetingDataAggregateAzure
from dags.forecast.sketches.randomly_sampled_avails.tasks.weekly_aggregates.lookup_last_good_iso_weekdays import \
    LookupLastGoodIsoWeekdays
from dags.forecast.sketches.randomly_sampled_avails.tasks.weekly_aggregates.weekly_avails_containment_records import \
    WeeklyAvailsContainment
from dags.forecast.sketches.randomly_sampled_avails.tasks.weekly_aggregates.weekly_feedback_containment_records import \
    WeeklyFeedbackContainmentRecords
from dags.forecast.sketches.randomly_sampled_avails.tasks.weekly_aggregates.weekly_pointer_records import \
    WeeklyPointerRecords
from dags.forecast.sketches.randomly_sampled_avails.tasks.weekly_aggregates.weekly_processing_cluster import \
    WeeklyProcessingCluster
from dags.forecast.sketches.randomly_sampled_avails.tasks.weekly_aggregates.weekly_ram_vectors import WeeklyRamVectors
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import FORECAST


class ForecastingRamDAG:
    """
    Class to encapsulate RAM DAG.

    This DAG has as input avails data from different sources and outputs sketches (probabilistic data structures)
    Sketches produced by this DAG are stored in Aerospike where then are consumed by RAM service
    """

    def __init__(self):
        self._create_dag()
        self._create_tasks()
        self._set_dependencies()

    def _create_dag(self):
        self.dag = TtdDag(
            dag_id=DAG_NAME,
            start_date=datetime(2025, 5, 2, 3, 0),
            schedule_interval="0 3 * * *",
            tags=['FORECAST'],
            max_active_runs=3,
            slack_channel='#dev-forecasting-alarms',
            slack_tags=FORECAST.data_charter().sub_team,
        )

    def _create_tasks(self):
        self._create_input_tasks()
        self._create_pre_processing_tasks()
        self._create_avails_processing_tasks()
        self._create_bid_feedback_processing_tasks()
        self._create_targeting_data_processing_tasks()
        self._create_containment_processing_tasks()
        self._create_weekly_aggregates_tasks()
        self._create_post_processing_tasks()
        self._create_output_tasks()

    def _set_dependencies(self):
        self._set_input_tasks_dependencies()
        self._set_bid_feedback_processing_tasks_dependencies()
        self._set_pre_processing_tasks_dependencies()
        self._set_avails_processing_tasks_dependencies()
        self._set_targeting_data_processing_tasks_dependencies()
        self._set_containment_processing_tasks_dependencies()
        self._set_weekly_aggregate_tasks_dependencies()
        self._set_post_processing_tasks_dependencies()
        self._set_output_tasks_dependencies()

    def _set_output_tasks_dependencies(self):
        self.validate_and_collect_metrics >> self.lookup_inactive_aerospike_set
        self.write_to_aerospike_cluster.add_parallel_body_task(self.write_ram_data_to_aerospike)
        self.lookup_inactive_aerospike_set >> self.write_to_aerospike_cluster
        self.write_ram_data_to_aerospike.watch_job_task >> self.update_active_aerospike_set

    def _set_post_processing_tasks_dependencies(self):
        self.prepare_data_for_export >> self.validate_and_collect_metrics
        self.post_processing_cluster.add_parallel_body_task(self.prepare_data_for_export)
        self.post_processing_cluster.add_parallel_body_task(self.validate_and_collect_metrics)
        self.weekly_feedback_containment_records.watch_job_task >> self.post_processing_cluster

    def _set_weekly_aggregate_tasks_dependencies(self):
        self.weekly_ram_vectors.watch_job_task >> self.weekly_pointer_records
        self.weekly_pointer_records.watch_job_task >> self.weekly_avails_containment_records
        self.weekly_avails_containment_records.watch_job_task >> self.weekly_feedback_containment_records
        self.weekly_processing_cluster.add_parallel_body_task(self.weekly_ram_vectors)
        self.weekly_processing_cluster.add_parallel_body_task(self.weekly_pointer_records)
        self.weekly_processing_cluster.add_parallel_body_task(self.weekly_avails_containment_records)
        self.weekly_processing_cluster.add_parallel_body_task(self.weekly_feedback_containment_records)
        self.lookup_last_good_iso_weekdays >> self.weekly_processing_cluster

    def _set_containment_processing_tasks_dependencies(self):
        self.create_containment_pointer_records >> self.filter_containment_samples
        self.filter_containment_samples >> self.create_containment_records
        self.create_containment_records_cluster.add_parallel_body_task(self.create_containment_pointer_records)
        self.create_containment_records_cluster.add_parallel_body_task(self.create_containment_records)
        self.create_containment_records_cluster_azure.add_parallel_body_task(self.create_containment_records_azure)
        self.bid_feedback_hmh.watch_job_task >> self.create_containment_records_cluster
        self.avails_vectors_hmh.watch_job_task >> self.create_containment_records_cluster
        self.targeting_data_aggregate_merge.watch_job_task >> self.create_containment_records_cluster
        self.append_xdevice_targeting_data_ram_daily_vectors >> self.create_containment_records_cluster
        self.append_targeting_data_ram_daily_vectors >> self.create_containment_records_cluster
        self.filter_containment_samples.watch_job_task >> self.copy_dailyavails_vectors_sample_filtered
        self.copy_dailyavails_vectors_sample_filtered >> self.create_containment_records_cluster_azure
        self.create_containment_records_azure.watch_job_task >> self.append_daily_containement_records
        self.append_daily_containement_records >> self.lookup_last_good_iso_weekdays
        self.create_containment_records >> self.lookup_last_good_iso_weekdays

    def _set_targeting_data_processing_tasks_dependencies(self):
        self.targeting_data_partial_agg_cluster.add_parallel_body_task(self.tdid_and_hmh_aggregate)
        self.tdid_and_hmh_aggregate.watch_job_task >> self.data_elements_hmh_cluster_0
        self.tdid_and_hmh_aggregate.watch_job_task >> self.data_elements_hmh_cluster_1
        self.data_elements_hmh_cluster_0.add_parallel_body_task(self.store_data_element_partial_aggs_to_hdfs_0)
        self.data_elements_hmh_cluster_0.add_parallel_body_task(self.targeting_data_aggregate_0)
        self.data_elements_hmh_cluster_1.add_parallel_body_task(self.store_data_element_partial_aggs_to_hdfs_1)
        self.data_elements_hmh_cluster_1.add_parallel_body_task(self.targeting_data_aggregate_1)
        self.store_data_element_partial_aggs_to_hdfs_0 >> self.targeting_data_aggregate_0
        self.store_data_element_partial_aggs_to_hdfs_1 >> self.targeting_data_aggregate_1
        self.targeting_data_aggregate_merge_cluster.add_parallel_body_task(self.targeting_data_aggregate_merge)
        self.targeting_data_aggregate_0.watch_job_task >> self.targeting_data_aggregate_merge_cluster
        self.targeting_data_aggregate_1.watch_job_task >> self.targeting_data_aggregate_merge
        self.map_avails.watch_job_task >> self.targeting_data_partial_agg_cluster
        self.tdid_and_hmh_aggregate.watch_job_task >> self.copy_tdid_hmh_aggregate
        self.store_data_element_partial_aggs_to_hdfs_azure.watch_job_task >> self.targeting_data_aggregate_azure
        self.targeting_data_aggregate_azure.watch_job_task >> self.xdevice_targeting_data_aggregate_azure
        self.data_elements_hmh_cluster_azure.add_parallel_body_task(self.store_data_element_partial_aggs_to_hdfs_azure)
        self.data_elements_hmh_cluster_azure.add_parallel_body_task(self.targeting_data_aggregate_azure)
        self.data_elements_hmh_cluster_azure.add_parallel_body_task(self.xdevice_targeting_data_aggregate_azure)
        self.copy_tdid_hmh_aggregate >> self.data_elements_hmh_cluster_azure
        self.xdevice_targeting_data_aggregate_azure.watch_job_task >> self.append_xdevice_targeting_data_ram_daily_vectors
        self.targeting_data_aggregate_azure.watch_job_task >> self.append_targeting_data_ram_daily_vectors
        self.targeting_data_aggregate_merge.watch_job_task >> self.append_targeting_data_ram_daily_vectors
        self.targeting_data_aggregate_merge.watch_job_task >> self.append_xdevice_targeting_data_ram_daily_vectors

    def _set_avails_processing_tasks_dependencies(self):
        self.avails_vectors_hmh_cluster.add_parallel_body_task(self.avails_vectors_hmh)
        self.map_avails.watch_job_task >> self.avails_vectors_hmh_cluster

    def _set_pre_processing_tasks_dependencies(self):
        self.setup_avails_cluster.add_parallel_body_task(self.setup_stage_avails)
        self.create_bid_feedback_logs.watch_job_task >> self.setup_avails_cluster
        self.stage_targeting_data_table_cluster.add_parallel_body_task(self.stage_targeting_data_table)
        self.setup_stage_avails.watch_job_task >> self.stage_targeting_data_table_cluster
        self.stage_xdevice_targeting_data_table_cluster.add_parallel_body_task(self.stage_xdevice_targeting_data_table)
        self.setup_stage_avails.watch_job_task >> self.stage_xdevice_targeting_data_table_cluster
        self.map_avails_cluster.add_parallel_body_task(self.map_avails)
        self.stage_targeting_data_table.watch_job_task >> self.map_avails_cluster
        self.stage_xdevice_targeting_data_table.watch_job_task >> self.map_avails_cluster

    def _set_bid_feedback_processing_tasks_dependencies(self):
        self.bid_feedback_hmh_cluster.add_parallel_body_task(self.bid_feedback_hmh)
        self.create_bid_feedback_logs.watch_job_task >> self.bid_feedback_hmh_cluster

    def _set_input_tasks_dependencies(self):
        self.create_vector_values.watch_job_task >> self.create_bid_feedback_logs
        self.prepare_input_cluster.add_parallel_body_task(self.create_vector_values)
        self.prepare_input_cluster.add_parallel_body_task(self.create_bid_feedback_logs)

        self.dag >> self.set_timestamps_and_iso_weekday
        self.set_timestamps_and_iso_weekday >> self.prepare_input_cluster

    def _create_output_tasks(self):
        self.lookup_inactive_aerospike_set = create_get_inactive_set_version_and_lock_task(
            dag=self.dag.airflow_dag,
            task_id=LOOKUP_INACTIVE_AEROSPIKE_TASK_ID,
            aerospike_hosts=AEROSPIKE_HOSTS,
            namespace=RAM_NAMESPACE,
            metadata_set_name=METADATA_SET_NAME,
            set_key=RAM_SET_NAME,
            inactive_xcom_set_number_key=INACTIVE_AEROSPIKE_SET_NUMBER_KEY_AEROSPIKE,
            inactive_xcom_set_key=INACTIVE_AEROSPIKE_SET_KEY_AEROSPIKE,
            aerospike_gen_xcom_key=AEROSPIKE_GEN_KEY
        )
        self.update_active_aerospike_set = create_activate_and_unlock_set_version_task(
            dag=self.dag.airflow_dag,
            task_id=UPDATE_AEROSPIKE_SET_TASK_ID,
            aerospike_hosts=AEROSPIKE_HOSTS,
            inactive_get_task_id=LOOKUP_INACTIVE_AEROSPIKE_TASK_ID,
            job_name=DAG_NAME,
            namespace=RAM_NAMESPACE,
            set_key=RAM_SET_NAME,
            inactive_xcom_set_number_key=INACTIVE_AEROSPIKE_SET_NUMBER_KEY_AEROSPIKE,
            aerospike_gen_xcom_key=AEROSPIKE_GEN_KEY,
            metadata_set_name=METADATA_SET_NAME
        )
        self.write_to_aerospike_cluster = WriteToAerospikeCluster()
        self.write_ram_data_to_aerospike = WriteRamDataToAerospike()

    def _create_post_processing_tasks(self):
        self.post_processing_cluster = PostProcessingCluster()
        self.prepare_data_for_export = PrepareDataForExport()
        self.validate_and_collect_metrics = ValidateAndCollectDataMetrics()

    def _create_weekly_aggregates_tasks(self):
        self.weekly_processing_cluster = WeeklyProcessingCluster()
        self.weekly_avails_containment_records = WeeklyAvailsContainment()
        self.weekly_feedback_containment_records = WeeklyFeedbackContainmentRecords()
        self.weekly_pointer_records = WeeklyPointerRecords()
        self.weekly_ram_vectors = WeeklyRamVectors()
        self.lookup_last_good_iso_weekdays = LookupLastGoodIsoWeekdays(dag=self.dag.airflow_dag)

    def _create_containment_processing_tasks(self):
        self.create_containment_records_cluster = CreateContainmentRecordsCluster()
        self.create_containment_records_cluster_azure = CreateContainmentRecordsClusterAzure()
        self.filter_containment_samples = FilterContainmentSamples()
        self.create_containment_records = CreateContainmentRecords()
        self.copy_dailyavails_vectors_sample_filtered = CopyDailyAvailsVectorsSampleFilteredData()
        self.create_containment_records_azure = CreateContainmentRecordsAzure()
        self.create_containment_pointer_records = CreateContainmentPointerRecords()
        self.append_daily_containement_records = AppendRamDailyContainmentRecords()

    def _create_targeting_data_processing_tasks(self):
        self.targeting_data_partial_agg_cluster = TargetingDataPartialAggCluster()
        self.targeting_data_aggregate_merge_cluster = TargetingDataAggregateMergeCluster()
        self.data_elements_hmh_cluster_0 = DataElementsHMHCluster(0)
        self.data_elements_hmh_cluster_1 = DataElementsHMHCluster(1)
        self.data_elements_hmh_cluster_azure = DataElementsHMHClusterAzure()
        self.store_data_element_partial_aggs_to_hdfs_0 = StoreDataElementPartialAggsToHDFS(0)
        self.store_data_element_partial_aggs_to_hdfs_1 = StoreDataElementPartialAggsToHDFS(1)
        self.store_data_element_partial_aggs_to_hdfs_azure = StoreDataElementPartialAggsToHDFSAzure()
        self.tdid_and_hmh_aggregate = TdidHMHAggregate()
        self.targeting_data_aggregate_0 = TargetingDataAggregate(0)
        self.targeting_data_aggregate_1 = TargetingDataAggregate(1)
        self.targeting_data_aggregate_merge = TargetingDataAggregateMerge()
        self.targeting_data_aggregate_azure = TargetingDataAggregateAzure()
        self.xdevice_targeting_data_aggregate_azure = XDeviceTargetingDataAggregateAzure()
        self.copy_tdid_hmh_aggregate = CopyTdidAndHMHData()
        self.append_targeting_data_ram_daily_vectors = AppendTargetingDataRamDailyVectors()
        self.append_xdevice_targeting_data_ram_daily_vectors = AppendXDeviceTargetingDataRamDailyVectors()

    def _create_bid_feedback_processing_tasks(self):
        self.bid_feedback_hmh_cluster = BidFeedbackVectorsHMHCluster()
        self.bid_feedback_hmh = BidFeedbackVectorsHMH()

    def _create_avails_processing_tasks(self):
        self.avails_vectors_hmh_cluster = AvailsVectorsHMHCluster()
        self.avails_vectors_hmh = AvailsVectorsHMH()

    def _create_pre_processing_tasks(self):
        self.setup_avails_cluster = SetupStageAvailsTableCluster()
        self.setup_stage_avails = SetupStageAvails()
        self.stage_targeting_data_table_cluster = StageTargetingDataTableCluster()
        self.stage_targeting_data_table = StageTargetingDataTable()
        self.stage_xdevice_targeting_data_table_cluster = StageXDeviceTargetingDataTableCluster()
        self.stage_xdevice_targeting_data_table = StageXDeviceTargetingDataTable()
        self.map_avails_cluster = MapAvailsCluster()
        self.map_avails = MapAvails()

    def _create_input_tasks(self):
        self.set_timestamps_and_iso_weekday = SetTimestampsAndIsoWeekday(dag=self.dag.airflow_dag)
        self.prepare_input_cluster = PrepareInputsCluster()
        self.create_vector_values = CreateVectorValues()
        self.create_bid_feedback_logs = CreateBidFeedbackLogs()


ttd_dag = ForecastingRamDAG().dag
dag = ttd_dag.airflow_dag
