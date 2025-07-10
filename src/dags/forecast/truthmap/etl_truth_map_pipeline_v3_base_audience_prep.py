from datetime import datetime, timedelta
from typing import Any, List, Union

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from google.protobuf.timestamp_pb2 import Timestamp

from dags.forecast.truthmap.generate_frequency_map_cluster import get_generate_frequency_map_cluster, MapType
from dags.forecast.validation.history_service.history_service_rpcs import retrieve_forecasts_with_audience
from dags.forecast.truthmap.utils.datasets import get_frequency_map_result
from dags.forecast.utils.team import TEAM_NAME
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
###########################################
#   job settings
###########################################
from ttd.ttdenv import TtdEnvFactory

tmap_gen_job_schedule_interval = timedelta(days=30)
job_name = "etl-truth-map-pipeline-v3-audience-prep"
jar_path = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
spark_3_emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3  # Ensure spark3 and the correct scala version is used for the spark steps


def id_to_targeting_data_grouping_mapping_task_name(targeting_data_grouping):
    return f"IdMapping-{targeting_data_grouping}"


preparing_targeting_data_grouping_task_name = "PrepareTargetingDataGroupings"
targeting_data_grouping_mapping_type = "targetingDataGrouping"
data_group = "dataGroup"
audience = "audience"
audience_prep_suffix = "audience-prep"


class AudienceEnhancedTruthMapDag:

    def __init__(
        self,
        job_start_date: datetime,
        job_schedule_interval: str,
        frequency_map_dag_version: int,
        # These ints are passed into ExternalTaskSensors later. They will need to correspond to the
        # appropriate fmap DAGs.
        id_to_targeting_data_mapping_execution_delta: int,
        avail_sampling_rate: int,
        avail_stream: str,
        id_types: list,
        time_ranges: tuple = None,
        # Basically put this in place here to enforce creation of a new DAG if something was changed. For example,
        # if you change the execution time of an existing DAG, Airflow won't pick up that change unless a new DAG
        # was created.
        version_number: str = ""
    ):
        self.job_start_date = job_start_date
        self.job_schedule_interval = job_schedule_interval
        self.frequency_map_dag_version = frequency_map_dag_version
        self.avail_sampling_rate = avail_sampling_rate
        self.avail_stream = avail_stream
        self.id_to_targeting_data_mapping_execution_delta = id_to_targeting_data_mapping_execution_delta
        self.id_types = id_types
        self.time_ranges = time_ranges
        if version_number:
            version_number = "-v" + version_number
        self.job_name = f"etl-truth-map-pipeline-forecasting-{avail_stream}{version_number}-{audience_prep_suffix}"
        self.dag = self.create_dag()

    def create_dag(self) -> DAG:
        forecasting_tmap_audience_prep_dag = TtdDag(
            dag_id=self.job_name,
            start_date=self.job_start_date,
            schedule_interval=self.job_schedule_interval,
            retries=1,
            retry_delay=timedelta(minutes=2),
            tags=[TEAM_NAME],
            slack_tags=TEAM_NAME,
            slack_channel="#dev-forecast-models-charter-alerts",
            enable_slack_alert=True
        )

        dag = forecasting_tmap_audience_prep_dag.airflow_dag

        ###########################################
        #   EMR job settings
        ###########################################
        master_fleet_instance_type_configs = EmrFleetInstanceTypes(
            instance_types=[R6gd.r6gd_8xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
        )

        core_fleet_instance_type_configs = EmrFleetInstanceTypes(
            instance_types=[
                R6gd.r6gd_8xlarge().with_fleet_weighted_capacity(2),
                R6gd.r6gd_12xlarge().with_fleet_weighted_capacity(3),
                R6gd.r6gd_16xlarge().with_fleet_weighted_capacity(4)
            ],
            on_demand_weighted_capacity=72,
        )

        ###########################################
        standard_cluster_tags = {
            "Team": TEAM_NAME,
        }

        def upload_forecasts_with_audience_to_s3(**kwargs):
            target_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d")
            start_timestamp = Timestamp()
            end_timestamp = Timestamp()
            start_timestamp.FromDatetime(target_date)
            end_timestamp.FromDatetime(target_date + timedelta(days=1))
            ttd_env_string = TtdEnvFactory.get_from_system().execution_env
            retrieve_forecasts_with_audience(
                ttd_env=ttd_env_string,
                start=start_timestamp,
                end=end_timestamp,
                should_upload_to_cloud_storage=True,
                should_return_results=False
            )
            return True

        upload_forecasts_with_audience_to_s3_task = OpTask(
            op=PythonOperator(
                task_id='get-forecasts-with-audience-from-history-service',
                dag=dag,
                python_callable=upload_forecasts_with_audience_to_s3,
                provide_context=True
            )
        )

        def generate_truth_maps_branch(tmap_gen_clusters: List[EmrClusterTask], final_dummy: OpTask,
                                       **kwargs: Any) -> Union[List[str], str]:
            # The plan is to generate truth maps on the first day of each month
            if datetime.strptime(kwargs["ds"], "%Y-%m-%d").day == 1:
                return [f"select_subnets_{cluster.task_id}" for cluster in tmap_gen_clusters]
            else:
                return final_dummy.task_id

        for id_type in self.id_types:
            targeting_data_fmap_result = get_frequency_map_result(self.avail_stream, id_type)

            wait_for_targeting_data_fmap_result = OpTask(
                op=DatasetCheckSensor(
                    task_id=f"wait_for_datasets_{id_type}",
                    datasets=[targeting_data_fmap_result],
                    ds_date="{{data_interval_start.to_datetime_string()}}",
                    poke_interval=int(timedelta(minutes=5).total_seconds()),
                    mode="reschedule",
                    timeout=int(timedelta(hours=8).total_seconds()),
                )
            )

            prepare_targeting_data_grouping_and_mapping_cluster_task_name = f"{preparing_targeting_data_grouping_task_name}" \
                                                                            f"_And_{id_to_targeting_data_grouping_mapping_task_name('all')}" \
                                                                            f"-{self.avail_stream}-{id_type}"

            prepare_targeting_data_grouping_and_mapping_cluster_task = EmrClusterTask(
                name=prepare_targeting_data_grouping_and_mapping_cluster_task_name,
                master_fleet_instance_type_configs=master_fleet_instance_type_configs,
                core_fleet_instance_type_configs=core_fleet_instance_type_configs,
                cluster_tags={
                    **standard_cluster_tags, "Process": "PreparingTargetingData"
                },
                enable_prometheus_monitoring=True,
                emr_release_label=spark_3_emr_release_label
            )

            prepare_targeting_data_grouping_task = EmrJobTask(
                cluster_specs=prepare_targeting_data_grouping_and_mapping_cluster_task.cluster_specs,
                name=f"{preparing_targeting_data_grouping_task_name}-{id_type}",
                class_name="com.thetradedesk.etlforecastjobs.universalforecasting.truthmap.PrepareTargetingDataForTruthMaps",
                executable_path=jar_path,
                eldorado_config_option_pairs_list=[("availStream", self.avail_stream), ("idType", id_type), ("targetDate", "{{ ds }}")]
            )

            targeting_data_grouping_mapping_config = [("availStream", self.avail_stream), ("idType", id_type),
                                                      ("mappingTypes", targeting_data_grouping_mapping_type), ("targetDate", "{{ ds }}")]

            targeting_data_grouping_id_mapping_task_data_group = EmrJobTask(
                cluster_specs=prepare_targeting_data_grouping_and_mapping_cluster_task.cluster_specs,
                name=f"{id_to_targeting_data_grouping_mapping_task_name('dataGroup')}-{id_type}",
                class_name="com.thetradedesk.etlforecastjobs.universalforecasting.frequencymap.generation.MappingGeneratingJob",
                executable_path=jar_path,
                eldorado_config_option_pairs_list=targeting_data_grouping_mapping_config + [("targetingDataGrouping", data_group)],
                # big executors to avoid OOM errors
                additional_args_option_pairs_list=[("executor-memory", "211G"), ("driver-memory", "211G"),
                                                   ("conf", "spark.executor.memoryOverhead=21606m"),
                                                   ("conf", "spark.driver.memoryOverhead=21606m"),
                                                   ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                                   ("conf", "spark.driver.maxResultSize=211G"),
                                                   ("conf", "spark.sql.shuffle.partitions=12000"), ("conf", "spark.yarn.maxAppAttempts=1")]
            )

            targeting_data_grouping_id_mapping_task_audience = EmrJobTask(
                cluster_specs=prepare_targeting_data_grouping_and_mapping_cluster_task.cluster_specs,
                name=f"{id_to_targeting_data_grouping_mapping_task_name('audience')}-{id_type}",
                class_name="com.thetradedesk.etlforecastjobs.universalforecasting.frequencymap.generation.MappingGeneratingJob",
                executable_path=jar_path,
                eldorado_config_option_pairs_list=targeting_data_grouping_mapping_config + [("targetingDataGrouping", audience)]
            )

            prepare_targeting_data_grouping_and_mapping_cluster_task.add_parallel_body_task(
                ChainOfTasks(
                    f"{preparing_targeting_data_grouping_task_name}_{id_to_targeting_data_grouping_mapping_task_name('dataGroup')}"
                    f"_{id_to_targeting_data_grouping_mapping_task_name('audience')}_{id_type}", [
                        prepare_targeting_data_grouping_task, targeting_data_grouping_id_mapping_task_data_group,
                        targeting_data_grouping_id_mapping_task_audience
                    ]
                )
            )

            forecasting_tmap_audience_prep_dag >> wait_for_targeting_data_fmap_result
            wait_for_targeting_data_fmap_result >> prepare_targeting_data_grouping_and_mapping_cluster_task
            forecasting_tmap_audience_prep_dag >> upload_forecasts_with_audience_to_s3_task
            upload_forecasts_with_audience_to_s3_task >> prepare_targeting_data_grouping_and_mapping_cluster_task

            generate_data_group_truth_maps_cluster_task = get_generate_frequency_map_cluster(
                map_type=MapType.TruthMap,
                avail_stream=self.avail_stream,
                id_type=id_type,
                mapping_types=targeting_data_grouping_mapping_type,
                time_ranges=self.time_ranges,
                target_date='{{ ds }}',
                jar_path=jar_path,
                emr_release_label=spark_3_emr_release_label,
                timeout=6,
                task_id_suffix="-dataGroup",
                additional_config=[("partitioning", data_group)]
            )

            generate_audience_truth_maps_cluster_task = get_generate_frequency_map_cluster(
                map_type=MapType.TruthMap,
                avail_stream=self.avail_stream,
                id_type=id_type,
                mapping_types=targeting_data_grouping_mapping_type,
                time_ranges=self.time_ranges,
                target_date='{{ ds }}',
                jar_path=jar_path,
                emr_release_label=spark_3_emr_release_label,
                timeout=6,
                task_id_suffix="-audience",
                additional_config=[("partitioning", audience)]
            )

            tmap_cluster_tasks = [generate_data_group_truth_maps_cluster_task, generate_audience_truth_maps_cluster_task]

            final_dummy = OpTask(op=DummyOperator(task_id=f"final_dummy_{id_type}", dag=dag))

            truth_maps_generate_branch = OpTask(
                op=BranchPythonOperator(
                    task_id=f"generate_targeting_data_grouping_truth_maps_{id_type}",
                    python_callable=generate_truth_maps_branch,
                    op_kwargs={
                        "tmap_gen_clusters": tmap_cluster_tasks,
                        "final_dummy": final_dummy
                    },
                    provide_context=True,
                    dag=dag
                )
            )
            prepare_targeting_data_grouping_and_mapping_cluster_task >> truth_maps_generate_branch

            truth_maps_generate_branch >> generate_data_group_truth_maps_cluster_task >> final_dummy
            truth_maps_generate_branch >> generate_audience_truth_maps_cluster_task >> final_dummy

        return dag
