from datetime import datetime, timedelta
from typing import Any, Callable, List, Union

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

from dags.forecast.truthmap.generate_frequency_map_cluster import MapType, \
    get_generate_frequency_map_cluster, LARGE_FMAP_CLUSTER_SETTINGS
from dags.forecast.truthmap.utils.datasets import get_audience_number_to_data_group_numbers, \
    get_data_group_number_to_targeting_data_ids, get_id_to_audiences_frequency
from dags.forecast.truthmap.utils.constants.mapping_type import MappingType
from dags.forecast.truthmap.utils.constants.task_name import TaskName
from dags.forecast.truthmap.utils.job_configs import get_base_truth_map_combination_generation_config, \
    get_base_truth_map_mapping_config, enhance_base_truth_map_combination_generation_config_with_partials, \
    enhance_base_truth_map_mapping_config_with_partials
from dags.forecast.utils.avails import get_wait_avails_operator
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
tmap_gen_job_schedule_interval = timedelta(days=30)
job_name = "etl-truth-map-pipeline-v3"
jar_path = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
spark_3_emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3  # Ensure spark3 and the correct scala version is used for the spark steps


class TruthMapDag:

    def __init__(
        self,
        job_start_date: datetime,
        job_schedule_interval: str,
        avail_sampling_rate: int,
        avail_stream: str,
        combination_list_names: List[str],
        combination_list_types: List[str],
        id_types: List[str],
        time_ranges: tuple = None,
        # Basically put this in place here to enforce creation of a new DAG if something was changed. For example,
        # if you change the execution time of an existing DAG, Airflow won't pick up that change unless a new DAG
        # was created.
        version_number: str = ""
    ):
        self.job_start_date = job_start_date
        self.job_schedule_interval = job_schedule_interval
        self.avail_sampling_rate = avail_sampling_rate
        self.avail_stream = avail_stream
        self.combination_list_names = combination_list_names
        self.combination_list_types = combination_list_types
        self.id_types = id_types
        self.time_ranges = time_ranges
        if version_number:
            version_number = "-v" + version_number
        self.job_name = f"etl-truth-map-pipeline-forecasting-{avail_stream}{version_number}"
        self.dag = self.create_dag()

    def create_dag(self) -> DAG:
        forecasting_tmap_dag = TtdDag(
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

        dag = forecasting_tmap_dag.airflow_dag

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
            on_demand_weighted_capacity=180,
        )

        wait_for_avails_data_ready = get_wait_avails_operator(self.avail_stream, dag)

        ###########################################
        # Generating/combining combinations and id mapping happen on the same cluster since we use data cached to HDFS
        standard_cluster_tags = {
            "Team": TEAM_NAME,
        }

        forecasting_tmap_dag >> wait_for_avails_data_ready

        def generate_truth_maps_branch(tmap_gen_clusters: List[EmrClusterTask], final_dummy: OpTask,
                                       **kwargs: Any) -> Union[List[str], str]:
            # The plan is to generate truth maps on the first day of each month
            if datetime.strptime(kwargs["ds"], "%Y-%m-%d").day == 1:
                return [f"select_subnets_{cluster.task_id}" for cluster in tmap_gen_clusters]
            else:
                return final_dummy.task_id

        for id_type in self.id_types:
            for combination_list_name, combination_list_type in zip(self.combination_list_names, self.combination_list_types):
                if combination_list_name == MappingType.INTERSECT_COMBINATION_LIST_NAME and combination_list_type == MappingType.INTERSECT_COMBINATION_TYPE:
                    self._generate_intersect_combinations_truth_maps_dag(
                        id_type=id_type,
                        combination_list_name=combination_list_name,
                        combination_list_type=combination_list_type,
                        dag=dag,
                        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
                        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
                        standard_cluster_tags=standard_cluster_tags,
                        wait_for_avails_data_ready=wait_for_avails_data_ready,
                        generate_truth_maps_branch=generate_truth_maps_branch
                    )
                elif combination_list_name == MappingType.UNION_COMBINATION_LIST_NAME and combination_list_type == MappingType.UNION_COMBINATION_TYPE:
                    self._generate_union_combinations_truth_maps_dag(
                        id_type=id_type,
                        combination_list_name=combination_list_name,
                        combination_list_type=combination_list_type,
                        dag=dag,
                        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
                        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
                        standard_cluster_tags=standard_cluster_tags,
                        wait_for_avails_data_ready=wait_for_avails_data_ready,
                        generate_truth_maps_branch=generate_truth_maps_branch
                    )
                else:
                    raise Exception(
                        f"Combination list name {combination_list_name} and combination list type {combination_list_type} are incompatible"
                    )
        return dag

    def _get_combination_generation_and_mapping_cluster_task(
        self, id_type: str, combination_list_type: str, master_fleet_instance_type_configs: EmrFleetInstanceTypes,
        core_fleet_instance_type_configs: EmrFleetInstanceTypes, standard_cluster_tags: dict
    ) -> EmrClusterTask:
        return EmrClusterTask(
            name=
            f"{TaskName.COMBINATION_GENERATION_TASK_NAME}_And_{TaskName.ID_MAPPING_TASK_NAME}-{self.avail_stream}-{id_type}-{combination_list_type}",
            master_fleet_instance_type_configs=master_fleet_instance_type_configs,
            core_fleet_instance_type_configs=core_fleet_instance_type_configs,
            cluster_tags={
                **standard_cluster_tags, "Process": "Combination-Generation-And-Mapping"
            },
            enable_prometheus_monitoring=True,
            emr_release_label=spark_3_emr_release_label
        )

    def _generate_intersect_combinations_truth_maps_dag(
        self, id_type: str, combination_list_name: str, combination_list_type: str, dag: DAG,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes, core_fleet_instance_type_configs: EmrFleetInstanceTypes,
        standard_cluster_tags: dict, wait_for_avails_data_ready: OpTask, generate_truth_maps_branch: Callable[..., Union[List[str], str]]
    ) -> None:
        audience_number_to_data_group_numbers = get_audience_number_to_data_group_numbers(self.avail_stream, id_type)
        data_group_number_to_targeting_data_ids = get_data_group_number_to_targeting_data_ids(self.avail_stream, id_type)
        id_to_audiences_frequency = get_id_to_audiences_frequency(self.avail_stream, id_type)

        wait_for_audiences_ready = OpTask(
            op=DatasetCheckSensor(
                task_id=f"wait_for_audiences_{id_type}",
                datasets=[audience_number_to_data_group_numbers, data_group_number_to_targeting_data_ids, id_to_audiences_frequency],
                ds_date="{{data_interval_start.to_datetime_string()}}",
                poke_interval=int(timedelta(minutes=5).total_seconds()),
                mode="reschedule",
                timeout=int(timedelta(hours=8).total_seconds()),
            )
        )

        combination_generation_and_mapping_cluster_task = self._get_combination_generation_and_mapping_cluster_task(
            id_type, combination_list_type, master_fleet_instance_type_configs, core_fleet_instance_type_configs, standard_cluster_tags
        )

        base_combination_generation_config = get_base_truth_map_combination_generation_config(
            self.avail_sampling_rate, self.avail_stream, combination_list_name, id_type
        )
        avail_sourced_random_intersect_combination_generation_config = enhance_base_truth_map_combination_generation_config_with_partials(
            base_combination_generation_config, combination_list_name
        )

        truth_map_combination_generation_task = EmrJobTask(
            cluster_specs=combination_generation_and_mapping_cluster_task.cluster_specs,
            name=f"{TaskName.COMBINATION_GENERATION_TASK_NAME}-{id_type}-{combination_list_type}",
            class_name="com.thetradedesk.etlforecastjobs.universalforecasting.truthmap.combinationgeneration.CombinationListGenerationJob",
            executable_path=jar_path,
            eldorado_config_option_pairs_list=avail_sourced_random_intersect_combination_generation_config,
            additional_args_option_pairs_list=[("conf", "spark.kryoserializer.buffer.max=512m")],
            configure_cluster_automatically=True
        )

        base_mapping_config = get_base_truth_map_mapping_config(
            self.avail_stream, combination_list_name, combination_list_type, MappingType.INTERSECT_COMBINATION_MAPPING_TYPE, id_type
        )
        truth_map_mapping_config = enhance_base_truth_map_mapping_config_with_partials(base_mapping_config, combination_list_name)

        intersect_combinations_id_mapping_task = EmrJobTask(
            cluster_specs=combination_generation_and_mapping_cluster_task.cluster_specs,
            name=f"{TaskName.ID_MAPPING_TASK_NAME}-{id_type}-{combination_list_type}",
            class_name="com.thetradedesk.etlforecastjobs.universalforecasting.frequencymap.generation.MappingGeneratingJob",
            executable_path=jar_path,
            eldorado_config_option_pairs_list=truth_map_mapping_config,
            # big executors to avoid OOM errors
            additional_args_option_pairs_list=[("executor-memory", "211G"), ("driver-memory", "211G"),
                                               ("conf", "spark.executor.memoryOverhead=21606m"),
                                               ("conf", "spark.driver.memoryOverhead=21606m"),
                                               ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                               ("conf", "spark.driver.maxResultSize=211G"), ("conf", "spark.sql.shuffle.partitions=12000"),
                                               ("conf", "spark.yarn.maxAppAttempts=1")]
        )

        truth_map_combination_generation_task >> intersect_combinations_id_mapping_task

        combination_generation_and_mapping_cluster_task.add_parallel_body_task(
            ChainOfTasks(
                f"{TaskName.COMBINATION_GENERATION_TASK_NAME}_{TaskName.ID_MAPPING_TASK_NAME}_{id_type}_{combination_list_type}", [
                    truth_map_combination_generation_task,
                    intersect_combinations_id_mapping_task,
                ]
            )
        )

        wait_for_avails_data_ready >> wait_for_audiences_ready >> combination_generation_and_mapping_cluster_task

        ###########################################
        # Generating truth maps from aggregated mappings
        tmap_cluster_tasks = [
            get_generate_frequency_map_cluster(
                map_type=MapType.TruthMap,
                avail_stream=self.avail_stream,
                id_type=id_type,
                mapping_types=MappingType.INTERSECT_COMBINATION_MAPPING_TYPE,
                time_ranges=self.time_ranges,
                target_date='{{ ds }}',
                jar_path=jar_path,
                emr_release_label=spark_3_emr_release_label,
                timeout=6,
                task_id_suffix=f"-{name}",
                additional_config=[("combinationListType", combination_list_type), ("combinationListName", name), ("partitioning", name)]
            ) for name in [
                MappingType.INTERSECT_COMBINATION_LIST_NAME, MappingType.INTERSECT_COMBINATION_LIST_NAME +
                MappingType.AUDIENCE_SUFFIX, MappingType.INTERSECT_COMBINATION_LIST_NAME +
                MappingType.PARTIAL_SUFFIX, MappingType.INTERSECT_COMBINATION_LIST_NAME + MappingType.PARTIAL_SUFFIX +
                MappingType.AUDIENCE_SUFFIX
            ]
        ]

        final_dummy = OpTask(op=DummyOperator(task_id=f"final_dummy_{id_type}", dag=dag))

        truth_maps_generate_branch = OpTask(
            op=BranchPythonOperator(
                task_id=f"generate_truth_maps_{id_type}_{combination_list_type}",
                python_callable=generate_truth_maps_branch,
                op_kwargs={
                    "tmap_gen_clusters": tmap_cluster_tasks,
                    "final_dummy": final_dummy
                },
                provide_context=True,
                dag=dag
            )
        )
        combination_generation_and_mapping_cluster_task >> truth_maps_generate_branch
        for tmap_cluster_task in tmap_cluster_tasks:
            truth_maps_generate_branch >> tmap_cluster_task >> final_dummy

    def _generate_union_combinations_truth_maps_dag(
        self, id_type: str, combination_list_name: str, combination_list_type: str, dag: DAG,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes, core_fleet_instance_type_configs: EmrFleetInstanceTypes,
        standard_cluster_tags: dict, wait_for_avails_data_ready: OpTask, generate_truth_maps_branch: Callable[..., Union[List[str], str]]
    ) -> None:
        combination_generation_and_mapping_cluster_task = self._get_combination_generation_and_mapping_cluster_task(
            id_type, combination_list_type, master_fleet_instance_type_configs, core_fleet_instance_type_configs, standard_cluster_tags
        )

        avail_sourced_random_union_combination_generation_config = get_base_truth_map_combination_generation_config(
            self.avail_sampling_rate, self.avail_stream, combination_list_name, id_type
        )

        truth_map_combination_generation_task = EmrJobTask(
            cluster_specs=combination_generation_and_mapping_cluster_task.cluster_specs,
            name=f"{TaskName.COMBINATION_GENERATION_TASK_NAME}-{id_type}-{combination_list_type}",
            class_name="com.thetradedesk.etlforecastjobs.universalforecasting.truthmap.combinationgeneration.CombinationListGenerationJob",
            executable_path=jar_path,
            eldorado_config_option_pairs_list=avail_sourced_random_union_combination_generation_config,
            additional_args_option_pairs_list=[("conf", "spark.kryoserializer.buffer.max=512m")],
            configure_cluster_automatically=True
        )

        truth_map_mapping_config = get_base_truth_map_mapping_config(
            self.avail_stream, combination_list_name, combination_list_type, MappingType.UNION_COMBINATION_MAPPING_TYPE, id_type
        )

        union_combination_dimensions = ["vector", "scalar", "privateContractId", "combined"]
        mapping_tasks = []
        for union_combination_dimension in union_combination_dimensions:
            truth_map_mapping_config_union = truth_map_mapping_config + [("unionCombinationDimension", union_combination_dimension)]
            combinations_id_mapping_task = EmrJobTask(
                cluster_specs=combination_generation_and_mapping_cluster_task.cluster_specs,
                name=f"{TaskName.ID_MAPPING_TASK_NAME}-{id_type}-{combination_list_type}-{union_combination_dimension}",
                class_name="com.thetradedesk.etlforecastjobs.universalforecasting.frequencymap.generation.MappingGeneratingJob",
                executable_path=jar_path,
                eldorado_config_option_pairs_list=truth_map_mapping_config_union,
                # big executors to avoid OOM errors
                additional_args_option_pairs_list=[("executor-memory", "211G"), ("driver-memory", "211G"),
                                                   ("conf", "spark.executor.memoryOverhead=21606m"),
                                                   ("conf", "spark.driver.memoryOverhead=21606m"),
                                                   ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                                   ("conf", "spark.driver.maxResultSize=211G"),
                                                   ("conf", "spark.sql.shuffle.partitions=12000"), ("conf", "spark.yarn.maxAppAttempts=1")],
                timeout_timedelta=timedelta(hours=6)
            )
            mapping_tasks.append(combinations_id_mapping_task)

        combination_generation_and_mapping_cluster_task.add_parallel_body_task(
            ChainOfTasks(
                f"{TaskName.COMBINATION_GENERATION_TASK_NAME}_{TaskName.ID_MAPPING_TASK_NAME}_{id_type}_{combination_list_type}",
                [truth_map_combination_generation_task] + mapping_tasks
            )
        )

        wait_for_avails_data_ready >> combination_generation_and_mapping_cluster_task

        ###########################################
        # Generating truth maps from aggregated mappings
        generate_union_combination_truth_maps_cluster_task = get_generate_frequency_map_cluster(
            map_type=MapType.TruthMap,
            avail_stream=self.avail_stream,
            id_type=id_type,
            mapping_types=MappingType.UNION_COMBINATION_MAPPING_TYPE,
            time_ranges=self.time_ranges,
            target_date='{{ ds }}',
            jar_path=jar_path,
            emr_release_label=spark_3_emr_release_label,
            timeout=6,
            task_id_suffix=f"-{combination_list_name}",
            additional_config=[("combinationListType", combination_list_type), ("combinationListName", combination_list_name),
                               ("unionCombinationDimension", "combined")],
            cluster_settings=LARGE_FMAP_CLUSTER_SETTINGS,
        )

        final_dummy = OpTask(op=DummyOperator(task_id=f"final_dummy_{id_type}_{combination_list_type}", dag=dag))

        truth_maps_generate_branch = OpTask(
            op=BranchPythonOperator(
                task_id=f"generate_truth_maps_{id_type}_{combination_list_type}",
                python_callable=generate_truth_maps_branch,
                op_kwargs={
                    "tmap_gen_clusters": [generate_union_combination_truth_maps_cluster_task],
                    "final_dummy": final_dummy
                },
                provide_context=True,
                dag=dag
            )
        )
        combination_generation_and_mapping_cluster_task >> truth_maps_generate_branch
        truth_maps_generate_branch >> generate_union_combination_truth_maps_cluster_task >> final_dummy
