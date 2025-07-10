from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from dags.idnt.feature_flags import FeatureFlags
from dags.idnt.identity_clusters import ComputeType, IdentityClusters
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables, RunTimes, Tags, Directories
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from datasources.sources.xdgraph_datasources import XdGraphDatasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

job_schedule_interval = '0 23 * * SUN'
job_start_date = datetime(2024, 9, 15)
job_name = 'iav2graph-generator'

run_time = RunTimes.previous_full_day
date_to_run = RunTimes.previous_full_day

write_env: str = TtdEnvFactory.get_from_system().dataset_write_env

num_cores = 50 * 96

dag = DagHelpers.identity_dag(dag_id=job_name, schedule_interval=job_schedule_interval, start_date=job_start_date, run_only_latest=True)

iav2graph_generation_dag: DAG = dag.airflow_dag

check_overall_ids_input = DagHelpers.check_datasets([IdentityDatasets.avails_id_overall_agg, IdentityDatasets.original_ids_overall_agg])
check_user_segments_input = DagHelpers.check_datasets_with_lookback(
    dataset_name="user_segments",
    datasets=[IdentityDatasets.user_segments_combined],
    poke_minutes=15,
    timeout_hours=4,
    lookback_days=7  # Look back to see if there is data available within the past week
)

shared_dataset_checks_complete = OpTask(op=EmptyOperator(task_id="shared_datasets_complete", dag=dag.airflow_dag))

dag >> check_overall_ids_input >> shared_dataset_checks_complete
dag >> check_user_segments_input >> shared_dataset_checks_complete

# TODO move the dataset checks out here, connect the checks to the dataset input

default_iav2_graph_name = 'iav2graph'
default_raw_adbrain_name = 'adbrain_legacy'

legacy_iav2_graph_name = "iav2graph_legacy"


def iav2_generator_cluster(
    graph_name: str,
    output_graph_name: str,
    cluster_name: str,
    iav2_generator_extra_eldorado_config_option_pairs_list: Optional[List[Tuple[str, str]]] = None,
) -> EmrClusterTask:
    graph_generator_cluster = IdentityClusters.get_cluster(cluster_name, dag, num_cores, ComputeType.STORAGE, cpu_bounds=(64, 2048))

    if (extra_configs := iav2_generator_extra_eldorado_config_option_pairs_list) is None:
        extra_configs = []

    graph_generator_cluster.add_sequential_body_task(
        EmrJobTask(
            name=f"generate_{graph_name}",
            class_name='jobs.identity.alliance.v2.generator.IAv2GraphGenerator',
            eldorado_config_option_pairs_list=[
                ('runDate', run_time),
                ('GraphOutputPath', output_graph_name),
                ('segmentDataLookbackDays', 3),
            ] + extra_configs,
            timeout_timedelta=timedelta(hours=6),
            executable_path=Executables.identity_repo_executable,
            configure_cluster_automatically=False,
        )
    )

    return graph_generator_cluster


# adbrain_legacy, both pre-etl and post-etl stay in the same path after og release
# since they are only for iav2, no need to change their paths at all
generate_iav2_based_on_ttdgraph_v1 = iav2_generator_cluster(
    graph_name=legacy_iav2_graph_name, output_graph_name=legacy_iav2_graph_name, cluster_name="generate_iav2_based_on_ttdgraph_v1"
)

# modify this to branch on inputs
generate_iav2_based_on_ttdgraph_v2 = iav2_generator_cluster(
    graph_name=default_iav2_graph_name,
    output_graph_name="og-iav2graph",
    iav2_generator_extra_eldorado_config_option_pairs_list=[("EnableReplacingAdbrainWithOpenGraph", "true")],
    cluster_name="generate_iav2_based_on_ttdgraph_v2"
)

if FeatureFlags.enable_cookieless_graph_generation:
    cookieless_iav2_graph_name = f"cookieless{default_iav2_graph_name}"
    cookieless_iav2_cluster = iav2_generator_cluster(
        graph_name=cookieless_iav2_graph_name,
        output_graph_name=cookieless_iav2_graph_name,
        iav2_generator_extra_eldorado_config_option_pairs_list=[("EnableThirdPartyCookiesDeprecationSimulation", "true")],
        cluster_name="generate_iav2_based_on_cookieless_ttdgraph"
    )

wait_for_ttgraph_v1 = DagHelpers.check_datasets(datasets=[IdentityDatasets.get_post_etl_graph(default_raw_adbrain_name)], timeout_hours=12)
wait_for_ttgraph_v2 = DagHelpers.check_datasets(datasets=[IdentityDatasets.get_post_etl_graph("nextgen")], timeout_hours=12)

shared_dataset_checks_complete >> wait_for_ttgraph_v1 >> generate_iav2_based_on_ttdgraph_v1
shared_dataset_checks_complete >> wait_for_ttgraph_v2 >> generate_iav2_based_on_ttdgraph_v2

if FeatureFlags.enable_cookieless_graph_generation:
    wait_for_cookieless = DagHelpers.check_datasets(
        datasets=[IdentityDatasets.get_post_etl_graph("cookielessnextgen4iav2")], timeout_hours=12
    )
    shared_dataset_checks_complete >> wait_for_cookieless >> cookieless_iav2_cluster

# add legacy ids to prod iav2 from legacy_iav2
add_alternative_ids_cluster = IdentityClusters.get_cluster("add_alternative_ids_cluster", dag, num_cores)
add_alternative_ids_cluster.add_sequential_body_task(
    IdentityClusters.task(
        class_name="com.thetradedesk.idnt.identity.pipelines.IdentityAllianceRollout",
        eldorado_configs=[
            ("graphs.opengraph.IdentityAllianceRollout.iaWithOgPath", f"s3://ttd-identity/data/prod/graph/{str(write_env)}/og-iav2graph/"),
            ("graphs.opengraph.IdentityAllianceRollout.iaPath", f"s3://ttd-identity/data/prod/graph/{str(write_env)}/iav2graph_legacy/ "),
            (
                "graphs.opengraph.IdentityAllianceRollout.mergedIAOutputPath",
                f"s3://ttd-identity/data/prod/graph/{str(write_env)}/iav2graph"
            ),
        ],
    )
)

generate_iav2_based_on_ttdgraph_v1 >> add_alternative_ids_cluster
generate_iav2_based_on_ttdgraph_v2 >> add_alternative_ids_cluster


# identity alliance etl
# this is a bit ugly now and could use some cleaning up, but moving it here to reduce the number of dags
class IdentityAllianceETL:
    """identityAllianceETL
    usage of graph_name
    - as part of task id to differentiate tasks on each graph in etl jobs
    - input to dat pipeline, and will be appended to dat output path
    - output folder name for graph in sxd-etl/universal/pre-dat-graphs/ and sxd-etl/universal/, aka post-dat-graphs
    - output folder name for graph in metrics path and score norm path

    usage of vendor_name
    - think it as a short name of graph_name, just used in some special cases (when people chose to use a short name)
    - most of the time it"s same as graph_name, iav2 vs iav2graph is the main discrepancy
    - as part of task id to differentiate tasks on each graph in dat pipeline
    - output folder for dat pipeline
    - as part of lookup_table full name

    usage of vertica_graph_name
    - output folder name for universal-to-vertica-format step in thetradedesk-useast-data-import/
    - graph loader monitors on that path and grab the most recent graph to load to vertica and data server
    - most of the time it"s same as graph_name
    - for cookieless iav2, since it"s for internal test, we use an abnormal name to indicate that

    usage of vertica_vendor_name
    - was assigned independently, so it may be different from vertica_graph_name
    - see UniversalToVerticaFormat.vendorMap to get the vertica_vendor_name

    please note that the config below may mean different thing
    - etl (VENDOR_NAME): the name of graph parser, should always be "iav2"
    - lookup_table_gen (DATA_TO_BE_SCORED_NAMES): used as part of graph input path, usually graph_name
    - person_score_normalization (VENDOR_NAME): used as part of graph input path, usually graph_name
    - vertica_format_person (VENDOR_NAME): vertica_vendor_name for person graph
    - vertica_format_household (VENDOR_NAME): vertica_vendor_name for household graph
    """
    data_import_bucket = "thetradedesk-useast-data-import"

    pre_etl_dir = "s3://ttd-identity/data/prod/graph/prod"
    ttd_env = Tags.environment()
    test_folder = Directories.test_dir_no_prod()

    raw_etl_output = f"s3://{data_import_bucket}/sxd-etl{test_folder}/universal/raw-etl-output"

    # store the etled graph before dat generation
    graph_etl_base_path_pre_dat = f"s3://{data_import_bucket}/sxd-etl{test_folder}/universal/pre-dat-graphs"
    graph_etl_base_path = f"s3://{data_import_bucket}/sxd-etl{test_folder}/universal"
    metrics_path = f"s3://{data_import_bucket}/sxd-etl{test_folder}/metrics/"

    dat_env = "prod" if str(ttd_env) == "prod" else "test"
    dat_output_path = f"s3://ttd-identity/datapipeline/{dat_env}/dat"

    uts_path = f"s3://{data_import_bucket}/sxd-etl/deterministic/uts_graph/devices/v=1"

    # constants
    prod_iav2_graph_name = "iav2graph"
    prod_iav2_vendor_name = "iav2"
    prod_iav2_household_graph_name = "iav2graph_household"
    iav2_parser_name = "iav2"
    legacy_iav2_graph_name = "iav2graph_legacy"

    @classmethod
    def _get_etl_cluster(
        cls,
        graph_name: str,
        household_graph_name: str,
        output_path: str,
    ) -> EmrJobTask:
        etl_cluster = IdentityClusters.get_cluster(f"{graph_name}-etl_cluster", dag, num_cores, ComputeType.STORAGE, use_delta=False)

        num_partition = "1200" if graph_name == "iav2graph" else "800"
        etl_cluster.add_parallel_body_task(
            EmrJobTask(
                name="etl",
                class_name=Executables.etl_driver_class,
                eldorado_config_option_pairs_list=[
                    ("config.resource", "application.conf"),
                    ("LOCAL", "false"),
                    ("INPUT_PATH", f"{cls.pre_etl_dir}/{graph_name}"),
                    ("OUTPUT_PATH", f"{output_path}/{graph_name}"),
                    ("HOUSEHOLD_OUTPUT_PATH", f"{output_path}/{household_graph_name}"),
                    ("NUM_PARTITIONS", num_partition),
                    ("SAMPLE_RATE", "1.0"),
                    ("MAX_DEVICES_PER_GROUP", "60"),
                    ("MAX_DEVICES_PER_GROUP_HOUSEHOLD", "80"),
                    ("METRICS_PATH", cls.metrics_path),
                    ("GATEWAY_ADDRESS", "prom-push-gateway.adsrvr.org:80"),
                    # parser name in etl driver
                    ("VENDOR_NAME", cls.iav2_parser_name),
                    ("runDate", RunTimes.previous_full_day)
                ],
                timeout_timedelta=timedelta(hours=4),
                executable_path=Executables.etl_repo_executable,
                configure_cluster_automatically=False
            )
        )
        return etl_cluster

    @classmethod
    def _get_vertica_format_cluster(
        cls,
        graph_name: str,
        household_graph_name: str,
        vertica_graph_name: str,
        vertica_vendor_name: str,
        skip_vertica_household_format_step: bool = False,
        gen_partial_output: bool = False
    ) -> EmrJobTask:

        # vertica format step expects graph to be already normalized, which both the IAv2 Person/HH graphs are
        score_norm_vertica_format_cluster = IdentityClusters.get_cluster(
            f"{graph_name}-vertica-format_cluster", dag, num_cores, ComputeType.STORAGE, use_delta=False
        )

        extra_vertica_format_person_config_option_pairs = []

        extra_vertica_format_person_config_option_pairs.extend([("USE_INPUT_SUB_FOLDER", "true"), ("INPUT_SUB_FOLDER", "success")])

        if gen_partial_output:
            extra_vertica_format_person_config_option_pairs.append(
                ('PARTIAL_OUTPUT_PATH', f's3://{cls.data_import_bucket}/{vertica_graph_name}{cls.test_folder}/universal/coldstorage_load')
            )
        score_norm_vertica_format_cluster.add_sequential_body_task(
            EmrJobTask(
                name=f"run_vertica_format_person_{graph_name}",
                class_name="com.thetradedesk.etl.logformats.UniversalToVerticaFormat",
                eldorado_config_option_pairs_list=[
                    ("config.resource", "application.conf"), ("LOCAL", "false"), ("INPUT_PATH", f"{cls.graph_etl_base_path}/{graph_name}"),
                    ("OUTPUT_PATH", f"s3://{cls.data_import_bucket}/{vertica_graph_name}{cls.test_folder}/universal"),
                    ("STATS_PATH", f"s3://{cls.data_import_bucket}/sxd-etl{cls.test_folder}/toVerticaFormat/{vertica_graph_name}"),
                    ("NUM_PARTITIONS", "1000"), ("VENDOR_NAME", vertica_vendor_name), ("RAW_ID_MAPPING_TABLE_VENDORS", vertica_vendor_name)
                ] + extra_vertica_format_person_config_option_pairs,
                timeout_timedelta=timedelta(hours=1),
                executable_path=Executables.etl_repo_executable,
                configure_cluster_automatically=False,
            )
        )

        if not skip_vertica_household_format_step:
            extra_vertica_format_household_config_option_pairs = []
            if gen_partial_output:
                extra_vertica_format_household_config_option_pairs.append((
                    'PARTIAL_OUTPUT_PATH',
                    f's3://{cls.data_import_bucket}/{household_graph_name}{cls.test_folder}/universal/coldstorage_load'
                ))

            vertica_vendor_household_name = f"{vertica_vendor_name}hh" if vertica_graph_name != "iav2graph_legacy" else "iav2hhlegacy"
            score_norm_vertica_format_cluster.add_sequential_body_task(
                EmrJobTask(
                    name=f"run_vertica_format_household_{graph_name}",
                    class_name="com.thetradedesk.etl.logformats.UniversalToVerticaFormat",
                    eldorado_config_option_pairs_list=[
                        ("config.resource", "application.conf"), ("LOCAL", "false"),
                        ("INPUT_PATH", f"{cls.graph_etl_base_path}/{household_graph_name}"),
                        ("OUTPUT_PATH", f"s3://{cls.data_import_bucket}/{household_graph_name}{cls.test_folder}/universal"),
                        ("STATS_PATH", f"s3://{cls.data_import_bucket}/sxd-etl{cls.test_folder}/toVerticaFormat/{household_graph_name}"),
                        ("NUM_PARTITIONS", "1000"), ("USE_INPUT_SUB_FOLDER", "true"), ("INPUT_SUB_FOLDER", "success"),
                        ("GATEWAY_ADDRESS", "prom-push-gateway.adsrvr.org:80"), ("VENDOR_NAME", vertica_vendor_household_name),
                        ("RAW_ID_MAPPING_TABLE_VENDORS", vertica_vendor_household_name)
                    ] + extra_vertica_format_household_config_option_pairs,
                    timeout_timedelta=timedelta(hours=1),
                    executable_path=Executables.etl_repo_executable,
                    configure_cluster_automatically=False,
                )
            )

        return score_norm_vertica_format_cluster

    @staticmethod
    def graph_extensions_cluster(name: str):
        return IdentityClusters.get_cluster(name, dag, num_cores, ComputeType.STORAGE, instance_configuration_spark_log4j=None)

    @staticmethod
    def copy_data(dataset_name: str):
        dataset = XdGraphDatasources.xdGraph(dataset_name)

        return DatasetTransferTask(
            name=f"{dataset.data_name}_copy_to_azure",
            dataset=dataset,
            src_cloud_provider=CloudProviders.aws,
            dst_cloud_provider=CloudProviders.azure,
            partitioning_args=dataset.get_partitioning_args(ds_date=date_to_run),
            transfer_timeout=timedelta(hours=6),
            max_threads=5,
            drop_dst=True  # delete dst data before copying
        )

    @classmethod
    def add_etl_steps(
        cls,
        previous_step: OpTask,
        graph_name: str,
        household_graph_name: str,
        vendor_name: str,
        vertica_graph_name: str,
        vertica_vendor_name: str,
        etl_cluster: EmrJobTask,
        skip_dat: bool = False,
        skip_vertica_formatting: bool = False,
        skip_vertica_household_format_step: bool = False,
        skip_copy_to_azure: bool = False,
        skip_metrics: bool = True,
        extra_default_eldorado_config_option_pairs_list: Optional[List[Tuple[str, str]]] = None
    ):

        if extra_default_eldorado_config_option_pairs_list is None:
            extra_default_eldorado_config_option_pairs_list = []

        graph_pre_etl = f"s3://ttd-identity/data/prod/graph/prod/{graph_name}"
        graph_pre_etl_w_date = f"{graph_pre_etl}/{date_to_run}"

        # dat extension
        countries_to_run = "US,MX,CA,ID,AU,GB,DE,ES,IN,FR,IT,JP,HK,PH,TW,BE,NZ,TR,VN"
        dat_output_path_for_graph = f"{cls.dat_output_path}/{vendor_name}"
        person_graph_input_path = f"{cls.graph_etl_base_path_pre_dat}/{graph_name}/{date_to_run}/success"
        household_graph_input_path = f"{cls.graph_etl_base_path_pre_dat}/{household_graph_name}/{date_to_run}/success"
        dat_eldorado_option_pairs_list = [
            ("countries", countries_to_run),
            ("graphVendor", vendor_name),
            ("outputPath", dat_output_path_for_graph),
            ("idAvailsBasePath", "s3://ttd-identity/data/prod/events/avails-pipeline-daily-agg/idfull/v=3/date="),
            ("idlessAvailsBasePath", "s3://ttd-identity/data/prod/events/avails-pipeline-daily-agg/idless/v=3/date="),
            ("graphProdPath", graph_pre_etl_w_date),
            ("finalGraphProdPath", person_graph_input_path),
            ("finalHouseholdGraphProdPath", household_graph_input_path),
        ] + extra_default_eldorado_config_option_pairs_list

        def get_dat_task(
            task_name: str,
            task_class_name: str,
            additional_eldorado_args: List[Tuple[str, str]] = [],
            additional_args_option_pairs: List[Tuple[str, str]] = [],
            timeout_hours: int = 4
        ) -> EmrJobTask:
            delta_option_pairs_list = [
                ("packages", "io.delta:delta-core_2.12:2.2.0"),
            ]
            dat_task = IdentityClusters.task(
                class_name=f"com.thetradedesk.ds.libs.graphextension.{task_class_name}",
                runDate_arg="date",
                eldorado_configs=dat_eldorado_option_pairs_list + additional_eldorado_args,
                extra_args=delta_option_pairs_list + additional_args_option_pairs,
                timeout_hours=timeout_hours,
                executable_path=Executables.dat_executable,
            )
            dat_task.name = task_name
            return dat_task

        # QUESTION(bence.komarniczky): why do we need 3 clusters for this?
        # IP and Value
        ip_and_value_dat_cluster = cls.graph_extensions_cluster(f"{graph_name}-ip_and_value_dat_cluster")
        ip_and_value_dat_cluster.add_parallel_body_task(
            get_dat_task(
                task_name=f"ip_and_value-run_data_preparation_{vendor_name}", task_class_name="IpChurnDataPreparation", timeout_hours=5
            )
        )
        ip_and_value_dat_cluster.add_sequential_body_task(
            get_dat_task(
                task_name=f"ip_and_value-run_model_training_{vendor_name}",
                task_class_name="IpChurnModelTraining",
                additional_args_option_pairs=[
                    ("conf", "spark.dynamicAllocation.enabled=false")
                    # disable dynamicAllocation since model training option .setUseBarrierExecutionMode(true) doesn"t support it
                ],
                timeout_hours=5
            )
        )
        ip_and_value_dat_cluster.add_sequential_body_task(
            get_dat_task(task_name=f"ip_and_value-run_value_calculator_{vendor_name}", task_class_name="ValueCalculator", timeout_hours=6)
        )

        # Predictor
        predictor_dat_cluster = cls.graph_extensions_cluster(f"{graph_name}-predictor_dat_cluster")
        predictor_dat_cluster.add_parallel_body_task(
            get_dat_task(
                task_name=f"predictor-run_person_mapping_{vendor_name}", task_class_name="AvailsPersonPredictor", timeout_hours=10
            )
        )
        predictor_dat_cluster.add_sequential_body_task(
            get_dat_task(
                task_name=f"predictor-run_single_device_predictor_{vendor_name}", task_class_name="SingleDevicePredictor", timeout_hours=6
            )
        )

        # Finalizer
        finalizer_dat_cluster = cls.graph_extensions_cluster(f"{graph_name}-finalize_dat_cluster")
        finalizer_dat_cluster.add_parallel_body_task(
            get_dat_task(task_name=f"run_graph_augmentation_{vendor_name}", task_class_name="Finalizer", timeout_hours=5)
        )
        finalizer_dat_cluster.add_sequential_body_task(
            get_dat_task(task_name=f"run_stats_calculator_{vendor_name}", task_class_name="StatsCalculator", timeout_hours=2)
        )
        person_graph_with_dat_final_path = f"{cls.graph_etl_base_path}/{graph_name}/{date_to_run}/success"
        household_graph_with_dat_final_path = f"{cls.graph_etl_base_path}/{household_graph_name}/{date_to_run}/success"
        finalizer_dat_cluster.add_sequential_body_task(
            get_dat_task(
                task_name=f"run_sanity_check_{vendor_name}",
                task_class_name="SanityCheck",
                additional_eldorado_args=[("graphWithDATOutputPath", person_graph_with_dat_final_path),
                                          ("householdGraphWithDATOutputPath", household_graph_with_dat_final_path)],
                timeout_hours=3
            )
        )

        # Convert to Vertica format
        score_norm_vertica_format_cluster = cls._get_vertica_format_cluster(
            graph_name, household_graph_name, vertica_graph_name, vertica_vendor_name, skip_vertica_household_format_step
        )

        previous_step >> etl_cluster
        tail_task: EmrClusterTask = etl_cluster

        if not skip_dat:
            tail_task >> ip_and_value_dat_cluster
            tail_task >> predictor_dat_cluster

            ip_and_value_dat_cluster >> finalizer_dat_cluster
            predictor_dat_cluster >> finalizer_dat_cluster

            tail_task = finalizer_dat_cluster

        if not skip_metrics:
            check_bidfeedback_dataset = DagHelpers.check_datasets([RtbDatalakeDatasource.rtb_bidfeedback_v5.with_check_type('day')])
            uniqueness_score_cluster = IdentityClusters.get_cluster(
                "UniquenessMetricCluster", dag, 10000, ComputeType.STORAGE, cpu_bounds=(64, 2048)
            )
            uniqueness_score_cluster.add_sequential_body_task(
                IdentityClusters
                .task("jobs.identity.alliance.v2.GraphMetrics.UniquenessMetric", executable_path=Executables.identity_repo_executable)
            )
            # we don't overwrite tail task here since we can copy and run metrics/vertica at the same time
            tail_task >> check_bidfeedback_dataset >> uniqueness_score_cluster

        if not skip_vertica_formatting:
            tail_task >> score_norm_vertica_format_cluster
            tail_task = score_norm_vertica_format_cluster

        if not skip_copy_to_azure and str(cls.ttd_env) == "prod":
            tail_task >> cls.copy_data(graph_name)
            tail_task >> cls.copy_data(household_graph_name)

    @classmethod
    def add_etl_iav2_based_on_ttdgraph_v2(cls, previous_step: OpTask):
        og_iav2_etl_cluster = cls._get_etl_cluster(
            cls.prod_iav2_graph_name, cls.prod_iav2_household_graph_name, cls.graph_etl_base_path_pre_dat
        )
        cls.add_etl_steps(
            previous_step=previous_step,
            graph_name=cls.prod_iav2_graph_name,
            household_graph_name=cls.prod_iav2_household_graph_name,
            vendor_name=cls.prod_iav2_vendor_name,
            vertica_graph_name=cls.prod_iav2_graph_name,
            vertica_vendor_name=cls.prod_iav2_vendor_name,
            etl_cluster=og_iav2_etl_cluster,
            skip_copy_to_azure=False,
            skip_metrics=False,
        )

        return og_iav2_etl_cluster

    @classmethod
    def add_etl_iav2_based_on_ttdgraph_v2_cookieless(cls, previous_step: OpTask):
        if FeatureFlags.enable_cookieless_graph_generation:
            # cookieless iav2 for internal testing
            # no vertica household graph
            # copy to azure
            cookieless_iav2_graph_name = "cookielessiav2graph"
            cookieless_iav2_household_graph_name = "cookielessiav2graph_household"
            cookieless_iav2_vendor_name = "cookielessiav2"
            # Temporary store the cookieless graph in a location that TaskService doesn"t watch.
            cookieless_iav2_vertica_graph_name = "internaltestgraph_noupload"
            # The Vertica formatting application maps this name to the cross device vendor ID
            # that is used by all downstream components such as Bidder, TTD UI, attribution
            # in Vertica etc.
            cookieless_iav2_vertica_vendor_name = "internaltest"
            cookieless_iav2_etl_cluster = cls._get_etl_cluster(
                cookieless_iav2_graph_name, cookieless_iav2_household_graph_name, cls.graph_etl_base_path_pre_dat
            )
            cls.add_etl_steps(
                previous_step=previous_step,
                graph_name=cookieless_iav2_graph_name,
                household_graph_name=cookieless_iav2_household_graph_name,
                vendor_name=cookieless_iav2_vendor_name,
                vertica_graph_name=cookieless_iav2_vertica_graph_name,
                vertica_vendor_name=cookieless_iav2_vertica_vendor_name,
                etl_cluster=cookieless_iav2_etl_cluster,
                skip_vertica_household_format_step=True,
                extra_default_eldorado_config_option_pairs_list=[("enableThirdPartyCookiesDeprecationSimulation", "true")]
            )

    @classmethod
    def add_etl_iav2_based_on_ttdgraph_v1(cls, previous_step: OpTask, iav2_etl_based_on_v2: OpTask):
        legacy_iav2_household_graph_name = "iav2graph_household_legacy"
        legacy_iav2_vertica_vendor_name = "iav2legacy"
        legacy_iav2_etl_cluster = cls._get_etl_cluster(
            graph_name=legacy_iav2_graph_name, household_graph_name=legacy_iav2_household_graph_name, output_path=cls.raw_etl_output
        )

        legacy_iav2_alternative_id_cluster = IdentityClusters.set_step_concurrency(
            IdentityClusters.get_cluster("iav2graph_legacy-add_alternative_ids_cluster", dag, 3500)
        )

        alternative_id_person_task = IdentityClusters.task(
            class_name="com.thetradedesk.idnt.identity.pipelines.LegacyIdentityAllianceRollout",
            eldorado_configs=[
                ("graphs.opengraph.LegacyIdentityAllianceRollout.legacyIAPath", f"{cls.raw_etl_output}/{legacy_iav2_graph_name}/"),
                (
                    "graphs.opengraph.LegacyIdentityAllianceRollout.ogIAPath",
                    f"{cls.graph_etl_base_path_pre_dat}/{cls.prod_iav2_graph_name}/"
                ),
                (
                    "graphs.opengraph.LegacyIdentityAllianceRollout.mergedIAOutputPath",
                    f"{cls.graph_etl_base_path}/{legacy_iav2_graph_name}"
                ),
            ],
            timeout_hours=2,
            action_on_failure="CONTINUE",
            task_name_suffix="person_graph"
        )
        legacy_iav2_alternative_id_cluster.add_parallel_body_task(alternative_id_person_task)

        alternative_id_household_task = IdentityClusters.task(
            class_name="com.thetradedesk.idnt.identity.pipelines.LegacyIdentityAllianceRollout",
            eldorado_configs=[
                (
                    "graphs.opengraph.LegacyIdentityAllianceRollout.legacyIAPath",
                    f"{cls.raw_etl_output}/{legacy_iav2_household_graph_name}/"
                ),
                (
                    "graphs.opengraph.LegacyIdentityAllianceRollout.ogIAPath",
                    f"{cls.graph_etl_base_path_pre_dat}/{cls.prod_iav2_household_graph_name}/"
                ),
                (
                    "graphs.opengraph.LegacyIdentityAllianceRollout.mergedIAOutputPath",
                    f"{cls.graph_etl_base_path}/{legacy_iav2_household_graph_name}"
                ),
                (
                    "graphs.opengraph.LegacyIdentityAllianceRollout.mergedIADeltaOutputPath",
                    "s3://ttd-identity/deltaData/openGraph/mergedLegacyIav2HHGraph/rawDeltaOut"
                ),
            ],
            timeout_hours=2,
            action_on_failure="CONTINUE",
            task_name_suffix="household_graph"
        )
        legacy_iav2_alternative_id_cluster.add_parallel_body_task(alternative_id_household_task)

        legacy_iav2_score_norm_vertica_cluster = cls._get_vertica_format_cluster(
            graph_name=legacy_iav2_graph_name,
            household_graph_name=legacy_iav2_household_graph_name,
            vertica_graph_name=legacy_iav2_graph_name,
            vertica_vendor_name=legacy_iav2_vertica_vendor_name,
            skip_vertica_household_format_step=False,
            gen_partial_output=True
        )

        previous_step >> legacy_iav2_etl_cluster >> legacy_iav2_alternative_id_cluster
        iav2_etl_based_on_v2 >> legacy_iav2_alternative_id_cluster
        legacy_iav2_alternative_id_cluster >> legacy_iav2_score_norm_vertica_cluster


etl_v2_step = IdentityAllianceETL.add_etl_iav2_based_on_ttdgraph_v2(add_alternative_ids_cluster)
IdentityAllianceETL.add_etl_iav2_based_on_ttdgraph_v2_cookieless(add_alternative_ids_cluster)
IdentityAllianceETL.add_etl_iav2_based_on_ttdgraph_v1(generate_iav2_based_on_ttdgraph_v1, etl_v2_step)


def get_uid2_graph_stats_cluster() -> EmrClusterTask:
    """UID2 Graph Stats

    Weekly job to run per-country calculations for each graph's UID2 stats:
    - Total UID2 count
    - Persons or households connected to UID2, depending on graph group type
    - Devices connected to UID2
    """
    cluster = IdentityClusters.get_cluster("Uid2GraphStats", dag, 384)
    cluster.add_sequential_body_task(IdentityClusters.task("jobs.identity.uid2.stats.Uid2GraphStats"))

    return cluster


etl_v2_step >> get_uid2_graph_stats_cluster()
