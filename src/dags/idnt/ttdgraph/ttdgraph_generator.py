from airflow import DAG

from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.statics import Executables, Geos, Tags, RunTimes
from datetime import datetime, timedelta
from ttd.el_dorado.v2.emr import EmrJobTask, EmrClusterTask
from typing import List, Tuple
from ttd.tasks.op import OpTask
from ttd.spark import ParallelTasks

num_cores = 192 * 96
opengraph_publish_num_cores = 192 * 120

dag = DagHelpers.identity_dag(
    dag_id="ttdgraph-generator",
    start_date=datetime(2025, 3, 20),
    schedule_interval="0 14 * * SUN",
)

check_bidstream = DagHelpers.check_datasets(
    # this is 0 based so we check for the past 7 days
    lookback=6,
    datasets=[
        # These are "Identity Avails V2" and can be removed after the last two dependencies are moved to V3
        IdentityDatasets.avails_daily_agg_v3,
        IdentityDatasets.avails_idless_daily_agg,
        # These are the "Identity Avails V3" based on Unsampled Avails
        IdentityDatasets.avails_v3_daily_agg,
        IdentityDatasets.avails_v3_daily_agg_idless,
        IdentityDatasets.daily_bidrequest,
        IdentityDatasets.daily_bidfeedback
    ],
)

daily_processing_cluster = IdentityClusters.get_cluster("ttdgraph_daily", dag, 7000)

for days_ago in range(7, 0, -1):
    suffix = f"lag{days_ago}"
    daily_processing_cluster.add_parallel_body_task(
        IdentityClusters.task(
            Executables.class_name("pipelines.OpenGraphDaily"),
            runDate=RunTimes.date_x_days_ago(days_ago),
            task_name_suffix=f"_lag{days_ago}"
        )
    )

weekly_inputs_cluster = IdentityClusters.get_cluster(
    "ttdgraph_weekly_input_cluster",
    dag,
    num_cores,
    ComputeType.ARM_GENERAL,
    ComputeType.ARM_GENERAL,
)

weekly_inputs_cluster.add_sequential_body_task(
    IdentityClusters.task(
        Executables.class_name("pipelines.OpenGraphWeeklyInputsPipeline"),
        # this is to disable eldoardo optimisations - this is the default
        extra_args=[("conf", "spark.python.worker.reuse=true")]
    )
)

multi_week_cluster = IdentityClusters.get_cluster(
    "ttdgraph_multiweek_cluster",
    dag,
    num_cores,
    ComputeType.ARM_STORAGE_LARGE,
    ComputeType.ARM_GENERAL,
)

multi_week_cluster.add_sequential_body_task(
    IdentityClusters.task(Executables.class_name("pipelines.OpenGraphMultiWeekPipeline"), timeout_hours=10)
)

metadata_cluster = IdentityClusters.get_cluster(
    "ttgraph_v2_weekly_metadata", dag, num_cores, ComputeType.ARM_MEMORY, ComputeType.ARM_GENERAL
)

metadata_cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.OpenGraphMetaPipeline")))

edge_init_cluster = IdentityClusters.get_cluster("ttdgraph_v2_edge_init", dag, num_cores, ComputeType.ARM_MEMORY, ComputeType.ARM_GENERAL)
edge_init_cluster.add_sequential_body_task(IdentityClusters.task(Executables.class_name("pipelines.OpenGraphEdgeInit")))


def clustering_job(group_name: str, regions: List[str], num_cores: int, cluster_args: List[Tuple[str, str]] = []) -> EmrClusterTask:
    cluster = IdentityClusters.get_cluster(f"clustering_{group_name}", dag, num_cores, ComputeType.STORAGE, parallelism_multiplier=1.0)

    additional_args: List[Tuple[str, str]] = [
        # memory settings
        # TODO tune this if needed
        ("executor-cores", "7"),
        ("executor-memory", "50000m"),
        ("conf", "spark.driver.memory=80000m"),
        ("conf", "spark.driver.memoryOverhead=16000m"),
        ("conf", "spark.executor.cores=7"),
        ("conf", "spark.executor.memory=50000m"),
        ("conf", "spark.executor.memoryOverhead=8000m"),
        ("conf", "spark.sql.shuffle.partitions=8000"),
        # temporary change for 2025-05-10 run
        ("conf", "spark.databricks.delta.schema.autoMerge.enabled=true"),
    ]

    for region in regions:
        final_cluster_args = cluster_args.copy()
        final_cluster_args.append(("graphs.opengraph.regions", f"""'{region}'"""))
        safe_region = region.replace(" ", "_")

        new_task = EmrJobTask(
            name=f"{group_name}_{safe_region}",
            class_name=f"{Executables.identity_repo_class}.pipelines.OpenGraphClustering",
            eldorado_config_option_pairs_list=IdentityClusters.default_config_pairs(final_cluster_args),
            additional_args_option_pairs_list=additional_args,
            timeout_timedelta=timedelta(hours=5),
            action_on_failure="CONTINUE",
            executable_path=Executables.identity_repo_executable,
        )

        cluster.add_parallel_body_task(new_task)

    return IdentityClusters.set_step_concurrency(cluster)


# TODO add extra arguments for better performance
clustering_jobs = [clustering_job(region_key, regions, num_cores) for (region_key, regions) in Geos.ttdgraph_regions.items()]
regional_clustering = ParallelTasks("regional_clustering", clustering_jobs).as_taskgroup("regional_clustering_tasks")

graph_publish_cluster = IdentityClusters.get_cluster("graph_publish_cluster", dag, opengraph_publish_num_cores, ComputeType.STORAGE)
graph_publish_cluster.add_sequential_body_task(
    IdentityClusters.task(
        f"{Executables.identity_repo_class}.pipelines.OpenGraphPublish",
        extra_args=[
            # driver
            ('conf', 'spark.driver.cores=5'),
            ('conf', 'spark.driver.memory=42330m'),
            ('conf', 'spark.driver.memoryOverhead=4703m'),
            # executor - bigger executors really help us here
            ('conf', 'spark.executor.cores=5'),
            ('conf', 'spark.executor.memory=126990m'),
            ('conf', 'spark.executor.memoryOverhead=4703m'),
        ],
        timeout_hours=6
    )
)


def get_etl_load_cluster():

    test_folder = '' if Tags.environment() == 'prod' else '/test'
    universal_path = f"s3://thetradedesk-useast-data-import/sxd-etl{test_folder}/universal"
    etl_vendor_name = "nextgen"
    released_graph_etl_output = f"{universal_path}/{etl_vendor_name}"
    etl_cluster_cores = 8_000

    cluster = IdentityClusters.get_cluster(
        "etl_load_ttdgraph", dag, etl_cluster_cores, ComputeType.STORAGE, emr_release_label=Executables.emr_version_6
    )

    cluster.add_sequential_body_task(
        IdentityClusters.task(
            Executables.etl_driver_class,
            eldorado_configs=[
                ('VENDOR_NAME', "opengraph"),
                ('INPUT_PATH', "s3://ttd-identity/deltaData/openGraph/published/backwardsCompatibleFormat"),
                ('OUTPUT_PATH', released_graph_etl_output),
                ('HOUSEHOLD_OUTPUT_PATH', f"{released_graph_etl_output}_household"),
                ('NUM_PARTITIONS', '800'),
                ('METRICS_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/metrics/'),
                ('GATEWAY_ADDRESS', 'prom-push-gateway.adsrvr.org:80'),
                ('LOCAL', 'false'),
            ],
            executable_path=Executables.etl_repo_executable,
        )
    )

    # TODO this is using identity repo, but running on emr6.9 cluster, together with etl, hence needs spark 3.3
    cluster.add_sequential_body_task(
        IdentityClusters.task(
            "jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting",
            eldorado_configs=[("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.vendorName", "nextgen")],
            executable_path=Executables.identity_repo_executable_spark_3_3,
        )
    )

    cluster.add_sequential_body_task(
        IdentityClusters.task(
            "com.thetradedesk.etl.scoring.ScoreLookupTableDriver",
            runDate_arg="DATE",
            eldorado_configs=[
                ('NUM_PARTITIONS', '200'),
                ('LOCAL', 'false'),
                ('DATA_TO_BE_SCORED_BASE_INPUT_PATH', universal_path),
                ('DATA_TO_BE_SCORED_BASE_OUTPUT_PATH', universal_path),
                ('DATA_TO_BE_SCORED_NAMES', etl_vendor_name),
                ('BENCHMARK_DATA_PATHS', 's3://thetradedesk-useast-data-import/sxd-etl/deterministic/uts_graph/devices/v=1'),
                ('BENCHMARK_DATA_NAMES', 'uts'),
                ('FINAL_MERGED_CSV_PATH', f"s3://ttd-identity/lookupTable_{etl_vendor_name}{test_folder}/{RunTimes.previous_full_day}"),
                ('LOOKBACK_WINDOW', '60'),
                ('USE_PAIR_BASED_PRECISION', 'true'),
                ('USE_PERSON_DOWN_SAMPLE', 'false'),
                ('USE_UTS_INSTEAD_BENCHMARKS', 'true'),
                ('UTS_PATH', 's3://thetradedesk-useast-data-import/sxd-etl/deterministic/uts_graph/devices/v=1'),
            ],
            executable_path=Executables.etl_repo_executable,
        )
    )

    score_normalized_output_path = f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/scoreNormalized/{etl_vendor_name}'
    cluster.add_sequential_body_task(
        EmrJobTask(
            name="score_normalization_person",
            class_name="com.thetradedesk.etl.scoring.ScoreNormalizationDriver",
            eldorado_config_option_pairs_list=[('config.resource', 'application.conf'), ('INPUT_PATH', released_graph_etl_output),
                                               ('OUTPUT_PATH', score_normalized_output_path), ('LOCAL', 'false'),
                                               ('DETERMINSTIC_VENDOR', 'averaged_scores'),
                                               ('LOOK_UP_TABLE_INPUT_PATH', released_graph_etl_output),
                                               ('GATEWAY_ADDRESS', 'prom-push-gateway.adsrvr.org:80'), ('NUM_PARTITIONS', '800'),
                                               ('VENDOR_NAME', etl_vendor_name)],
            timeout_timedelta=timedelta(hours=4),
            executable_path=Executables.etl_repo_executable,
            configure_cluster_automatically=False
        )
    )

    cluster.add_sequential_body_task(
        EmrJobTask(
            name="household_format",
            class_name="com.thetradedesk.etl.logformats.UniversalToVerticaFormat",
            eldorado_config_option_pairs_list=[
                ('config.resource', 'application.conf'), ('INPUT_PATH', f"{released_graph_etl_output}_household"),
                ('OUTPUT_PATH', f"s3://thetradedesk-useast-data-import{test_folder}/miphh/universal"),
                ('STATS_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/toVerticaFormat/{etl_vendor_name}_household'),
                ('USE_INPUT_SUB_FOLDER', 'true'), ('INPUT_SUB_FOLDER', 'success'), ('LOCAL', 'false'),
                ('GATEWAY_ADDRESS', 'prom-push-gateway.adsrvr.org:80'), ('NUM_PARTITIONS', '1000'), ('VENDOR_NAME', f'{etl_vendor_name}hh'),
                ('UID2_CHECKPOINT_RAW_ID_PATH', 's3://ttd-identity/data/prod/tmp/ttdgraph/graph-checkpoint/uid2/'),
                ('IDL_BIDFEEDBACK_MAPPING_TABLE_RAW_ID_PATH', 's3://ttd-identity/data/prod/tmp/ttdgraph/graph-checkpoint/idlUiidMeta/')
            ],
            timeout_timedelta=timedelta(hours=4),
            executable_path=Executables.etl_repo_executable,
            configure_cluster_automatically=False
        )
    )

    cluster.add_sequential_body_task(
        EmrJobTask(
            name="person_format",
            class_name="com.thetradedesk.etl.logformats.UniversalToVerticaFormat",
            eldorado_config_option_pairs_list=[
                ('config.resource', 'application.conf'), ('INPUT_PATH', score_normalized_output_path),
                ('OUTPUT_PATH', f"s3://thetradedesk-useast-data-import{test_folder}/{etl_vendor_name}/universal"),
                ('STATS_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/toVerticaFormat/{etl_vendor_name}'),
                ('LOCAL', 'false'), ('NUM_PARTITIONS', '1000'), ('VENDOR_NAME', etl_vendor_name),
                ('UID2_CHECKPOINT_RAW_ID_PATH', 's3://ttd-identity/data/prod/tmp/ttdgraph/graph-checkpoint/uid2/'),
                ('IDL_BIDFEEDBACK_MAPPING_TABLE_RAW_ID_PATH', 's3://ttd-identity/data/prod/tmp/ttdgraph/graph-checkpoint/idlUiidMeta/')
            ],
            timeout_timedelta=timedelta(hours=4),
            executable_path=Executables.etl_repo_executable,
            configure_cluster_automatically=False
        )
    )

    return cluster


def get_metrics_steps() -> List[OpTask]:
    class_names = [f"{Executables.identity_repo_class}.pipelines.OpenGraphMonitoring"]

    clusters = []

    for class_name in class_names:
        class_ending = class_name.split(".")[-1]
        cluster = IdentityClusters.get_cluster(f"metrics_cluster_{class_ending}", dag, 10_000, ComputeType.STORAGE)
        cluster.add_sequential_body_task(IdentityClusters.task(class_name, timeout_hours=6))
        if class_ending == "OpenGraphMonitoring":
            cluster.add_sequential_body_task(IdentityClusters.task("com.thetradedesk.idnt.identity.pipelines.TtdGraphMonitoring"))
        clusters.append(cluster)

    ttdgraph_internal_sketch_cluster = IdentityClusters.get_cluster(
        "ttdgraph_metrics_sketches", dag, 3000, ComputeType.ARM_GENERAL, ComputeType.ARM_GENERAL, parallelism_multiplier=1.0
    )
    ttdgraph_internal_sketch_cluster.add_sequential_body_task(
        IdentityClusters.task(
            "com.thetradedesk.idnt.identity.pipelines.OpenGraphSketchPipeline",
            timeout_hours=7,
            # this is to disable eldoardo optimisations - this is the default
            extra_args=[("conf", "spark.python.worker.reuse=true")],
        )
    )
    clusters.append(ttdgraph_internal_sketch_cluster)
    return clusters


# daily datasets
dag >> check_bidstream >> daily_processing_cluster >> weekly_inputs_cluster >> multi_week_cluster

# weekly graph generation
multi_week_cluster >> edge_init_cluster >> regional_clustering >> graph_publish_cluster
multi_week_cluster >> metadata_cluster >> graph_publish_cluster

pre_etl_monitoring = ParallelTasks("pre_etl_monitoring", get_metrics_steps()).as_taskgroup("pre_etl_monitoring")

graph_publish_cluster >> pre_etl_monitoring
etl_cluster = get_etl_load_cluster()
graph_publish_cluster >> etl_cluster

final_dag: DAG = dag.airflow_dag
