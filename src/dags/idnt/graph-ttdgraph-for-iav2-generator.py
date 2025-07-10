from airflow import DAG

from datetime import datetime, timedelta
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.openlineage import OpenlineageConfig
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask

from dags.idnt.feature_flags import FeatureFlags
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers, TimeHelpers
from dags.idnt.statics import Executables, RunTimes, Tags
from dags.idnt.vendors.vendor_datasets import VendorDatasets

job_schedule_interval = '0 12 * * 0'  # We run this on Sunday, but not ETL the vertica/dataserver load
job_start_date = datetime(2024, 9, 3)
job_name = 'graph-ttdgraph-for-iav2-generator'

num_cores = 120 * 96

run_date = RunTimes.previous_full_day
throtle_lookback_days = 30
truedata_lookback_days = 90
uid2_lookback_days = 45
uid2_vendor_list = [
    "liveintent",
    "throtle",
    "truedata",
    "lifesight",
    "mediawallah",
    "networld",
    "rave",
    "pickwell",
    "kochava",
    "equifax",
    "wed",
    "digifish",
    "sirdata",
    "inquiro",
    "happygo",
    "bobbleai",
    "loyaltyMarketing",
    "rakuteninsight",
]

uid2_vendor_datasets = list(map(VendorDatasets.get_uid_vendor_dataset, uid2_vendor_list))

env = Tags.environment()
write_subdir = '/prod' if str(env) == 'prod' else '/test'

dag = DagHelpers.identity_dag(dag_id=job_name, schedule_interval=job_schedule_interval, start_date=job_start_date, run_only_latest=True)

ttdgraph_generation_dag: DAG = dag.airflow_dag
throtle_data = VendorDatasets.throtle.with_lookback(30)

check_singleton_graph = OpTask(
    op=DatasetCheckSensor(
        datasets=[IdentityDatasets.singleton_graph],
        ds_date=RunTimes.previous_day_last_hour,
        poke_interval=TimeHelpers.minutes(5),
        timeout=TimeHelpers.hours(6),
    )
)

check_throtle = DagHelpers.check_datasets_with_lookback(
    dataset_name="throtle", datasets=[VendorDatasets.throtle], poke_minutes=5, timeout_hours=6, lookback_days=throtle_lookback_days
)

check_truedata = DagHelpers.check_datasets_with_lookback(
    dataset_name="truedata", datasets=[VendorDatasets.truedata], poke_minutes=5, timeout_hours=6, lookback_days=truedata_lookback_days
)

check_uid2_vendors = DagHelpers.check_datasets_with_lookback(
    dataset_name="uid2_vendors", datasets=uid2_vendor_datasets, poke_minutes=5, timeout_hours=6, lookback_days=uid2_lookback_days
)


def get_ttdgraph_generation_cluster(graph_name: str, ttdgraph_generator_extra_eldorado_config_option_pairs_list=[]) -> EmrClusterTask:
    additional_app_configs = [{
        'Classification': 'hdfs-site',
        'Properties': {
            'dfs.replication': '2'  # update replication factor to 2 since we are running on demand core nodes
        }
    }]
    ttdgraph_generator_cluster = IdentityClusters.get_cluster(
        f"{graph_name}_generation_cluster",
        dag,
        num_cores,
        ComputeType.STORAGE,
        cpu_bounds=(64, 2048),
        additional_app_configs=additional_app_configs
    )

    # Without specifying executor memory overrides, we get obsecure Spark memory issues.
    spark_graph_generator_additional_args_option_pairs_list = [
        ("executor-memory", "384g"),
        ("executor-cores", "24"),
        ("conf", "spark.executor.memoryOverhead=12044m"),
        ("conf", "spark.yarn.executor.memoryOverhead=24044m"),
        ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ]

    ttdgraph_generator_cluster.add_parallel_body_task(
        EmrJobTask(
            name=f"generate_{graph_name}",
            class_name='jobs.identity.TTDGraphGenerator',
            eldorado_config_option_pairs_list=[
                ('date', run_date), ('GraphOutputFolder', graph_name),
                ('GraphCheckpointPath', f"s3://ttd-identity/data{write_subdir}/tmp/{graph_name}/graph-checkpoint")
            ] + ttdgraph_generator_extra_eldorado_config_option_pairs_list,
            timeout_timedelta=timedelta(hours=8),
            executable_path=Executables.identity_repo_executable,
            configure_cluster_automatically=False,
            additional_args_option_pairs_list=spark_graph_generator_additional_args_option_pairs_list,
            openlineage_config=OpenlineageConfig(enabled=False)
        )
    )

    ttdgraph_generator_cluster.add_sequential_body_task(
        EmrJobTask(
            name=f"person_reshuffler_{graph_name}",
            class_name='jobs.identity.ttdgraph.PersonShuffle',
            eldorado_config_option_pairs_list=[
                ('runDate', run_date),
                ("jobs.identity.ttdgraph.PersonShuffle.graphProdFolder", graph_name),
                ("jobs.identity.ttdgraph.PersonRelabel.outputFolder", f"{graph_name}/person_relabeller"),
                ("jobs.identity.ttdgraph.PersonShuffle.outputFolder", f"{graph_name}/person_reshuffler"),
            ],
            timeout_timedelta=timedelta(hours=1),
            executable_path=Executables.identity_repo_executable,
            configure_cluster_automatically=False
        )
    )

    ttdgraph_generator_cluster.add_sequential_body_task(
        EmrJobTask(
            name=f"person_relabeller_{graph_name}",
            class_name='jobs.identity.ttdgraph.PersonRelabel',
            eldorado_config_option_pairs_list=[
                ('runDate', run_date),
                ("jobs.identity.ttdgraph.PersonRelabel.outputFolder", f"{graph_name}/person_relabeller"),
                ("jobs.identity.ttdgraph.PersonShuffle.outputFolder", f"{graph_name}/person_reshuffler"),
            ],
            timeout_timedelta=timedelta(hours=1),
            executable_path=Executables.identity_repo_executable,
            configure_cluster_automatically=False
        )
    )

    return ttdgraph_generator_cluster


graph_name = "adbrain_legacy"
ttdgraph_generator = get_ttdgraph_generation_cluster(graph_name)

if FeatureFlags.enable_cookieless_graph_generation:
    cookieless_graph_name = "cookielessttdgraph4iav2"
    cookieless_ttdgraph_generator = get_ttdgraph_generation_cluster(
        cookieless_graph_name,
        ttdgraph_generator_extra_eldorado_config_option_pairs_list=[('EnableThirdPartyCookiesDeprecationSimulation', "true")]
    )

dag >> check_singleton_graph
dag >> check_throtle
dag >> check_truedata
dag >> check_uid2_vendors

check_singleton_graph >> ttdgraph_generator
check_throtle >> ttdgraph_generator
check_truedata >> ttdgraph_generator
check_uid2_vendors >> ttdgraph_generator

if FeatureFlags.enable_cookieless_graph_generation:
    check_singleton_graph >> cookieless_ttdgraph_generator
    check_throtle >> cookieless_ttdgraph_generator
    check_truedata >> cookieless_ttdgraph_generator
    check_uid2_vendors >> cookieless_ttdgraph_generator
