import copy
from datetime import timedelta, datetime

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.spark import Region
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from dags.audauto.utils import utils

emr_capacity = 500
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/spark-3.5.1/audience.jar"
spark_config = [("conf", "spark.sql.parquet.int96RebaseModeInRead=LEGACY"), ("conf", "spark.pyspark.python=/usr/bin/python3"),
                ("conf", "spark.pyspark.driver.python=/usr/bin/python3")]
additional_application = ["Hadoop"]
ebs_root_volume_size = 64

# generic spark settings list we'll add to each step.
spark_options_list = [
    ("executor-memory", "204G"),
    ("executor-cores", "32"),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.driver.memory=110G"),
    ("conf", "spark.driver.cores=15"),
    ("conf", "spark.sql.shuffle.partitions=4096"),
    ("conf", "spark.default.parallelism=4096"),
    ("conf", "spark.driver.maxResultSize=50G"),
    ("conf", "spark.dynamicAllocation.enabled=true"),
    ("conf", "spark.memory.fraction=0.7"),
    ("conf", "spark.sql.autoBroadcastJoinThreshold=2147483648"),  # 2GiB
    ("conf", "spark.memory.storageFraction=0.25")
]

job_setting_list = ([("date", "{{ ds }}"), ("ttd.env", TtdEnvFactory.get_from_system()),
                     ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")])

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

bootstrap_script_actions = [
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.5.0/latest/monitoring-scripts/node_exporter_bootstrap.sh", []
    ),
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.5.0/latest/monitoring-scripts/graphite_exporter_bootstrap.sh",
        ["s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.5.0/latest/monitoring-scripts"]
    ),
    ScriptBootstrapAction("s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.5.0/latest/bootstrap/import-ttd-certs.sh", []),
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.5.0/latest/monitoring-scripts/bootstrap_stat_collector.sh",
        ["s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.5.0/latest/monitoring-scripts/spark_stat_collect.py"]
    ),
    ScriptBootstrapAction("s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/tensorflow_init.sh", []),
]

rsm_etl_dag = TtdDag(
    dag_id="perf-automation-rsmv2-deal-score",
    start_date=datetime(2025, 5, 20, 2, 0),
    schedule_interval='0 10 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    enable_slack_alert=False,
    run_only_latest=False,
    tags=["DATPERF", "RSM"]
)

environment = TtdEnvFactory.get_from_system()
env = environment.execution_env
adag = rsm_etl_dag.airflow_dag

seed_id_path = "s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/seedids/v=2/date={{ ds_nodash }}"
users_scores_path = "s3://thetradedesk-mlplatform-us-east-1/data/prod/audience/scores/tdid2seedid_all/v=1/date={{ ds_nodash }}"
deal_sv_path = "s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date={{ ds_nodash }}"
tdid_by_dcsv_out_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{env}/audience/RSMV2/deal_score_tdids/v=1/date={{{{ ds_nodash }}}}"
tdid_by_dcsv_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{env}/audience/RSMV2/deal_score_tdids/v=1/"
agg_out_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{env}/audience/RSMV2/deal_score/v=1/date={{{{ ds_nodash }}}}"
agg_score_out_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{env}/audience/RSMV2/deal_score_agg/v=1/date={{{{ ds_nodash }}}}"

num_workers = emr_capacity
cpu_per_worker = 32

# Setting the max number of users (cap_users) to 12500 because that is the optimal number of users to get an accurate score
cap_users = 12500
# Setting the min number of users to the lowest allowable pass rate
min_users = 8000

group_size = 6

look_back = 6

emr_cluster = utils.create_emr_cluster(
    name="DATPERF-Audience-RSMV2-Deal-Score",
    capacity=emr_capacity,
    bootstrap_script_actions=bootstrap_script_actions,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

###############################################################################
# S3 dataset sources
###############################################################################
seedid_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="audience/scores/seedids",
    version=2,
    env_aware=True,
).with_env(TtdEnvFactory.prod)

tdid2seedid_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data",
    data_name="audience/scores/seedpopulationscore",  # ultimate success signal found here
    version=1,
    env_aware=True,
).with_env(TtdEnvFactory.prod)

dealsv_dataset = DateGeneratedDataset(
    bucket="ttd-deal-quality",
    path_prefix="env=prod",
    data_name="VerticaExportDealRelevanceSeedMapping/VerticaAws",
    version=None,
    env_aware=False
)

avails = AvailsDatasources.identity_and_deal_agg_hourly_dataset.with_check_type("day").with_region(Region.US_EAST_1
                                                                                                   ).with_env(TtdEnvFactory.prod)


def create_emr_spark_job(name, class_name, jar, spark_options_list, job_setting_list, emr, timeout=timedelta(hours=3)):
    t = EmrJobTask(
        name=name,
        class_name=class_name,
        additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
        eldorado_config_option_pairs_list=copy.deepcopy(job_setting_list),
        executable_path=jar,
        timeout_timedelta=timeout,
    )
    emr.add_parallel_body_task(t)
    return t


###############################################################################
# S3 dataset sensors
###############################################################################
dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[seedid_dataset, tdid2seedid_dataset, dealsv_dataset, avails],
        ds_date='{{data_interval_start.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 23,
    )
)

########################################################################################################################
# Avails Processing
# The avails data is processed in chunks of size `group_size` to balance resource usage and execution time.
# Processing the entire dataset at once consumes excessive resources and is difficult to manage on EMR clusters
# Conversely, processing data hourly is too granular, resulting in longer total runtime and inefficient resource usage.
########################################################################################################################
tasks = []
for hour in range(0, 24, group_size):
    tasks.append(
        create_emr_spark_job(
            f"Avails_Deal_Score_Search_{hour}_{hour + group_size}",
            "com.thetradedesk.audience.jobs.dealscore.AvailsFindDealScores",
            AUDIENCE_JAR,
            spark_options_list,
            job_setting_list + [("deal_sv_path", deal_sv_path),
                                ("out_path", tdid_by_dcsv_out_path + f"/hour_range={hour}_{hour + group_size}"),
                                ("num_workers", num_workers), ("cpu_per_worker", cpu_per_worker), ("min_hour", hour),
                                ("max_hour", hour + group_size)],
            emr_cluster,
            timeout=timedelta(hours=3)
        )
    )

deals_agg = create_emr_spark_job(
    "Avails_Deal_Aggregate",
    "com.thetradedesk.audience.jobs.dealscore.AvailsDealAggregate",
    AUDIENCE_JAR,
    spark_options_list,
    job_setting_list + [("seed_id_path", seed_id_path), ("seed_id_path", seed_id_path), ("users_scores_path", users_scores_path),
                        ("deal_sv_path", deal_sv_path), ("tdids_by_dcsv_path", tdid_by_dcsv_path), ("out_path", agg_out_path),
                        ("agg_score_out_path", agg_score_out_path), ("num_workers", num_workers), ("cpu_per_worker", cpu_per_worker),
                        ("cap_users", cap_users), ("min_users", min_users), ("look_back", look_back)],
    emr_cluster,
    timeout=timedelta(hours=3)
)

rsm_etl_dag >> dataset_sensor >> emr_cluster

for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]

tasks[-1] >> deals_agg

final_dag_check = FinalDagStatusCheckOperator(dag=adag)
emr_cluster.last_airflow_op() >> final_dag_check
