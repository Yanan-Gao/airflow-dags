from datetime import timedelta, datetime

from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.aws.emr_pyspark import S3PysparkEmrTask
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.confetti.confetti_task_factory import (
    make_confetti_tasks,
    resolve_env,
    make_confetti_post_processing_task,
)
from ttd.eldorado.xcom.helpers import get_xcom_pull_jinja_string

from dags.audauto.utils import utils
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from airflow.operators.python import PythonOperator

emr_capacity = 150
AUDIENCE_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/audience.jar"



java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
spark_config = [("conf", "spark.sql.parquet.int96RebaseModeInRead=LEGACY"), ("conf", "spark.pyspark.python=/usr/bin/python3"),
                ("conf", "spark.pyspark.driver.python=/usr/bin/python3")]
additional_application = ["Hadoop"]
ebs_root_volume_size = 64

experiment = "yanan-demo"

environment = TtdEnvFactory.get_from_system()
env = environment.execution_env

override_env = "test" if env == "prodTest" else env
imp_read_env = "prod"
# confetti utilities need the run date string
run_date = "{{ ds }}"
# how much to sample from Geronimo data source
sampling_rate = 3

# generic spark settings list we'll add to each step.
spark_options_list = [("executor-memory", "204G"), ("executor-cores", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=110G"),
                      ("conf", "spark.driver.cores=15"), ("conf", "spark.sql.shuffle.partitions=6200"),
                      ("conf", "spark.default.parallelism=6200"), ("conf", "spark.driver.maxResultSize=50G"),
                      ("conf", "spark.dynamicAllocation.enabled=true"), ("conf", "spark.memory.fraction=0.7"),
                      ("conf", "spark.memory.storageFraction=0.25")]

job_setting_list = ([("date", "{{ ds }}"), ("ttd.env", override_env),
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
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/node_exporter_bootstrap.sh", []
    ),
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/graphite_exporter_bootstrap.sh",
        ["s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts"]
    ),
    ScriptBootstrapAction("s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/bootstrap/import-ttd-certs.sh", []),
    ScriptBootstrapAction(
        "s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/bootstrap_stat_collector.sh",
        ["s3://ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/monitoring-scripts/spark_stat_collect.py"]
    ),
    ScriptBootstrapAction("s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/tensorflow_init.sh", []),
]

rsm_etl_dag = TtdDag(
    dag_id="perf-automation-rsmv2-offline-score",
    start_date=datetime(2025, 5, 20, 2, 0),
    schedule_interval='40 0 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    slack_channel="#dev-perf-auto-alerts-rsm",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "RSM", "RSMV2"]
)

experiment = "yanan-demo"
experiment_path = f"/{experiment}" if experiment else ""
confetti_env = resolve_env(env, experiment)

adag = rsm_etl_dag.airflow_dag

# since S3PysparkEmrTask does not work with a .json file, so as a workaround, we copy and rename it to features_json
feature_path_origin = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/schema/RSMV2/v=1/{{{{ ds_nodash }}}}000000/features.json"
feature_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/schema/RSMV2/v=1/{{{{ ds_nodash }}}}000000/features_json"
data_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{imp_read_env}/audience/RSMV2/Imp_Seed_None/v=1/{{{{ ds_nodash }}}}000000/"
model_path = f"s3://thetradedesk-mlplatform-us-east-1/models/prod/RSMV2/bidrequest_model/{{{{ ds_nodash }}}}000000/"
output_path = f"s3://thetradedesk-mlplatform-us-east-1/data/{override_env}/audience/RSMV2/emb/raw/v=1/date={{{{ ds_nodash }}}}"
seed_emb_path = f"s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/embedding/RSMV2/v=1/{{{{ ds_nodash }}}}000000/"

geronimo_etl_dataset = utils.get_geronimo_etl_dataset()
feature_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"configdata/prod",
    env_aware=False,
    data_name="audience/schema/RSMV2/v=1",
    version=None,
    date_format="%Y%m%d000000",
    success_file="features.json"
)

# use user feature merge job success status instead, will change to a new one in the future
feature_store_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/feature_store/prod",
    env_aware=False,
    data_name="user_features_merged/v=1",
    version=None,
    date_format="%Y%m%d00",
    success_file="_SUCCESS"
)

dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[geronimo_etl_dataset, feature_dataset, feature_store_dataset],
        ds_date='{{logical_date.at(23).to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

model_dataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix=f"models/prod",
    env_aware=False,
    data_name=f"RSMV2/bidrequest_model",
    version=None,
    date_format="%Y%m%d000000"
)

model_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='model_available',
        datasets=[model_dataset],
        ds_date='{{logical_date.at(23).to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 20,
    )
)


# step 1: copy feature.json as feature_json
def copy_s3_object(**kwargs):
    # Extract the parameters from the context
    source_bucket, source_key = utils.extract_bucket_and_key(kwargs['source'])
    destination_bucket, destination_key = utils.extract_bucket_and_key(kwargs['destination'])

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    try:
        # Copy the object
        s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key}, Bucket=destination_bucket, Key=destination_key)
        print(f"Copied {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error in credentials: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


# Define the PythonOperator task
copy_feature_json = OpTask(
    op=PythonOperator(
        task_id="copy_feature_json",
        provide_context=True,
        python_callable=copy_s3_object,
        op_kwargs={
            'source': feature_path_origin,
            'destination': feature_path,
        },
        dag=adag
    )
)


# step2: cleanup the folder of emb output path
def delete_s3_objects(**kwargs):
    """
    Deletes all objects in an S3 bucket with the specified prefix.

    :param bucket_name: str, The name of the S3 bucket.
    :param prefix: str, The prefix of the objects to delete.
    """
    s3_path = kwargs['s3_path']
    utils.delete_s3_objects_with_prefix(s3_path=s3_path)


emr_cluster_part1 = utils.create_emr_cluster(
    name="AUDAUTO-Audience-RSMV2-Relevance-Offline-part1", capacity=emr_capacity, bootstrap_script_actions=bootstrap_script_actions
)

# Confetti prepare/gate for the Imp2Br model input generation
prep_imp2br, gate_imp2br = make_confetti_tasks(
    group_name="audience",
    job_name="Imp2BrModelInferenceDataGenerator",
    experiment_name=experiment,
    run_date=run_date,
)

post_processing_imp2br = make_confetti_post_processing_task(
    job_name="Imp2BrModelInferenceDataGenerator",
    prep_task=prep_imp2br,
    cluster_id=emr_cluster_part1.cluster_id,
)

# step 3: generate the model input
gen_model_input = utils.create_emr_spark_job(
    "Generate_Model_Input",
    "com.thetradedesk.audience.jobs.Imp2BrModelInferenceDataGenerator",
    get_xcom_pull_jinja_string(task_ids=prep_imp2br.task_id, key="audienceJarPath"),
    spark_options_list + [("packages", "com.linkedin.sparktfrecord:spark-tfrecord_2.12:0.4.0")],
    job_setting_list
    + [("feature_path", feature_path_origin), ("sampling_rate", sampling_rate), ("confettiEnv", confetti_env), ("experimentName", experiment), ("confettiRuntimeConfigBasePath", get_xcom_pull_jinja_string(task_ids=prep_imp2br.task_id, key="confetti_runtime_config_base_path"))],
    emr_cluster_part1,
)


########################################################
# Part 2, wait for model, then proceed
########################################################

# clean up raw bid request level embedding, in case there is EMR retry
clean_up_raw_embedding = OpTask(
    op=PythonOperator(
        task_id='clean_up_raw_embedding',
        provide_context=True,
        python_callable=delete_s3_objects,
        op_kwargs={'s3_path': output_path + "/"},
        dag=rsm_etl_dag.airflow_dag,
    )
)

emr_cluster_part2 = utils.create_emr_cluster(
    name="AUDAUTO-Audience-RSMV2-Relevance-Offline-part2", capacity=emr_capacity, bootstrap_script_actions=bootstrap_script_actions
)

# Prepare Confetti runtime config for all part2 jobs
prep_part2, gate_part2 = make_confetti_tasks(
    group_name="audience",
    job_name="RelevanceModelOfflineScoringPart2",
    experiment_name=experiment,
    run_date=run_date,
)

post_processing_part2 = make_confetti_post_processing_task(
    job_name="RelevanceModelOfflineScoringPart2",
    prep_task=prep_part2,
    cluster_id=emr_cluster_part2.cluster_id,
)

# Step 4: generate the raw bid request level embedding, by model prediction with spark
arguments = [f"--feature_path={feature_path}", f"--data_path={data_path}", f"--model_path={model_path}", f"--out_path={output_path}"]

emb_gen = S3PysparkEmrTask(
    name="GenRawEmbeddingWithModelPrediction",
    entry_point_path="s3://thetradedesk-mlplatform-us-east-1/libs/audience/scripts/model_prediction_spark.py",
    additional_args_option_pairs_list=spark_options_list,
    cluster_specs=emr_cluster_part2.cluster_specs,
    command_line_arguments=arguments,
)
emr_cluster_part2.add_parallel_body_task(emb_gen)

# Step 5: aggregate raw embedding into TDID level
emb_aggregation = utils.create_emr_spark_job(
    "Embedding_Aggregation",
    "com.thetradedesk.audience.jobs.TdidEmbeddingAggregate",
    get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="audienceJarPath"),
    spark_options_list,
    job_setting_list
    + [
        ("confettiEnv", confetti_env),
        ("experimentName", experiment),
        (
            "confettiRuntimeConfigBasePath",
            get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="confetti_runtime_config_base_path"),
        ),
    ],
    emr_cluster_part2,
)

# Step 6: upload aggregated TTD level embeddings to coldstorage bucket so they would be picked by the process that send them.
emb_to_coldstorage = utils.create_emr_spark_job(
    "UploadEmbeddings",
    "com.thetradedesk.audience.jobs.UploadEmbeddings",
    get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="audienceJarPath"),
    spark_options_list,
    job_setting_list
    + [
        ("confettiEnv", confetti_env),
        ("experimentName", experiment),
        (
            "confettiRuntimeConfigBasePath",
            get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="confetti_runtime_config_base_path"),
        ),
    ],
    emr_cluster_part2,
)

# Step 7: dot product
emb_dot_product = utils.create_emr_spark_job(
    "Embedding_DotProduct",
    "com.thetradedesk.audience.jobs.TdidEmbeddingDotProductGeneratorOOS",
    get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="audienceJarPath"),
    spark_options_list,
    job_setting_list
    + [
        ("seed_emb_path", seed_emb_path),
        ("sampling_rate", sampling_rate),
        ("confettiEnv", confetti_env),
        ("experimentName", experiment),
        (
            "confettiRuntimeConfigBasePath",
            get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="confetti_runtime_config_base_path"),
        ),
    ],
    emr_cluster_part2,
)

# Step 8: apply min max scaling
score_min_max_scale_population = utils.create_emr_spark_job(
    "Score_Min_Max_Scale_Population_Score",
    "com.thetradedesk.audience.jobs.TdidSeedScoreScale",
    get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="audienceJarPath"),
    spark_options_list + [("conf", "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false")],  # skip _SUCCESS file generation
    job_setting_list
    + [
        ("sampling_rate", sampling_rate),
        ("confettiEnv", confetti_env),
        ("experimentName", experiment),
        (
            "confettiRuntimeConfigBasePath",
            get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="confetti_runtime_config_base_path"),
        ),
    ],
    emr_cluster_part2,
)

# Step 9: check data quality
data_quality_check = utils.create_emr_spark_job(
    "data_quality_check",
    "com.thetradedesk.audience.jobs.TdidSeedScoreQualityCheck",
    get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="audienceJarPath"),
    spark_options_list,
    job_setting_list
    + [
        ("confettiEnv", confetti_env),
        ("experimentName", experiment),
        (
            "confettiRuntimeConfigBasePath",
            get_xcom_pull_jinja_string(task_ids=prep_part2.task_id, key="confetti_runtime_config_base_path"),
        ),
    ],
    emr_cluster_part2,
)

rsm_etl_dag >> dataset_sensor >> prep_imp2br >> gate_imp2br >> emr_cluster_part1 >> model_sensor >> copy_feature_json >> clean_up_raw_embedding >> prep_part2 >> gate_part2 >> emr_cluster_part2
emb_gen >> emb_aggregation >> emb_to_coldstorage >> emb_dot_product >> score_min_max_scale_population >> data_quality_check
final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=adag))
emr_cluster_part2 >> final_dag_check
emr_cluster_part1 >> post_processing_imp2br >> final_dag_check
emr_cluster_part2 >> post_processing_part2 >> final_dag_check
