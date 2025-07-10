from datetime import timedelta, datetime

from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.datasets.hour_dataset import HourDataset
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from dags.audauto.utils import utils
from ttd.ttdenv import TtdEnvFactory

write_env = 'prod' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'test'
db = "audauto_feature" if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'audauto_feature_test'

bucket = "thetradedesk-mlplatform-us-east-1"
path_prefix = "features/feature_store/prod/online"

job_setting_list = ([
    ("jobConfigPath", "s3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/online/jobs/LUF_offline_agg.json"),
    ("dt", "{{ds_nodash}}"), ("yesterday", "{{yesterday_ds_nodash}}"), ("env_read", "prod"), ("env_write", write_env), ("db", db),
    ("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")
])

# generic spark settings list we'll add to each step.
num_clusters = 10
num_partitions = int(round(3.1 * num_clusters)) * 10
# Jar
KONGMING_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"
spark_options_list = utils.get_spark_options_list(num_partitions)

start_date = datetime(2025, 7, 1, 0, 0)

ttd_dag = TtdDag(
    dag_id="perf-automation-feature-store-live-agg",
    start_date=start_date,
    schedule_interval='50 0 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    slack_tags=AUDAUTO.team.sub_team,
    run_only_latest=False,
    tags=["AUDAUTO", "FEATURE_STORE"]
)
adag = ttd_dag.airflow_dag

click_parquet_data = HourDataset(
    bucket=bucket,
    path_prefix=path_prefix,
    data_name="clicktracker/parquet",
    version=None,
    date_format="date=%Y%m%d",
    hour_format='hour={hour:d}',
    env_aware=False,
    check_type="hour",
    success_file=None
)
event_parquet_data = HourDataset(
    bucket=bucket,
    path_prefix=path_prefix,
    data_name="eventtracker/parquet",
    version=None,
    date_format="date=%Y%m%d",
    hour_format='hour={hour:d}',
    env_aware=False,
    check_type="hour",
    success_file=None
)
conversion_parquet_data = HourDataset(
    bucket=bucket,
    path_prefix=path_prefix,
    data_name="conversiontracker/parquet",
    version=None,
    date_format="date=%Y%m%d",
    hour_format='hour={hour:d}',
    env_aware=False,
    check_type="hour",
    success_file=None
)
dataset_sensor_task = OpTask(
    op=DatasetCheckSensor(
        task_id='data_available',
        datasets=[click_parquet_data, event_parquet_data, conversion_parquet_data],
        ds_date='{{logical_date.at(23).to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)

emr = utils.create_emr_cluster("LiveUserFeatureEmr", 40)
daily_adgroup_policy_snapshot = utils.create_emr_spark_job(
    "LiveUserFeatureAgg", "com.thetradedesk.featurestore.jobs.SparkSqlJob", KONGMING_JAR,
    spark_options_list + [("conf", "spark.sql.catalogImplementation=hive")], job_setting_list, emr
)

ttd_dag >> dataset_sensor_task >> emr

final_dag_check = FinalDagStatusCheckOperator(dag=adag)
emr.last_airflow_op() >> final_dag_check
