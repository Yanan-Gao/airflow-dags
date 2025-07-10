from airflow import DAG
from datetime import datetime, timedelta
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Tags, Executables, RunTimes
from dags.idnt.vendors.vendor_datasets import VendorDatasets

bidfeedback_eu_s3_bucket = 'thetradedesk-useast-logs-2'
input_path_base = f's3://{bidfeedback_eu_s3_bucket}/unmaskedandunhashedbidfeedback/collected/'
fraud_path_base = f's3a://{bidfeedback_eu_s3_bucket}/bidfeedback/cleanselineblocked/'

ttd_env = Tags.environment()
path_change = '' if str(ttd_env) == 'prod' else '/test'

s3_bucket = 'ttd-identity'
output_path_base = f's3://{s3_bucket}/datapipeline/sources{path_change}/bidfeedback_new'
output_path_base_eu = f's3://{s3_bucket}/datapipeline/sources{path_change}/bidfeedback_eu_v2'
stats_path = f's3://{s3_bucket}/adbrain-stats/etl{path_change}/bidfeedback_new'

run_time = RunTimes.previous_full_day

dag = DagHelpers.identity_dag(
    dag_id='etl-bidfeedback-eu',
    start_date=datetime(2024, 11, 22),
    schedule_interval='15 0 * * *',
    retry_delay=timedelta(minutes=30),
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=4),
    retries=5
)

cluster = IdentityClusters.get_cluster('etl-bidfeedback-eu', dag, 3000, ComputeType.STORAGE, use_delta=False)

check_bidfeedback_eu_input_data = DagHelpers.check_datasets(datasets=VendorDatasets.get_bidfeedback_eu_raw_data())

bidfeedback_eu_etl_run_step = IdentityClusters.task(
    class_name='com.thetradedesk.etl.misc.BidFeedbackEtl',
    executable_path=Executables.etl_repo_executable,
    eldorado_configs=[('LOCAL', 'false'), ('START_DATE', run_time), ('END_DATE', run_time), ('INPUT_PATH_BASE', input_path_base),
                      ('INPUT_PARTITIONS', 60000), ('OUTPUT_PATH_BASE', output_path_base),
                      ('OUTPUT_PATH_BASE_EU_DATA', output_path_base_eu), ('FRAUD_PATH_BASE', fraud_path_base), ('STATS_PATH', stats_path),
                      ('READ_NEW_CSV_FORMAT', 'true'), ('SPLIT_EU_DATA', 'true'), ('NUM_PARTITIONS', '400')]
)

cluster.add_sequential_body_task(bidfeedback_eu_etl_run_step)
dag >> check_bidfeedback_eu_input_data >> cluster

final_dag: DAG = dag.airflow_dag
