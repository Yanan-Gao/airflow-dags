from airflow import DAG
from datetime import datetime, timedelta
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Tags, Executables, RunTimes
from dags.idnt.vendors.vendor_datasets import VendorDatasets

ttd_env = Tags.environment()
path_change = '' if str(ttd_env) == 'prod' else '/test'

input_path_base = 's3://thetradedesk-useast-data-import/mobilewalla/dids'
s3_bucket = 'ttd-identity'
output_path_base = f's3://{s3_bucket}/datapipeline{path_change}/sources'
stats_path = f's3://{s3_bucket}/adbrain-stats/etl{path_change}/mobilewalla'

run_time = RunTimes.days_02_ago_full_day

dag = DagHelpers.identity_dag(
    dag_id='etl-mobilewalla',
    start_date=datetime(2024, 11, 15),
    schedule_interval='0 16 * * *',
    retry_delay=timedelta(minutes=30),
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=6)
)

cluster = IdentityClusters.get_cluster(
    'etl-mobilewalla', dag, 3000, ComputeType.STORAGE, use_delta=False, emr_release_label=Executables.emr_version_6
)

# generating mobilewalla dataset for each region's s3 path
regions = ['AU', 'CA', 'ID', 'JP', 'TH', 'US']
all_mobilewalla_data = list(map(VendorDatasets.get_mobilewalla_raw_data, regions))

check_mobilewalla_data = DagHelpers.check_datasets(datasets=all_mobilewalla_data)

mobilewalla_etl_run_step = IdentityClusters.task(
    class_name='com.thetradedesk.etl.misc.MobileWallaETL',
    eldorado_configs=[('LOCAL', 'false'), ('DATE_TIME_FROM', run_time), ('DATE_TIME_TO', run_time), ('INPUT_PATH_BASE', input_path_base),
                      ('INPUT_PARTITIONS', 60000), ('OUTPUT_PATH_BASE', output_path_base), ('STATS_PATH', stats_path)],
    executable_path=Executables.etl_repo_executable
)

cluster.add_sequential_body_task(mobilewalla_etl_run_step)
dag >> check_mobilewalla_data >> cluster

final_dag: DAG = dag.airflow_dag
