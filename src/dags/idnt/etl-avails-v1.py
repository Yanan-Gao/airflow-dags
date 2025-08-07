from datetime import datetime, timedelta
from airflow import DAG

from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables
from ttd.ttdenv import TtdEnvFactory

job_name = 'etl-avails-v1'

ttd_env = TtdEnvFactory.get_from_system()
write_subdir = '' if str(ttd_env) == 'prod' else '/test'

output_path_base = f's3://ttd-identity/datapipeline{write_subdir}/sources/avails-idnt'
output_dnt_path_base = f's3://ttd-identity/datapipeline{write_subdir}/sources/avails-idnt-dnt'
output_masked_ip_path_base = f's3://ttd-identity/datapipeline{write_subdir}/sources/avails-idnt-masked-ip'
output_hashed_id_path_base = f's3://ttd-identity/datapipeline{write_subdir}/sources/avails-idnt-hashed-id'

stats_path = f's3://ttd-identity/adbrain-stats{write_subdir}/etl/avails-idnt'

hours_to_run = 3
hours_buffer = 2

dag = DagHelpers.identity_dag(
    dag_id=job_name, schedule_interval=timedelta(hours=3), start_date=datetime(2024, 10, 9, 4, 30), run_only_latest=True, max_active_runs=3
)

cluster = IdentityClusters.get_cluster('cluster', dag, 3100, ComputeType.MEMORY, use_delta=False)


def get_hour_by_offset(offset: int):
    return f'{{{{ (data_interval_end - macros.timedelta(hours={offset})).strftime(\"%Y-%m-%dT%H:00:00\") }}}}'


# how we decide which hours to run?
# - when it reaches to actual execution_date (next_execution_date)
# - we kick off the run for execution_date, which is 3 hours behind
# - with 2 hours as buffer time, the hours we pick are 2-5 hours before execution_date, starting from the earliest hour
# ------------------------------------------->t
#   |   |   |   |      |        |
#   |<->|<->|<->|<---->|<--execution_date
#   |   |   |       |  |        |
#   |   |   |       |  |<------>|<--actual execution_date
#   |   |   |       |      |
#   |   |   |       |      |<--schedule 3h
#   |   |   |       |
#   |   |   |       |<--hours_buffer 2h
#   |   |   |
#   |<--first processed hour
#       |<--second processed hour
#           |<--third processed hour

for i in range(hours_to_run, 0, -1):  # 5, 4, 3
    hour_offset = hours_buffer + i
    data_date_hour_start = get_hour_by_offset(hour_offset)
    data_date_hour_end = get_hour_by_offset(hour_offset - 1)

    cluster.add_sequential_body_task(
        IdentityClusters.task(
            task_name_suffix=f"_offset_{hour_offset}",
            class_name="com.thetradedesk.etl.misc.AvailsEtl",
            executable_path=Executables.etl_repo_executable,
            runDate_arg="DATE",  # Not used
            eldorado_configs=[
                ('LOCAL', 'false'),
                ('DATE_TIME_FROM', data_date_hour_start),
                ('DATE_TIME_TO', data_date_hour_end),
                ('OUTPUT_PATH_BASE', output_path_base),
                ('OUTPUT_DNT_PATH_BASE', output_dnt_path_base),
                ('OUTPUT_MASKED_IP_PATH_BASE', output_masked_ip_path_base),
                ('OUTPUT_PATH_BASE_HASHED_ID', output_hashed_id_path_base),
                ('STATS_PATH', stats_path),
            ],
            extra_args=[
                # driver
                ('conf', 'spark.driver.cores=10'),
                ('conf', 'spark.driver.memory=100g'),
                ('conf', 'spark.driver.memoryOverhead=2048'),
                # executor
                ('conf', 'spark.executor.cores=8'),
                ('conf', 'spark.executor.memory=75g'),
                ('conf', 'spark.executor.memoryOverhead=2048'),
                ('conf', 'spark.sql.shuffle.partitions=6300'),
                ('conf', 'spark.default.parallelism=6300'),
                ('conf', 'spark.dynamicAllocation.enabled=false'),
                ('conf', 'spark.executor.instances=450'),
            ],
            timeout_hours=5
        )
    )

etl_avails_v1_dag: DAG = dag.airflow_dag

dag >> cluster
