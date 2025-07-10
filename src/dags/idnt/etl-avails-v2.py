from datetime import datetime, timedelta
from airflow import DAG

from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables
from ttd.ttdenv import TtdEnvFactory

ttd_env = TtdEnvFactory.get_from_system()
write_subdir = '' if str(ttd_env) == 'prod' else '/test'

# input/output path definitions
output_path_bucket = 'ttd-identity'
output_ided_path_prefix = f'datapipeline{write_subdir}/sources/avails-idnt-v2'
output_ided_path_base = f's3://{output_path_bucket}/{output_ided_path_prefix}'
output_idless_path_prefix = f'datapipeline{write_subdir}/sources/avails-idnt-idless-v2'
output_idless_path_base = f's3://{output_path_bucket}/{output_idless_path_prefix}'
stat_path_base = f's3://ttd-identity/adbrain-stats{write_subdir}/etl/avails-idnt-v2'

hours_to_run = 3
hours_buffer = 2

dag = DagHelpers.identity_dag(
    dag_id='etl-avails-v2',
    schedule_interval=timedelta(hours=3),
    start_date=datetime(2024, 10, 9, 4, 30),
    run_only_latest=True,
    max_active_runs=3
)

cluster = IdentityClusters.get_cluster("cluster", dag, 3600, ComputeType.MEMORY, use_delta=False)

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
    input_hour = f'{{{{ (data_interval_end - macros.timedelta(hours={hour_offset})).strftime(\"%Y/%m/%d/%H\") }}}}'
    output_hour = f'{{{{ (data_interval_end - macros.timedelta(hours={hour_offset})).strftime(\"%Y-%m-%d/%H\") }}}}'

    cluster.add_sequential_body_task(
        IdentityClusters.task(
            task_name_suffix=f"_offset_{hour_offset}",
            class_name="com.thetradedesk.identity.avails.etl.AvailsEtl",
            executable_path=Executables.avails_etl_v2_executable,
            runDate_arg="DATE",  # Not used
            extra_args=[
                # driver
                ('conf', 'spark.driver.cores=8'),
                ('conf', 'spark.driver.memory=80g'),
                ('conf', 'spark.driver.memoryOverhead=2048'),
                # executor
                ('conf', 'spark.executor.cores=7'),
                ('conf', 'spark.executor.memory=55g'),
                ('conf', 'spark.executor.memoryOverhead=2048'),
                ('conf', 'spark.sql.shuffle.partitions=6300'),
                ('conf', 'spark.default.parallelism=6300'),
                ('conf', 'spark.dynamicAllocation.enabled=false'),
                ('conf', 'spark.executor.instances=450'),
            ],
            eldorado_configs=[
                ('LOCAL', 'false'),
                ('INPUT_PATH', f's3://thetradedesk-useast-partners-avails/tapad/{input_hour}'),
                ('OUTPUT_IDED_PATH', f'{output_ided_path_base}/{output_hour}'),
                ('OUTPUT_IDLESS_PATH', f'{output_idless_path_base}/{output_hour}'),
                ('STATS_PATH', f'{stat_path_base}/{output_hour}'),
            ],
            timeout_hours=8
        )
    )

dag >> cluster

etl_avails_v2_dag: DAG = dag.airflow_dag
