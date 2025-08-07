"""
This DAG transfers datasets required to be held by legal to a protected destination bucket.
It copies datasets that are generated hourly.
"""
from datetime import datetime, timedelta

from dags.pdg.legal_hold.legal_hold_hourly_datasets import legal_hold_hourly_datasets
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import pdg
from ttd.tasks.op import OpTask

dag_id = "legal-hold-dataset-transfer-hourly"
aws_conn_id = "ttd-legal-hold-aws"
azure_conn_id = "ttd-legal-hold-azure"
data_start_date = "{{ (data_interval_start - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}"

dag = TtdDag(
    dag_id=dag_id,
    start_date=datetime.now() - timedelta(days=100),
    schedule_interval="0 * * * *",
    slack_channel=pdg.alarm_channel,
    retries=0,
    dag_tsg="https://thetradedesk.atlassian.net/wiki/x/h4AfI",
    tags=["Legal Hold"],
    dagrun_timeout=timedelta(hours=24),
    run_only_latest=False,
    max_active_tasks=12 * 50,
    max_active_runs=12
)

adag = dag.airflow_dag

for dataset in legal_hold_hourly_datasets:
    conn_id = azure_conn_id if dataset.cloud_provider == CloudProviders.azure else aws_conn_id

    dataset_check = OpTask(
        op=DatasetCheckSensor(
            datasets=[dataset.source_dataset],
            ds_date=data_start_date,
            task_id=f"dataset_check_{dataset.dataset_name}",
            poke_interval=60,  # 1 minute
            timeout=60 * 60 * 24,  # 1 day
            cloud_provider=dataset.cloud_provider
        )
    )

    transfer = DatasetTransferTask(
        name=f"dataset_transfer_{dataset.dataset_name}",
        dataset=dataset.source_dataset,
        src_cloud_provider=dataset.cloud_provider,
        dst_cloud_provider=dataset.cloud_provider,
        partitioning_args=dataset.source_dataset.get_partitioning_args(ds_date=data_start_date),
        src_conn_id=conn_id,
        dst_conn_id=conn_id,
        dst_dataset=dataset.dest_dataset,
        prepare_finalise_timeout=timedelta(hours=2),
        num_partitions=dataset.num_partitions,
        max_threads=dataset.max_threads,
        max_try_count=10
    )

    dag >> dataset_check >> transfer
