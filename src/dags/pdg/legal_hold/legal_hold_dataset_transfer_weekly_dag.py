"""
This DAG transfers datasets required to be held by legal to a protected destination bucket.
It copies datasets that are generated weekly.
"""

from datetime import datetime, timedelta

from dags.pdg.legal_hold.legal_hold_weekly_datasets import legal_hold_weekly_datasets
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import pdg
from ttd.tasks.op import OpTask

dag_id = "legal-hold-dataset-transfer-weekly"
aws_conn_id = "ttd-legal-hold-aws"
azure_conn_id = "ttd-legal-hold-azure"

dag = TtdDag(
    dag_id=dag_id,
    start_date=datetime.now() - timedelta(days=100),
    schedule_interval="0 0 * * 4",  # Weekly on Thursday at 00:00 UTC. Datasets are currently generated between Saturday and Wednesday
    slack_channel=pdg.alarm_channel,
    retries=0,
    dag_tsg="https://thetradedesk.atlassian.net/wiki/x/h4AfI",
    tags=["Legal Hold"],
    dagrun_timeout=timedelta(days=7),
    run_only_latest=False,
    max_active_tasks=3 * 20,
    max_active_runs=3
)

adag = dag.airflow_dag

for dataset in legal_hold_weekly_datasets:
    conn_id = azure_conn_id if dataset.cloud_provider == CloudProviders.azure else aws_conn_id

    # get the most recent dataset date based on the day of the week it gets generated on
    ds_date = f"{{{{ (data_interval_end - macros.timedelta(days=(data_interval_end.weekday() - {dataset.weekday}) % 7)).strftime('%Y-%m-%d') }}}}"
    ds_date_with_hour = f"{{{{ (data_interval_end - macros.timedelta(days=(data_interval_end.weekday() - {dataset.weekday}) % 7)).strftime('%Y-%m-%d 00:00:00') }}}}"

    dataset_check = OpTask(
        op=DatasetCheckSensor(
            datasets=[dataset.source_dataset],
            ds_date=ds_date_with_hour,
            task_id=f"dataset_check_{dataset.dataset_name}",
            poke_interval=60 * 30,  # 30 minutes
            timeout=60 * 60 * 24 * 7,  # 7 days
            cloud_provider=dataset.cloud_provider
        )
    )

    transfer = DatasetTransferTask(
        name=f"dataset_transfer_{dataset.dataset_name}",
        dataset=dataset.source_dataset,
        src_cloud_provider=dataset.cloud_provider,
        dst_cloud_provider=dataset.cloud_provider,
        partitioning_args=dataset.source_dataset.get_partitioning_args(ds_date=ds_date),
        src_conn_id=conn_id,
        dst_conn_id=conn_id,
        dst_dataset=dataset.dest_dataset,
        prepare_finalise_timeout=timedelta(minutes=90),
        num_partitions=dataset.num_partitions,
        max_threads=dataset.max_threads,
        max_try_count=10
    )

    dag >> dataset_check >> transfer
