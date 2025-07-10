from datetime import datetime, timedelta

from dags.hpc.counts_datasources import CountsDatasources, CountsDataName
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import hpc
from datasources.datasources import Datasources

###########################################
# Variables
###########################################
job_schedule_interval = "0 * * * *"
job_start_date = datetime(2024, 10, 21)
dag_id = "counts-dataset-copy"
recency_check_task_id = "recency_check_task"
set_timestamp_task_id = "set_timestamp_task"
timeout = 12  # Hours after the current DateHour we expect to see the downstream dataset. Tune if delays are longer.

###########################################
# DAG
###########################################
dag = TtdDag(
    dag_id=dag_id,
    max_active_runs=5,
    retries=1,
    slack_channel=hpc.alarm_channel,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    run_only_latest=False,
    dagrun_timeout=timedelta(hours=timeout),
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/PADVI',
    tags=[hpc.jira_team]
)
adag = dag.airflow_dag


###########################################
# Copy
###########################################
def copy(upstream_dataset: HourGeneratedDataset, downstream_datasets: list[HourGeneratedDataset]):
    upstream_dataset_check_sensor = OpTask(
        op=DatasetCheckSensor(
            task_id=f"{upstream_dataset.data_name}_check_sensor",
            datasets=[upstream_dataset.with_check_type('hour')],
            ds_date="{{ (logical_date).to_datetime_string() }}",
            poke_interval=60 * 10,
            cloud_provider=CloudProviders.aws,
            timeout=60 * 60 * timeout,  # Waits for delays in upstream dataset.
        )
    )

    for downstream_dataset in downstream_datasets:
        downstream_dataset_check_sensor = OpTask(
            op=DatasetCheckSensor(
                task_id=f"{downstream_dataset.data_name}_check_sensor",
                datasets=[downstream_dataset.with_check_type('hour')],
                ds_date="{{ (logical_date).to_datetime_string() }}",
                poke_interval=60 * 10,
                cloud_provider=CloudProviders.aws,
                timeout=60 * 60 * timeout  # Waits for downstream dataset.
            )
        )

        downstream_dataset_copy_task = DatasetTransferTask(
            name=f'{downstream_dataset.data_name}_copy_task',
            dataset=downstream_dataset,
            src_cloud_provider=CloudProviders.aws,
            dst_cloud_provider=CloudProviders.azure,
            partitioning_args=downstream_dataset.get_partitioning_args(ds_date="{{ (logical_date).to_datetime_string() }}"),
        )

        upstream_dataset_check_sensor >> downstream_dataset_check_sensor >> downstream_dataset_copy_task

    return upstream_dataset_check_sensor


###########################################
# Dependencies
###########################################
dag >> copy(Datasources.avails.avails_7_day, [CountsDatasources.get_counts_dataset(CountsDataName.ACTIVE_TDID_DAID)])
