from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.eldorado.base import TtdDag
from datetime import datetime

from ttd.slack.slack_groups import dataproc

job_schedule_interval = "0 * * * *"
job_start_date = datetime(2023, 3, 24)

dag = TtdDag(
    dag_id="demo-dataset-transfer-task",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=dataproc.alarm_channel,
    slack_tags=dataproc.sub_team,
    tags=["DATAPROC"],
)

adag = dag.airflow_dag

hour_dataset = Datasources.test.hour_dataset

dataset_transfer_task = DatasetTransferTask(
    name="transfer-large-dataset",
    dataset=hour_dataset,
    src_cloud_provider=CloudProviders.aws,
    dst_cloud_provider=CloudProviders.azure,
    partitioning_args=hour_dataset.get_partitioning_args(ds_date="{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}"),
)
dag >> dataset_transfer_task
