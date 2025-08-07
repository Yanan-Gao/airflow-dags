from datetime import datetime
from datasources.sources.ctv_datasources import CtvDatasources
from ttd.cloud_provider import CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.slack.slack_groups import OMNIUX
from ttd.tasks.op import OpTask

job_start_date = datetime(2023, 7, 17, 0, 0)
job_schedule_interval = "0 0 * * 6"

# whip folders to import data from
whip_external_folders = ["Content", "Episodes", "Sources", "Stats"]

ttd_dag = TtdDag(
    dag_id="whip-data-import",
    schedule_interval=job_schedule_interval,
    start_date=job_start_date,
    slack_channel=OMNIUX.omniux().alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    tags=[OMNIUX.team.name, "qualityscore"],
    run_only_latest=True
)

dag = ttd_dag.airflow_dag

dag_builder = ttd_dag

for folder in whip_external_folders:
    recency_task_id = f"recency_check_{folder}"
    recency_operator_step = OpTask(
        op=DatasetRecencyOperator(
            dag=dag,
            datasets_input=[CtvDatasources.whip_external(folder)],
            cloud_provider=CloudProviders.aws,
            lookback_days=7,
            xcom_push=True,
            task_id=recency_task_id,
            connection_id="aws_whip_media",
        )
    )

    whip_import_task = DatasetTransferTask(
        name=f'import_{folder}_from_whip',
        dataset=CtvDatasources.whip_external(folder),
        dst_dataset=CtvDatasources.whip_internal(folder),
        src_cloud_provider=CloudProviders.aws,
        dst_cloud_provider=CloudProviders.aws,
        src_conn_id="aws_whip_media",
        partitioning_args={"ds_date": f"{{{{ task_instance.xcom_pull(task_ids='{recency_task_id}', key='old').strftime('%Y-%m-%d') }}}}"},
        force_indirect_copy=True,
        max_threads=1
    )

    dag_builder = dag_builder >> recency_operator_step >> whip_import_task

dag = ttd_dag.airflow_dag
