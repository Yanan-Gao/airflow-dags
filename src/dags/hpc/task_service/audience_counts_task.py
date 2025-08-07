from datetime import datetime, timedelta

from dags.hpc.counts_datasources import CountsDatasources
from ttd.cloud_provider import CloudProviders
from ttd.eldorado.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import hpc
from ttd.tasks.op import OpTask
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask

dag = TtdDag(
    dag_id="audience-counts-task",
    start_date=datetime(2024, 12, 1, 0, 0),
    schedule_interval=timedelta(days=1),
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/VAACG',
    run_only_latest=True,
    tags=[hpc.jira_team]
)
airflow_dag = dag.airflow_dag

advertiser_ids = [
    "fyc7s1m", "wevmkz5", "jig5tyb", "5hga7ev", "2wswws9", "819y21h", "g9bppef", "3wltzsg", "3dv2pv3", "vttj6an", "i6iod49", "dz3pyga",
    "h3alyry"
]
audience_counts_task = OpTask(
    op=TaskServiceOperator(
        task_name="AudienceCountsTask",
        scrum_team=hpc,
        task_config_name="AudienceCountsTaskConfig",
        resources=TaskServicePodResources
        .custom(request_cpu="8", request_memory="32Gi", limit_memory="32Gi", limit_ephemeral_storage="4Gi"),
        task_execution_timeout=timedelta(hours=12),
        retries=3,
        configuration_overrides={
            "AudienceCountsTask.RunDateTime": "{{ dag_run.start_date.strftime(\"%Y-%m-%dT%H:00:00\") }}",
            "AudienceCountsTask.AdvertiserIds": ','.join(advertiser_ids),
            "AudienceCountsTask.MaxParallelCountsServiceRequests": "256",
            "AudienceCountsTask.RequestBatchSize": "1",
            "AudienceCountsTask.MaxRequestTries": "3",
            "AudienceCountsTask.RequestTimeoutMs": str(300 * 1000)
        }
    )
)

audience_counts_records_dataset = CountsDatasources.get_audience_counts_records_dataset()
historical_dataset_transfer_task = DatasetTransferTask(
    name="copy-audience-counts-records",
    dataset=audience_counts_records_dataset,
    src_cloud_provider=CloudProviders.aws,
    dst_cloud_provider=CloudProviders.aws,
    partitioning_args=audience_counts_records_dataset.get_partitioning_args(
        ds_date="{{ dag_run.start_date.strftime(\"%Y-%m-%d %H:00:00\") }}"
    ),
    dst_dataset=CountsDatasources.get_audience_counts_records_dataset(is_historical=True),
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=airflow_dag))

dag >> audience_counts_task >> historical_dataset_transfer_task >> final_dag_status_step
