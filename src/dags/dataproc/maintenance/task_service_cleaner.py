import logging
from datetime import datetime, timedelta
from typing import List, Optional

from airflow.operators.python import PythonOperator
from dateutil import parser
from kubernetes.client import CoreV1Api
from kubernetes.client.models import V1PersistentVolumeClaim, V1Pod
from slack.errors import SlackApiError

from ttd.cloud_provider import CloudProvider
from ttd.eldorado.base import TtdDag
from ttd.operators.task_service_operator import TaskServiceOperator
from ttd.slack.slack_groups import dataproc
from ttd.task_service.k8s_connection_helper import (
    K8sSovereignConnectionHelper,
    aws,
    azure,
    alicloud,
)
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageType,
)
from ttd.tasks.op import OpTask
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge
from ttd.ttdslack import get_slack_client

dag_name = "task-service-cleaning"
secrets_file_prefix = "pre-init"
config_map_name = "task-service-versions"
secret_and_version_expiration_period = timedelta(days=28)
expired_pvc_deletion_period = timedelta(days=7)

dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2022, 11, 13),
    schedule_interval=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=5),
    run_only_latest=True,
    slack_channel=dataproc.alarm_channel,
    slack_alert_only_for_prod=False,
    tags=["DATAPROC", "Maintenance"],
    default_args={"owner": "DATAPROC"},
)
adag = dag.airflow_dag


def clean_pvcs(k8s_sovereign_connection_helper: K8sSovereignConnectionHelper) -> None:
    k8s_api_client = k8s_sovereign_connection_helper.cleaner.get_k8s_api_client()

    pvcs = k8s_api_client.list_namespaced_persistent_volume_claim(TaskServiceOperator.task_service_namespace).items
    pods = k8s_api_client.list_namespaced_pod(TaskServiceOperator.task_service_namespace).items

    expired_pvcs = get_expired_pvcs(client=k8s_api_client, pvcs=pvcs)
    send_slack_notifications(expired_pvcs)

    pvcs_ready_for_deletion = get_pvcs_ready_for_deletion(pvcs=pvcs)
    unmounted_temp_pvcs = get_unmounted_temp_pvcs(pvcs=pvcs, pods=pods)
    to_delete_pvcs = pvcs_ready_for_deletion + unmounted_temp_pvcs

    [
        k8s_api_client.delete_namespaced_persistent_volume_claim(pvc.metadata.name, TaskServiceOperator.task_service_namespace)
        for pvc in to_delete_pvcs
    ]

    push_pvc_metrics(
        all_pvcs=pvcs,
        deleted_pvcs=to_delete_pvcs,
        cloud_provider=k8s_sovereign_connection_helper.cloud_provider,
    )


def get_expired_pvcs(client: CoreV1Api, pvcs: List[V1PersistentVolumeClaim]) -> List[V1PersistentVolumeClaim]:
    expired_pvcs = []
    current_datetime = datetime.utcnow()

    for pvc in pvcs:
        last_used_at = parser.parse(pvc.metadata.annotations["last-used-at"])
        inactive_days_before_expire = timedelta(days=int(pvc.metadata.annotations.get("inactive-days-before-expire", 0)))
        pvc_age = current_datetime - last_used_at
        if ("delete-after" not in pvc.metadata.annotations and "inactive-days-before-expire" in pvc.metadata.annotations
                and pvc_age > inactive_days_before_expire):
            pvc.metadata.annotations["delete-after"] = (current_datetime + expired_pvc_deletion_period).isoformat()
            logging.info(f"Found expired PVC={pvc.metadata.name}, age={pvc_age}, inactive days before expire={inactive_days_before_expire}")
            logging.info(f"Updating delete-after annotation={pvc.metadata.annotations['delete-after']}")
            client.patch_namespaced_persistent_volume_claim(
                name=pvc.metadata.name,
                namespace=TaskServiceOperator.task_service_namespace,
                body=pvc,
            )
            expired_pvcs.append(pvc)

    return expired_pvcs


def get_pvcs_ready_for_deletion(pvcs: List[V1PersistentVolumeClaim], ) -> List[V1PersistentVolumeClaim]:
    pvcs_ready_for_deletion = []
    current_datetime = datetime.utcnow()

    for pvc in pvcs:
        if ("delete-after" in pvc.metadata.annotations and current_datetime >= parser.parse(pvc.metadata.annotations["delete-after"])):
            logging.info(f"Found to-delete PVC={pvc.metadata.name}, delete-after={pvc.metadata.annotations['delete-after']}")
            pvcs_ready_for_deletion.append(pvc)

    return pvcs_ready_for_deletion


def get_unmounted_temp_pvcs(pvcs: List[V1PersistentVolumeClaim], pods: List[V1Pod]) -> List[V1PersistentVolumeClaim]:
    temp_pvcs = [pvc for pvc in pvcs if pvc.metadata.labels["type"] == PersistentStorageType.ONE_POD_TEMP.value]
    temp_pvc_names = {pvc.metadata.name for pvc in temp_pvcs}
    mounted_temp_pvc_names = set()

    for pod in pods:
        if pod.spec.volumes is not None:
            for volume in pod.spec.volumes:
                pvc = volume.persistent_volume_claim
                if pvc is not None and pvc.claim_name in temp_pvc_names:
                    mounted_temp_pvc_names.add(pvc.claim_name)
    unmounted_temp_pvc_names = temp_pvc_names - mounted_temp_pvc_names
    unmounted_temp_pvcs = [pvc for pvc in temp_pvcs if pvc.metadata.name in unmounted_temp_pvc_names]

    for pvc in unmounted_temp_pvcs:
        logging.info(f"Found unmounted temporary PVC={pvc.metadata.name}, last-used-at={pvc.metadata.annotations['last-used-at']}")

    return unmounted_temp_pvcs


def send_slack_notifications(expired_pvcs: List[V1PersistentVolumeClaim]) -> None:
    slack_client = get_slack_client()
    init_text_placeholder = "Persistent Storage for TaskService task `{task_name}` is expired and will be deleted in {days} days. To retain this storage, contact #dev-taskservice or #scrum-dp-dataproc\n\nDetails in :thread:"
    details_text_placeholder = ("```PVC={pvc_name}\n\nAnnotations={annotations}\n\nLabels={labels}```")
    for pvc in expired_pvcs:
        channel = pvc.metadata.annotations["slack-alarm-channel"]
        try:
            init_message = slack_client.chat_postMessage(
                channel=channel,
                text=init_text_placeholder.format(
                    task_name=pvc.metadata.annotations["task-name"],
                    days=expired_pvc_deletion_period.days,
                ),
            )
            slack_client.chat_postMessage(
                channel=channel,
                thread_ts=init_message["message"]["ts"],
                text=details_text_placeholder.format(
                    pvc_name=pvc.metadata.name,
                    annotations=pvc.metadata.annotations,
                    labels=pvc.metadata.labels,
                ),
            )
        except SlackApiError as e:
            logging.error(f"Failed to post message in channel {channel}", exc_info=e)
            raise e


def push_pvc_metrics(
    all_pvcs: Optional[List[V1PersistentVolumeClaim]],
    deleted_pvcs: Optional[List[V1PersistentVolumeClaim]],
    cloud_provider: CloudProvider,
) -> None:
    logging.info("Pushing metrics")
    task_service_persistent_storage_one_pod_total_size_gi: TtdGauge = get_or_register_gauge(
        job=dag_name,
        name="task_service_persistent_storage_one_pod_total_size_gi",
        description="Total size in Gi of one-pod and one-pod-temp persistent storages"
    )
    task_service_persistent_storage_many_pods_total_size_gi: TtdGauge = get_or_register_gauge(
        job=dag_name,
        name="task_service_persistent_storage_many_pods_total_size_gi",
        description="Total size in Gi of many-pods persistent storages",
    )
    task_service_persistent_storage_deleted: TtdGauge = get_or_register_gauge(
        job=dag_name,
        name="task_service_persistent_storage_deleted",
        description="The number of abandoned and expired persistent storages deleted",
    )

    active_pvc_names = set([pvc.metadata.name for pvc in all_pvcs]) - set([pvc.metadata.name for pvc in deleted_pvcs])  # type: ignore
    active_pvcs = [pvc for pvc in all_pvcs if pvc.metadata.name in active_pvc_names]  # type: ignore

    one_pod_total_size_gi = 0
    many_pods_total_size_gi = 0

    for pvc in active_pvcs:
        storage_type = pvc.metadata.labels["type"]
        size_str = pvc.status.capacity["storage"]
        size_num = int(size_str.rstrip("Gi"))
        if (storage_type == PersistentStorageType.ONE_POD.value or storage_type == PersistentStorageType.ONE_POD_TEMP.value):
            one_pod_total_size_gi += size_num
        elif storage_type == PersistentStorageType.MANY_PODS.value:
            many_pods_total_size_gi += size_num

    deleted_pvc_num = len(deleted_pvcs)  # type: ignore

    gauge_labels = {"cloud_provider": cloud_provider.__str__()}  # type: ignore

    task_service_persistent_storage_one_pod_total_size_gi.labels(gauge_labels).set(one_pod_total_size_gi)
    task_service_persistent_storage_many_pods_total_size_gi.labels(gauge_labels).set(many_pods_total_size_gi)
    task_service_persistent_storage_deleted.labels(gauge_labels).set(deleted_pvc_num)

    push_all(job=dag_name)


task_info = [
    ("task-service-clean-pvcs", clean_pvcs, [aws, azure, alicloud]),
]
for task_id, python_callable, k8s_sovereign_connection_helpers in task_info:
    for k8s_sovereign_connection_helper in k8s_sovereign_connection_helpers:
        clean_task = OpTask(
            op=PythonOperator(
                task_id=f"{task_id}-{k8s_sovereign_connection_helper.cloud_provider}",
                python_callable=python_callable,
                op_kwargs={"k8s_sovereign_connection_helper": k8s_sovereign_connection_helper},
                dag=adag,
            )
        )
        dag >> clean_task
