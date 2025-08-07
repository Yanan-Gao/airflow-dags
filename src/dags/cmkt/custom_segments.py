from airflow.kubernetes.secret import Secret
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator

from ttd.eldorado.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import cmkt
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdprometheus import get_or_register_counter, push_all
from ttd.ttdrequests import get_request_session, install_certificates

from datetime import datetime, timedelta

import json
import time

# Environment based configuration. Default to Production config
_max_active_runs = 1
_env_name = "Production"
_slack_channel = "#scrum-cmkt-alarms"
_product_catalog_url = "https://productcatalog-endor.gen.adsrvr.org"
# This is stored as a Vault secret at https://vault.adsrvr.org/ui/vault/secrets/secret/kv/SCRUM-CMKT%2FAirflow%2FConnections%2F/directory?namespace=team-secrets
# More details at: https://atlassian.thetradedesk.com/confluence/x/q6GdEg
_kubernetes_connection_id = "airflow-2-pod-scheduling-rbac-conn-prod"

_dag_id = "cmkt-custom-segments"
_number_of_parallel_tasks = 5
_number_of_segments_to_retrieve = 10
_update_segment_status_max_tries = 3
_update_segment_status_retry_delay_seconds = 10
_k8s_namespace = "ttd-retail-airflow-tasks"
# Details on this ServiceAccount and RBAC required is at https://atlassian.thetradedesk.com/confluence/x/q6GdEg
_k8s_service_account_name = "airflow-pod-scheduling"
_docker_image_vertica_to_cloud = "production.docker.adsrvr.org/retailsegmentverticatocloud:0.1.300"
_docker_image_cloud_to_dataserver = "production.docker.adsrvr.org/retailsegmentcloudtodataserver:0.1.300"
_aws_config_k8s_secret_name = "retail-segments-aws-config"
_aws_config_aws_access_key = "aws-access-key"
_aws_config_aws_secret_key = "aws-secret-key"
_vertica_config_k8s_secret_name = "retail-segments-vertica-config"
_vertica_config_password = "password"

_update_segment_status_error_counter = get_or_register_counter(
    _dag_id, "custom_segment_update_segment_status_error_count", "A counter of the number of errors in the update-segment-status tasks",
    ["status_code", "has_exception"]
)

_ttd_env_env_var = "TTD_ENV"
_retail_segments_aws_access_key_env_var = "RETAIL_SEGMENTS_AWS_ACCESS_KEY"
_retail_segments_aws_secret_key_env_var = "RETAIL_SEGMENTS_AWS_SECRET_KEY"
_retail_segments_vertica_password_env_var = "RETAIL_SEGMENTS_VERTICA_PASSWORD"

_get_segments_to_refresh_task_id = "get-segments-to-refresh"
_skip_on_empty_segments_task_id_format_string = "skip-on-empty-segments-{}"
_vertica_to_cloud_task_id_format_string = "vertica-to-cloud-{}"
_post_vertica_to_cloud_branch_task_id_format_string = "post-vertica-to-cloud-branch-{}"
_post_vertica_to_cloud_update_segment_status_task_id_format_string = "post-vertica-to-cloud-update-segment-status-{}"
_cloud_to_dataserver_task_id_format_string = "cloud-to-dataserver-{}"
_post_cloud_to_dataserver_update_segment_status_task_id_format_string = "post-cloud-to-dataserver-update-segment-status-{}"

_segment_refresh_results_key = "segmentRefreshResults"
_segments_to_populate_key = "segmentsToPopulate"
_kubernetes_pod_operator_xcom_output_key = "return_value"

_vertica_to_cloud_k8s_resources = PodResources(request_cpu="0.5", request_memory="2Gi", limit_memory="4Gi", limit_ephemeral_storage="3Gi")

_cloud_to_dataserver_k8s_resources = PodResources(
    request_cpu="0.5",
    request_memory="2Gi",
    limit_memory="4Gi",
    limit_ephemeral_storage="3Gi",
)

_k8s_annotations = {
    "sumologic.com/include": "true",
    "sumologic.com/sourceCategory": _dag_id,
}

_vertica_to_cloud_secrets = [
    Secret(
        deploy_type="env",
        deploy_target=_retail_segments_vertica_password_env_var,
        secret=_vertica_config_k8s_secret_name,
        key=_vertica_config_password,
    ),
    Secret(
        deploy_type="env",
        deploy_target=_retail_segments_aws_access_key_env_var,
        secret=_aws_config_k8s_secret_name,
        key=_aws_config_aws_access_key,
    ),
    Secret(
        deploy_type="env",
        deploy_target=_retail_segments_aws_secret_key_env_var,
        secret=_aws_config_k8s_secret_name,
        key=_aws_config_aws_secret_key,
    ),
]
_cloud_to_dataserver_secrets = [
    Secret(
        deploy_type="env",
        deploy_target=_retail_segments_aws_access_key_env_var,
        secret=_aws_config_k8s_secret_name,
        key=_aws_config_aws_access_key,
    ),
    Secret(
        deploy_type="env",
        deploy_target=_retail_segments_aws_secret_key_env_var,
        secret=_aws_config_k8s_secret_name,
        key=_aws_config_aws_secret_key,
    ),
]

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest:
    _env_name = "Sandbox"
    _slack_channel = "#cmkt-test-alarms"
    _product_catalog_url = "https://productcatalog-endor.dev.gen.adsrvr.org"
    _kubernetes_connection_id = "airflow-2-pod-scheduling-rbac-conn-non-prod"

# Initialize metrics
_update_segment_status_error_counter.labels(status_code="400", has_exception="True").inc(0)
_update_segment_status_error_counter.labels(status_code="400", has_exception="False").inc(0)
_update_segment_status_error_counter.labels(status_code="None", has_exception="True").inc(0)

_ttd_dag = TtdDag(
    dag_id=_dag_id,
    slack_channel=_slack_channel,
    enable_slack_alert=False,
    start_date=datetime(2024, 8, 6),
    schedule_interval="0/30 * * * *",
    retries=2,
    max_active_runs=_max_active_runs,
    retry_delay=timedelta(minutes=1),
    tags=[
        cmkt.jira_team,
    ],
)

_airflow_dag = _ttd_dag.airflow_dag


def get_task_instance(**kwargs):
    return kwargs["ti"]


def get_segments_to_refresh(**kwargs):
    segments = None
    response = None

    try:
        response = get_request_session().get(
            "{}/segmentrefresh/candidates".format(_product_catalog_url),
            params={
                "top": str(_number_of_segments_to_retrieve),
            },
            verify=install_certificates(),
        )
        segments = response.json()
    except Exception as ex:
        print("Failed to retrieve segment refresh candidates. Response: {}.\n {}".format(response, ex))
        raise

    total_number_of_segments = len(segments)
    base_number_of_segments_per_task = total_number_of_segments // _number_of_parallel_tasks
    remaining_number_of_segments = total_number_of_segments - base_number_of_segments_per_task * _number_of_parallel_tasks

    segments_start_idx = 0
    for i in range(_number_of_parallel_tasks):
        number_of_segments_for_current_task = base_number_of_segments_per_task
        if i < remaining_number_of_segments:
            number_of_segments_for_current_task += 1
        end_idx = segments_start_idx + number_of_segments_for_current_task
        get_task_instance(**kwargs).xcom_push(
            key=str(i),
            value=list(segments[segments_start_idx:end_idx]),
        )
        segments_start_idx = end_idx


def skip_on_empty_segments(task_index, **kwargs):
    return get_task_instance(**kwargs).xcom_pull(task_ids=_get_segments_to_refresh_task_id, key=str(task_index))


def new_skip_on_empty_segments_optask(task_index):
    return OpTask(
        op=ShortCircuitOperator(
            task_id=_skip_on_empty_segments_task_id_format_string.format(task_index),
            provide_context=True,
            op_kwargs={
                "task_index": task_index,
            },
            python_callable=skip_on_empty_segments,
            dag=_airflow_dag,
        )
    )


def new_vertica_to_cloud_optask(task_index):
    task_id = _vertica_to_cloud_task_id_format_string.format(task_index)
    return OpTask(
        op=TtdKubernetesPodOperator(
            name=task_id,
            task_id=task_id,
            namespace=_k8s_namespace,
            image=_docker_image_vertica_to_cloud,
            resources=_vertica_to_cloud_k8s_resources,
            kubernetes_conn_id=_kubernetes_connection_id,
            image_pull_policy="Always",
            get_logs=True,
            is_delete_operator_pod=True,
            dag=_airflow_dag,
            startup_timeout_seconds=600,
            execution_timeout=timedelta(minutes=30),
            log_events_on_failure=True,
            service_account_name=_k8s_service_account_name,
            secrets=_vertica_to_cloud_secrets,
            annotations=_k8s_annotations,
            do_xcom_push=True,
            env_vars={
                _ttd_env_env_var: _env_name,
            },
            arguments=[
                "{{ run_id }}",
                task_id,
                f"{{{{ task_instance.xcom_pull(task_ids='{_get_segments_to_refresh_task_id}', key='{task_index}') | tojson }}}}",
            ],
        )
    )


def post_vertica_to_cloud_branch(vertica_to_cloud_task_id, update_segment_status_task_id, cloud_to_dataserver_task_id, **kwargs):
    next_task_ids = []
    vertica_to_cloud_xcom_output = get_task_instance(**kwargs).xcom_pull(
        task_ids=vertica_to_cloud_task_id,
        key=_kubernetes_pod_operator_xcom_output_key,
    )
    if _segment_refresh_results_key in vertica_to_cloud_xcom_output and vertica_to_cloud_xcom_output[_segment_refresh_results_key]:
        next_task_ids.append(update_segment_status_task_id)
    if _segments_to_populate_key in vertica_to_cloud_xcom_output and vertica_to_cloud_xcom_output[_segments_to_populate_key]:
        next_task_ids.append(cloud_to_dataserver_task_id)
    return next_task_ids


def new_post_vertica_to_cloud_branch_optask(
    task_index, vertica_to_cloud_task_id, update_segment_status_task_id, cloud_to_dataserver_task_id
):
    task_id = _post_vertica_to_cloud_branch_task_id_format_string.format(task_index)
    return OpTask(
        op=BranchPythonOperator(
            task_id=task_id,
            provide_context=True,
            op_kwargs={
                "vertica_to_cloud_task_id": vertica_to_cloud_task_id,
                "update_segment_status_task_id": update_segment_status_task_id,
                "cloud_to_dataserver_task_id": cloud_to_dataserver_task_id,
            },
            python_callable=post_vertica_to_cloud_branch,
            dag=_airflow_dag,
        )
    )


def update_segment_status(xcom_pull_task_id, dag_run_id, **kwargs):
    data_pulled_from_xcom = get_task_instance(**kwargs).xcom_pull(task_ids=xcom_pull_task_id, key=_kubernetes_pod_operator_xcom_output_key)
    print("data_pulled_from_xcom = {}".format(data_pulled_from_xcom))
    session = get_request_session()

    has_error = False
    for segment_refresh_result in data_pulled_from_xcom[_segment_refresh_results_key]:
        success = update_one_segment_status(dag_run_id, xcom_pull_task_id, session, segment_refresh_result)
        if not success:
            has_error = True
    push_all(_dag_id)

    if has_error:
        raise Exception("Some of the segment refresh status update failed")


def update_one_segment_status(dag_run_id, dag_task_id, session, segment_refresh_result):
    """
    update the status of the segment refresh
    :return: True if the status is updated successfully, otherwise False
    """
    final_http_status_code = None
    exception_occurred = False
    data = json.dumps({
        "dagRunId": dag_run_id,
        "dagTaskId": dag_task_id,
        "targetingDataId": str(segment_refresh_result["targetingDataId"]),
        "status": str(segment_refresh_result["status"]),
        "userCount": segment_refresh_result["userCount"],
        "errorMessage": segment_refresh_result["errorMessage"],
    })

    for _ in range(_update_segment_status_max_tries):
        final_http_status_code = None
        try:
            response = session.post(
                "{}/segmentrefresh/status".format(_product_catalog_url),
                headers={'Content-Type': 'application/json'},
                data=data,
                verify=install_certificates(),
            )
            print("Update segment status for {} returned status code {} and content {}".format(data, response.status_code, response.text))
            if response.status_code == 200:
                return True

            final_http_status_code = response.status_code
            if final_http_status_code == 400:
                break
        except Exception as ex:
            exception_occurred = True
            print("Update segment status for {} resulted in an exception. {}".format(data, ex))

        print("Waiting for {} seconds before next try".format(_update_segment_status_retry_delay_seconds))
        time.sleep(_update_segment_status_retry_delay_seconds)

    _update_segment_status_error_counter.labels(status_code=str(final_http_status_code), has_exception=str(exception_occurred)).inc(1)
    return False


def new_update_segment_status_optask(task_id, xcom_pull_task_id):
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            provide_context=True,
            op_kwargs={
                "xcom_pull_task_id": xcom_pull_task_id,
                "dag_run_id": "{{ run_id }}",
            },
            python_callable=update_segment_status,
            dag=_airflow_dag,
        )
    )


def new_cloud_to_dataserver_optask(task_index, xcom_pull_task_id):
    task_id = _cloud_to_dataserver_task_id_format_string.format(task_index)
    return OpTask(
        op=TtdKubernetesPodOperator(
            name=task_id,
            task_id=task_id,
            namespace=_k8s_namespace,
            image=_docker_image_cloud_to_dataserver,
            resources=_cloud_to_dataserver_k8s_resources,
            kubernetes_conn_id=_kubernetes_connection_id,
            image_pull_policy="Always",
            get_logs=True,
            is_delete_operator_pod=True,
            dag=_airflow_dag,
            startup_timeout_seconds=600,
            execution_timeout=timedelta(minutes=30),
            log_events_on_failure=True,
            service_account_name=_k8s_service_account_name,
            secrets=_cloud_to_dataserver_secrets,
            annotations=_k8s_annotations,
            do_xcom_push=True,
            env_vars={
                _ttd_env_env_var: _env_name,
            },
            arguments=[
                f"{{{{ task_instance.xcom_pull(task_ids='{xcom_pull_task_id}', key='{_kubernetes_pod_operator_xcom_output_key}') | tojson }}}}",
            ],
        )
    )


_get_segments_to_refresh_optask = OpTask(
    op=PythonOperator(
        task_id=_get_segments_to_refresh_task_id,
        python_callable=get_segments_to_refresh,
        provide_context=True,
        dag=_airflow_dag,
    )
)

_ttd_dag >> _get_segments_to_refresh_optask

for task_index in range(_number_of_parallel_tasks):
    skip_on_empty_segments_optask = new_skip_on_empty_segments_optask(task_index)

    vertica_to_cloud_optask = new_vertica_to_cloud_optask(task_index)

    post_vertica_to_cloud_update_segment_status_optask = new_update_segment_status_optask(
        _post_vertica_to_cloud_update_segment_status_task_id_format_string.format(task_index), vertica_to_cloud_optask.task_id
    )

    cloud_to_dataserver_optask = new_cloud_to_dataserver_optask(task_index, vertica_to_cloud_optask.task_id)

    post_vertica_to_cloud_branch_optask = new_post_vertica_to_cloud_branch_optask(
        task_index, vertica_to_cloud_optask.task_id, post_vertica_to_cloud_update_segment_status_optask.task_id,
        cloud_to_dataserver_optask.task_id
    )

    post_cloud_to_dataserver_update_segment_status_optask = new_update_segment_status_optask(
        _post_cloud_to_dataserver_update_segment_status_task_id_format_string.format(task_index),
        cloud_to_dataserver_optask.task_id,
    )

    _get_segments_to_refresh_optask >> skip_on_empty_segments_optask >> vertica_to_cloud_optask >> post_vertica_to_cloud_branch_optask
    post_vertica_to_cloud_branch_optask >> post_vertica_to_cloud_update_segment_status_optask
    post_vertica_to_cloud_branch_optask >> cloud_to_dataserver_optask >> post_cloud_to_dataserver_update_segment_status_optask
