import json
import time
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator, ShortCircuitOperator

from ttd.eldorado.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.slack.slack_groups import cmkt
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdprometheus import get_or_register_counter, push_all
from ttd.ttdrequests import get_request_session, install_certificates

_TTD_TENANT_ID = 1

# Environment based configuration. Default to Production config
_max_active_runs = 1
_env_name = None
_slack_channel = None
_product_catalog_url = None

_dag_id = "cmkt-elasticsearch-reindex"

_number_of_candidates_to_retrieve = 30

_post_reindex_status_max_tries = 3
_post_reindex_status_retry_delay_seconds = 10

_k8s_namespace = "ttd-retail-airflow-tasks"
# Details on this ServiceAccount and RBAC required is at https://atlassian.thetradedesk.com/confluence/x/q6GdEg
_k8s_service_account_name = "airflow-pod-scheduling"
# This is stored as a Vault secret at https://vault.adsrvr.org/ui/vault/secrets/secret/kv/SCRUM-CMKT%2FAirflow%2FConnections%2F/directory?namespace=team-secrets
# More details at: https://atlassian.thetradedesk.com/confluence/x/q6GdEg
_kubernetes_connection_id = None
_docker_image_reindex = "production.docker.adsrvr.org/elasticsearchreindex:0.1.346"

_ttd_env_env_var = "TTD_ENV"
_reindexer_aws_access_key_env_var = "REINDEXER_AWS_ACCESS_KEY"
_reindexer_aws_secret_key_env_var = "REINDEXER_AWS_SECRET_KEY"
_elasticsearch_password_env_var = "REINDEXER_ELASTICSEARCH_PASSWORD"

_get_reindex_candidates_task_id = "get-reindex-candidates"

# secrets
_aws_config_k8s_secret_name = "reindexer-aws-config"
_aws_config_aws_access_key = "aws-access-key"
_aws_config_aws_secret_key = "aws-secret-key"
_elasticsearch_config_k8s_secret_name = "reindexer-elasticsearch-config"
_elasticsearch_config_password = "password"

_reindexer_secrets = [
    Secret(
        deploy_type="env",
        deploy_target=_elasticsearch_password_env_var,
        secret=_elasticsearch_config_k8s_secret_name,
        key=_elasticsearch_config_password,
    ),
    Secret(
        deploy_type="env",
        deploy_target=_reindexer_aws_access_key_env_var,
        secret=_aws_config_k8s_secret_name,
        key=_aws_config_aws_access_key,
    ),
    Secret(
        deploy_type="env",
        deploy_target=_reindexer_aws_secret_key_env_var,
        secret=_aws_config_k8s_secret_name,
        key=_aws_config_aws_secret_key,
    ),
]

_reindex_k8s_resources = PodResources(
    request_cpu="1",
    request_memory="2Gi",
    limit_memory="4Gi",
    limit_ephemeral_storage="4Gi",
)

_k8s_annotations = {
    "sumologic.com/include": "true",
    "sumologic.com/sourceCategory": _dag_id,
}

_environment = TtdEnvFactory.get_from_system()
if _environment == TtdEnvFactory.prodTest:
    _env_name = "Sandbox"
    _slack_channel = "#cmkt-test-alarms"
    _product_catalog_url = "https://productcatalog-endor.dev.gen.adsrvr.org"
    _kubernetes_connection_id = "airflow-2-pod-scheduling-rbac-conn-non-prod"
elif _environment == TtdEnvFactory.prod:
    _env_name = "Production"
    _slack_channel = "#scrum-cmkt-alarms"
    _product_catalog_url = "https://productcatalog-endor.gen.adsrvr.org"
    _kubernetes_connection_id = "airflow-2-pod-scheduling-rbac-conn-prod"

if TtdEnvFactory.get_from_system() != TtdEnvFactory.prod:
    _number_of_candidates_to_retrieve = 10

# Initialize metrics

_reindex_post_status_error_counter = get_or_register_counter(
    _dag_id, "productcatalog_elasticsearch_reindex_post_status_error_count",
    "A counter of the number of errors from POST elasticsearch/reindex/status", ["status_code", "has_exception"]
)
_reindex_post_status_error_counter.labels(status_code="400", has_exception="True").inc(0)
_reindex_post_status_error_counter.labels(status_code="400", has_exception="False").inc(0)
_reindex_post_status_error_counter.labels(status_code="None", has_exception="True").inc(0)

_reindex_task_error_counter = get_or_register_counter(
    _dag_id, "productcatalog_elasticsearch_reindex_task_error_count", "A counter of the number of errors in the reindex task",
    ["merchant_id"]
)

_ttd_dag = TtdDag(
    dag_id=_dag_id,
    slack_channel=_slack_channel,
    enable_slack_alert=False,
    start_date=datetime(2025, 5, 27),
    schedule_interval="0/90 * * * *",
    retries=2,
    retry_delay=timedelta(minutes=1),
    max_active_runs=_max_active_runs,
    tags=[
        cmkt.jira_team,
    ],
)

_airflow_dag = _ttd_dag.airflow_dag


def get_candidates_to_reindex(**kwargs):
    candidates_override = kwargs['dag_run'].conf.get("candidates")

    if candidates_override is not None:
        validate_candidates_override(candidates_override)
        xcom_push_candidates_to_reindex(candidates_override, **kwargs)
        return

    response = None

    try:
        request_url = f"{_product_catalog_url}/elasticsearch/reindex/candidates"
        print(f"Sending request to {request_url} to get candidates to reindex")
        response = get_request_session().get(
            request_url,
            params={
                "top": str(_number_of_candidates_to_retrieve),
                "tenantId": str(_TTD_TENANT_ID),
            },
            headers={'Content-Type': 'application/json'},
            verify=install_certificates(),
        )
        print(f"Get reindex candidates returned status code {response.status_code} and content {response.text}")
        candidates = response.json()
    except Exception as ex:
        print(f"Failed to retrieve reindex candidates. Response: {response}.\n {ex}")
        raise

    xcom_push_candidates_to_reindex(candidates, **kwargs)


def validate_candidates_override(candidates):
    if not isinstance(candidates, list):
        raise Exception(f"candidates is not a list. Input: {candidates}")
    for candidate in candidates:
        for field in ["merchantId", "cloudServiceId"]:
            if candidate.get(field) is None:
                raise Exception(f"{field} is required in candidates override. Input: {candidate}")


def xcom_push_candidates_to_reindex(candidates, **kwargs):
    total_number_of_candidates = len(candidates)
    for i in range(total_number_of_candidates):
        kwargs["ti"].xcom_push(
            key=f"candidate_{i}",
            value=candidates[i],
        )


def xcom_pull_candidate_to_reindex(candidate_id, **kwargs):
    return kwargs["ti"].xcom_pull(task_ids=_get_reindex_candidates_task_id, key=f"candidate_{candidate_id}")


_get_candidates_to_reindex_optask = OpTask(
    op=PythonOperator(
        task_id=_get_reindex_candidates_task_id,
        python_callable=get_candidates_to_reindex,
        provide_context=True,
        dag=_airflow_dag,
    )
)


def new_skip_on_empty_candidate_optask(candidate_id):
    return OpTask(
        op=ShortCircuitOperator(
            task_id=f"skip-on-empty-candidate-{candidate_id}",
            provide_context=True,
            op_kwargs={
                "candidate_id": candidate_id,
            },
            python_callable=xcom_pull_candidate_to_reindex,
            dag=_airflow_dag,
        )
    )


def start_reindex(candidate_id, **kwargs):
    reindex_start_params_override = kwargs['dag_run'].conf.get("reindexStartParams")
    merchant_id = xcom_pull_candidate_to_reindex(candidate_id, **kwargs)["merchantId"]

    if reindex_start_params_override is not None:
        validate_reindex_start_params_override(reindex_start_params_override)
        for params_per_merchant in reindex_start_params_override:
            if params_per_merchant["merchantId"] == merchant_id:
                xcom_push_reindex_start_params(params_per_merchant, **kwargs)
                return

    request_url = f"{_product_catalog_url}/elasticsearch/reindex/status"
    print(f"Sending request to {request_url} to start reindex for Merchant ID {merchant_id}")
    response = get_request_session().post(
        request_url,
        data=json.dumps({
            "merchantId": merchant_id,
            "status": "InProgress",
        }),
        headers={'Content-Type': 'application/json'},
        verify=install_certificates(),
    )
    print(f"Post reindex status returned status code {response.status_code} and content {response.text}")
    if response.status_code != 200:
        raise Exception("POST elasticsearch/reindex/status returned non 200 OK response")

    parsed_response = response.json()
    xcom_push_reindex_start_params(parsed_response, **kwargs)


def validate_reindex_start_params_override(reindex_start_params):
    if not isinstance(reindex_start_params, list):
        raise Exception(f"reindexStartParams is not an array. Input: {reindex_start_params}")
    for params_per_merchant in reindex_start_params:
        for field in ["merchantId", "indexName", "bucketName", "snapshotPrefix", "cloudServiceId"]:
            if params_per_merchant.get(field) is None:
                raise Exception(f"{field} is required in candidates override. Input: {params_per_merchant}")


def xcom_push_reindex_start_params(reindex_start_params, **kwargs):
    kwargs["ti"].xcom_push(
        key="reindex_start_params",
        value=reindex_start_params,
    )


def xcom_pull_reindex_start_params(start_task_id, **kwargs):
    return kwargs["ti"].xcom_pull(task_ids=start_task_id, key="reindex_start_params")


def new_start_reindex_optask(candidate_id) -> OpTask:
    task_id = f"start-reindex-{candidate_id}"
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            python_callable=start_reindex,
            provide_context=True,
            dag=_airflow_dag,
            op_kwargs={"candidate_id": candidate_id},
        )
    )


def post_reindex_status(start_task_id, new_status, fail_reason="", **kwargs):
    reindex_start_params = xcom_pull_reindex_start_params(start_task_id, **kwargs)
    merchant_id = reindex_start_params["merchantId"]

    if is_reindex_start_params_overridden(merchant_id, **kwargs):
        return

    index_name = reindex_start_params["indexName"]
    session = get_request_session()
    exception_occurred = False
    final_http_status_code = None
    data = json.dumps({
        "status": new_status,
        "merchantId": merchant_id,
        "indexName": index_name,
        "failedReason": fail_reason,
    })
    if new_status == "Failed":
        _reindex_task_error_counter.labels(merchant_id=merchant_id).inc(1)
        push_all(_dag_id)

    for _ in range(_post_reindex_status_max_tries):
        final_http_status_code = None
        try:
            request_url = f"{_product_catalog_url}/elasticsearch/reindex/status"
            print(f"Sending request to {request_url} to end reindex for Merchant ID {merchant_id}")
            response = session.post(
                request_url,
                headers={'Content-Type': 'application/json'},
                data=data,
                verify=install_certificates(),
            )
            print(f"Post reindex status for {data} returned status code {response.status_code} and content {response.text}")
            if response.status_code == 200:
                return

            final_http_status_code = response.status_code
            if final_http_status_code == 400:
                break
        except Exception as ex:
            exception_occurred = True
            print(f"Post reindex status for {data} resulted in an exception. {ex}")

        print(f"Waiting for {_post_reindex_status_retry_delay_seconds} seconds before next try")
        time.sleep(_post_reindex_status_retry_delay_seconds)

    _reindex_post_status_error_counter.labels(status_code=str(final_http_status_code), has_exception=str(exception_occurred)).inc(1)
    push_all(_dag_id)
    raise Exception


def is_reindex_start_params_overridden(merchant_id, **kwargs):
    reindex_start_params_override = kwargs['dag_run'].conf.get("reindexStartParams")
    if reindex_start_params_override is None:
        return False
    for params_per_merchant in reindex_start_params_override:
        if params_per_merchant["merchantId"] == merchant_id:
            return True
    return False


def new_end_reindex_after_success_optask(candidate_id, start_task_id):
    task_id = f"end-reindex-{candidate_id}-success"
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            provide_context=True,
            op_kwargs={
                "start_task_id": start_task_id,
                "new_status": "Successful"
            },
            python_callable=post_reindex_status,
            dag=_airflow_dag,
            trigger_rule='all_success'
        )
    )


def new_end_reindex_after_fail_optask(candidate_id, start_task_id):
    task_id = f"end-reindex-{candidate_id}-fail"
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            provide_context=True,
            op_kwargs={
                "start_task_id": start_task_id,
                "new_status": "Failed",
                "fail_reason": "Failed in dag run_id: {{ run_id }}"
            },
            python_callable=post_reindex_status,
            dag=_airflow_dag,
            trigger_rule='one_failed'
        )
    )


def new_reindex_optask(candidate_id, start_task_id):
    task_id = f"reindex-{candidate_id}"
    return OpTask(
        op=TtdKubernetesPodOperator(
            name=task_id,
            task_id=task_id,
            namespace=_k8s_namespace,
            image=_docker_image_reindex,
            resources=_reindex_k8s_resources,
            kubernetes_conn_id=_kubernetes_connection_id,
            image_pull_policy="Always",
            get_logs=True,
            is_delete_operator_pod=True,
            dag=_airflow_dag,
            startup_timeout_seconds=600,
            execution_timeout=timedelta(minutes=90),
            log_events_on_failure=True,
            service_account_name=_k8s_service_account_name,
            secrets=_reindexer_secrets,
            annotations=_k8s_annotations,
            do_xcom_push=True,
            env_vars={
                _ttd_env_env_var: _env_name,
            },
            arguments=[
                f"{{{{ task_instance.xcom_pull(task_ids='{start_task_id}', key='reindex_start_params') | tojson }}}}",
            ],
        )
    )


_ttd_dag >> _get_candidates_to_reindex_optask

for candidate_id in range(_number_of_candidates_to_retrieve):
    skip_on_empty_candidate_optask = new_skip_on_empty_candidate_optask(candidate_id)
    start_reindex_optask = new_start_reindex_optask(candidate_id)
    reindex_optask = new_reindex_optask(candidate_id, start_reindex_optask.task_id)
    end_reindex_after_success_optask = new_end_reindex_after_success_optask(candidate_id, start_reindex_optask.task_id)
    end_reindex_after_fail_optask = new_end_reindex_after_fail_optask(candidate_id, start_reindex_optask.task_id)

    _get_candidates_to_reindex_optask >> skip_on_empty_candidate_optask >> start_reindex_optask >> reindex_optask >> [
        end_reindex_after_success_optask, end_reindex_after_fail_optask
    ]
