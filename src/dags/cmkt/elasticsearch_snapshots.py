from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import cmkt
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdprometheus import get_or_register_counter, push_all
from ttd.ttdrequests import get_request_session, install_certificates
from dags.cmkt.elasticsearch.cluster_tasks import new_cluster_task, new_snapshot_generator_job_task
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g

from datetime import datetime, timezone

import json
import time

# Environment based configuration. Default to Production config
_max_active_runs = 1
_slack_channel = None
_product_catalog_url = None
_output_key_prefix = ""

_environment = TtdEnvFactory.get_from_system()
if _environment == TtdEnvFactory.prodTest:
    _slack_channel = "#cmkt-test-alarms"
    _product_catalog_url = "https://productcatalog-endor.dev.gen.adsrvr.org"
    _output_key_prefix = "s3://ttd-product-catalog-elasticsearch-sandbox/env=test/"
elif _environment == TtdEnvFactory.prod:
    _slack_channel = "#scrum-cmkt-alarms"
    _product_catalog_url = "https://productcatalog-endor.gen.adsrvr.org"
    _output_key_prefix = "s3://ttd-product-catalog-elasticsearch/env=prod/"

_dag_id = "cmkt-elasticsearch-snapshots"

_post_snapshot_generation_status_max_tries = 3
_post_snapshot_generation_status_retry_delay_seconds = 10

_snapshots = [
    ("AdvertiserMerchantProduct", 2, [(M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1))]),
    ("CatalogList", 2, [(M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1))]),
    ("CatalogListItem", 2, [(M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1))]),
    ("MerchantCategory", 2, [(M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1))]),
    ("MerchantCategoryTaxonomyMapping", 2, [(M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1))]),
    ("MerchantProduct", 4, [(M7g.m7g_xlarge().with_ebs_size_gb(100).with_fleet_weighted_capacity(1))]),
]

# Initialize metrics
_snapshot_generator_post_status_error_counter = get_or_register_counter(
    _dag_id, "productcatalog_elasticsearch_snapshot_generator_post_status_error_count",
    "A counter of the number of errors in post_snapshot_generation_status within the end-snapshot-generation tasks", [
        "status_code", "has_exception"
    ]
)
_snapshot_generator_post_status_error_counter.labels(status_code="400", has_exception="True").inc(0)
_snapshot_generator_post_status_error_counter.labels(status_code="400", has_exception="False").inc(0)
_snapshot_generator_post_status_error_counter.labels(status_code="None", has_exception="True").inc(0)
_snapshot_generator_cluster_task_error_counter = get_or_register_counter(
    _dag_id, "productcatalog_elasticsearch_snapshot_generator_cluster_task_error_count",
    "A counter of the number of errors in the cluster task", ["snapshot"]
)
for snapshot, num_workers, instance_types in _snapshots:
    _snapshot_generator_cluster_task_error_counter.labels(snapshot=snapshot).inc(0)

_ttd_dag = TtdDag(
    dag_id=_dag_id,
    slack_channel=_slack_channel,
    enable_slack_alert=False,
    start_date=datetime(2025, 5, 30),
    schedule_interval="0 * * * *",
    max_active_runs=_max_active_runs,
    tags=[
        cmkt.jira_team,
    ],
)

_airflow_dag = _ttd_dag.airflow_dag


def start_snapshot_generation(snapshot, task_instance):
    task_instance.xcom_push(key="tenantId", value=1)
    response = None
    try:
        request_url = f"{_product_catalog_url}/elasticsearch/snapshot/{snapshot.lower()}/status"
        print(f"Sending request to {request_url} to begin snapshot generation")
        response = get_request_session(headers={
            'Content-Type': 'application/json'
        }).post(
            request_url,
            data=json.dumps({
                "tenantId": 1,
                "status": "InProgress"
            }),
            verify=install_certificates(),
        )
    except Exception as ex:
        print(f"Failed to connect to snapshot endpoint to start snapshot generation. Response: {response}.\n {ex}")
        raise

    parsed_response = response.json()
    task_instance.xcom_push(key="parsedResponse", value=parsed_response)
    for k in parsed_response:
        task_instance.xcom_push(key=k, value=parsed_response[k])

    if parsed_response['action'] == 'DoNothing':
        if snapshot == 'MerchantProduct':
            task_instance.xcom_push(key="partitionedOutputKey", value=parsed_response["previousSnapshotPrefix"])
        else:
            task_instance.xcom_push(key="outputKey", value=parsed_response["previousSnapshotPrefix"])
    else:
        started_at = datetime.strptime(parsed_response["startedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")

        task_instance.xcom_push(key="outputKey", value=construct_snapshot_prefix(snapshot, started_at))
        if snapshot == 'MerchantProduct':
            task_instance.xcom_push(key="partitionedOutputKey", value=construct_snapshot_prefix("MerchantProductByMerchantId", started_at))


def construct_snapshot_prefix(snapshot_type, datetime_value):
    return f"{_output_key_prefix}{snapshot_type}/date={datetime_value.strftime('%Y%m%d')}/time={datetime_value.strftime('%H%M%S')}/"


def next_task_given_action(snapshot, do_nothing_task_id, snapshot_generator_task_id, task_instance):
    action = task_instance.xcom_pull(task_ids=f"start-snapshot-generation-{snapshot}", key="action")
    if action == "DoNothing":
        return do_nothing_task_id
    else:
        return snapshot_generator_task_id


def new_start_snapshot_generation_optask(snapshot) -> OpTask:
    task_id = f"start-snapshot-generation-{snapshot}"
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            python_callable=start_snapshot_generation,
            provide_context=True,
            dag=_airflow_dag,
            op_kwargs={"snapshot": snapshot}
        )
    )


def post_snapshot_generation_status(
    snapshot,
    tenant_id,
    new_status,
    started_at=None,
    merchant_product_snapshot_started_at=None,
    merchant_product_details_started_at=None,
    output_key=None,
    partitioned_output_key=None,
    fail_reason=None
):
    session = get_request_session()
    exception_occurred = False
    final_http_status_code = None
    data = {
        "tenantId": tenant_id,
        "status": new_status,
    }
    if fail_reason is not None:
        data["failedReason"] = fail_reason
    if output_key is not None:
        data["snapshotPrefix"] = output_key
    if partitioned_output_key is not None:
        data["partitionedSnapshotPrefix"] = partitioned_output_key

    if snapshot == 'MerchantProductDetails':
        data["merchantProductSnapshotStartedAt"] = merchant_product_snapshot_started_at
        data["merchantProductDetailsSnapshotStartedAt"] = merchant_product_details_started_at
    else:
        data["startedAt"] = started_at

    if new_status == "Failed":
        _snapshot_generator_cluster_task_error_counter.labels(snapshot=snapshot).inc(1)
        push_all(_dag_id)

    for _ in range(_post_snapshot_generation_status_max_tries):
        final_http_status_code = None
        try:
            request_url = f"{_product_catalog_url}/elasticsearch/snapshot/{snapshot.lower()}/status"
            print(f"Sending request to {request_url} to end snapshot generation")
            response = session.post(
                request_url,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(data),
                verify=install_certificates(),
            )
            print(f"Post snapshot generation status for {data} returned status code {response.status_code} and content {response.text}")
            if response.status_code == 200:
                return

            final_http_status_code = response.status_code
            if final_http_status_code == 400:
                break
        except Exception as ex:
            exception_occurred = True
            print(f"Post snapshot generation status for {data} resulted in an exception. {ex}")

        print(f"Waiting for {_post_snapshot_generation_status_retry_delay_seconds} seconds before next try")
        time.sleep(_post_snapshot_generation_status_retry_delay_seconds)

    _snapshot_generator_post_status_error_counter.labels(
        status_code=str(final_http_status_code), has_exception=str(exception_occurred)
    ).inc(1)
    push_all(_dag_id)
    raise Exception


def new_end_snapshot_generation_after_success_optask(snapshot, start_task_id):
    task_id = f"end-snapshot-generation-{snapshot}-success"
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            provide_context=True,
            op_kwargs={
                "started_at":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='startedAt', default=None) }}",
                "tenant_id":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='tenantId') }}",
                "output_key":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='outputKey') }}",
                "partitioned_output_key":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='partitionedOutputKey', default=None) }}",
                "snapshot":
                snapshot,
                "new_status":
                "Successful",
                "merchant_product_snapshot_started_at":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='merchantProductSnapshotStartedAt', default=None) }}",
                "merchant_product_details_started_at":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='merchantProductDetailsStartedAt', default=None) }}"
            },
            python_callable=post_snapshot_generation_status,
            dag=_airflow_dag,
            trigger_rule='all_success'
        )
    )


def new_end_snapshot_generation_after_fail_optask(snapshot, start_task_id):
    task_id = f"end-snapshot-generation-{snapshot}-fail"
    return OpTask(
        op=PythonOperator(
            task_id=task_id,
            provide_context=True,
            op_kwargs={
                "started_at":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='startedAt', default=None) }}",
                "tenant_id":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='tenantId') }}",
                "snapshot":
                snapshot,
                "new_status":
                "Failed",
                "fail_reason":
                "Failed in dag run_id: {{ run_id }}",
                "merchant_product_snapshot_started_at":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='merchantProductSnapshotStartedAt', default=None) }}",
                "merchant_product_details_started_at":
                "{{ task_instance.xcom_pull(task_ids='" + start_task_id + "', key='merchantProductDetailsStartedAt', default=None) }}"
            },
            python_callable=post_snapshot_generation_status,
            dag=_airflow_dag,
            trigger_rule='one_failed'
        )
    )


def new_start_merchant_product_details_optask() -> OpTask:
    task_id = "start-merchantProductDetails"
    return OpTask(
        op=PythonOperator(
            task_id=task_id, python_callable=start_merchant_product_detail_generation, provide_context=True, dag=_airflow_dag
        )
    )


def start_merchant_product_detail_generation(task_instance):
    datetime_now = datetime.now(timezone.utc)
    task_instance.xcom_push(key="merchantProductDetailsStartedAt", value=datetime_now.strftime('%Y-%m-%dT%H:%M:%S'))
    merchant_product_details_output_key = f"{_output_key_prefix}MerchantProductDetails/date={datetime_now.strftime('%Y%m%d')}/time={datetime_now.strftime('%H%M%S')}/"
    task_instance.xcom_push(key="partitionedOutputKey", value=merchant_product_details_output_key)
    started_at = task_instance.xcom_pull(task_ids=tasks['MerchantProduct']['start'].task_id, key='startedAt')
    task_instance.xcom_push(key="merchantProductSnapshotStartedAt", value=started_at)
    tenant_id = task_instance.xcom_pull(task_ids=tasks['MerchantProduct']['start'].task_id, key='tenantId')
    task_instance.xcom_push(key="tenantId", value=tenant_id)
    # Fetch outputKeys from each snapshot generation task
    snapshot_prefixes = {
        "AdvertiserMerchantProduct":
        task_instance.xcom_pull(task_ids=tasks['AdvertiserMerchantProduct']['start'].task_id, key='outputKey'),
        "CatalogList":
        task_instance.xcom_pull(task_ids=tasks['CatalogList']['start'].task_id, key='outputKey'),
        "CatalogListItem":
        task_instance.xcom_pull(task_ids=tasks['CatalogListItem']['start'].task_id, key='outputKey'),
        "MerchantCategory":
        task_instance.xcom_pull(task_ids=tasks['MerchantCategory']['start'].task_id, key='outputKey'),
        "MerchantCategoryTaxonomyMapping":
        task_instance.xcom_pull(task_ids=tasks['MerchantCategoryTaxonomyMapping']['start'].task_id, key='outputKey'),
        "MerchantProductByMerchantId":
        task_instance.xcom_pull(task_ids=tasks['MerchantProduct']['start'].task_id, key='partitionedOutputKey')
    }

    task_instance.xcom_push(key="snapshotPrefixes", value=','.join(f'{k}={v}' for k, v in snapshot_prefixes.items()))
    task_instance.xcom_push(key="action", value='GenerateMerchantProductDetails')


tasks = {}
for snapshot, num_workers, instance_types in _snapshots:
    start_snapshot_generation_optask = new_start_snapshot_generation_optask(snapshot)
    snapshot_generator_cluster_task = new_cluster_task("snapshot-generator-" + snapshot, instance_types, num_workers)
    snapshot_generator_task = new_snapshot_generator_job_task(snapshot, start_snapshot_generation_optask.task_id)
    snapshot_generator_cluster_task.add_parallel_body_task(snapshot_generator_task)
    end_snapshot_generation_after_success_optask = new_end_snapshot_generation_after_success_optask(
        snapshot, start_snapshot_generation_optask.task_id
    )
    end_snapshot_generation_after_fail_optask = new_end_snapshot_generation_after_fail_optask(
        snapshot, start_snapshot_generation_optask.task_id
    )

    check_one_succeeded_optask = OpTask(
        op=EmptyOperator(
            task_id=f"check-one-succeeded-{snapshot}",
            trigger_rule="none_failed_min_one_success",
        )
    )

    continue_to_run_cluster_optask = OpTask(op=EmptyOperator(task_id=f"continue-to-run-cluster-{snapshot}"))

    branch_given_action_optask = OpTask(
        op=BranchPythonOperator(
            task_id=f"branch-{snapshot}",
            python_callable=next_task_given_action,
            op_kwargs={
                "snapshot": snapshot,
                "do_nothing_task_id": check_one_succeeded_optask.task_id,
                "snapshot_generator_task_id": continue_to_run_cluster_optask.task_id
            },
            provide_context=True,
            dag=_airflow_dag
        )
    )

    _ttd_dag >> start_snapshot_generation_optask >> branch_given_action_optask

    branch_given_action_optask >> continue_to_run_cluster_optask >> snapshot_generator_cluster_task >> [
        end_snapshot_generation_after_success_optask, end_snapshot_generation_after_fail_optask
    ]

    tasks[snapshot] = {
        "start": start_snapshot_generation_optask,
        "branch": branch_given_action_optask,
        "end_success": end_snapshot_generation_after_success_optask,
        "end_fail": end_snapshot_generation_after_fail_optask,
        "check_one_succeeded": check_one_succeeded_optask
    }
    branch_given_action_optask >> check_one_succeeded_optask
    end_snapshot_generation_after_success_optask >> check_one_succeeded_optask

# Execute MerchantProductDetails dependent on the success of all other snapshots
start_merchant_product_details_optask = new_start_merchant_product_details_optask()
merchant_product_details_cluster_task = new_cluster_task("snapshot-generator-MerchantProductDetails")
merchant_product_details_cluster_task.add_parallel_body_task(
    new_snapshot_generator_job_task('MerchantProductDetails', start_merchant_product_details_optask.task_id)
)

for task in tasks.values():
    task["check_one_succeeded"] >> start_merchant_product_details_optask

start_merchant_product_details_optask >> merchant_product_details_cluster_task

end_merchant_product_details_success_optask = new_end_snapshot_generation_after_success_optask(
    'MerchantProductDetails', start_merchant_product_details_optask.task_id
)
end_merchant_product_details_fail_optask = new_end_snapshot_generation_after_fail_optask(
    'MerchantProductDetails', start_merchant_product_details_optask.task_id
)

merchant_product_details_cluster_task >> [end_merchant_product_details_success_optask, end_merchant_product_details_fail_optask]
