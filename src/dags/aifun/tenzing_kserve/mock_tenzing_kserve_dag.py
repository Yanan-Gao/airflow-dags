from datetime import timedelta, datetime
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.tenzing_base_operator import (TenzingBaseOperator, ApiActionEnum, KserveClientEnvEnum)
from airflow.operators.python import PythonOperator
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import AIFUN
from dags.aifun.tenzing_kserve.endpoint_def_json import resource_assets

DAG_ID = "af_kserve_tenzing_mock_dag"
ttd_dag = TtdDag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 2, 4, 1, 0),
    schedule_interval=timedelta(hours=1),
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_tags=AIFUN.team.jira_team,
    enable_slack_alert=False,
)


def generate_base_ep_model_uri():
    return "s3://thetradedesk-mlplatform-us-east-1/mlops/model_serving/store/aifun/test_tf/vTest"


def generate_vmlf_ep_model_version():
    return "4"


ops = []
# example task where a model's URI is computed in a separate step
base_model_uri_xcom_op = OpTask(
    op=PythonOperator(task_id="base_model_uri_xcom_op", python_callable=generate_base_ep_model_uri, do_xcom_push=True)
)
vmlf_model_version_xcom_op = OpTask(
    op=PythonOperator(task_id="vmlf_model_version_xcom_op", python_callable=generate_vmlf_ep_model_version, do_xcom_push=True)
)

# you can then pull the value like so via xcom
base_model_uri_xcom_value = (f'{{{{ task_instance.xcom_pull(dag_id="{DAG_ID}", '
                             f'task_ids="{base_model_uri_xcom_op.task_id}") }}}}')
vmlf_model_version_xcom_value = (
    f'{{{{ task_instance.xcom_pull(dag_id="{DAG_ID}", '
    f'task_ids="{vmlf_model_version_xcom_op.task_id}") }}}}'
)

ops.append(base_model_uri_xcom_op)
ops.append(vmlf_model_version_xcom_op)

airflow_env = TtdEnvFactory.get_from_system()
kserve_envs = [airflow_env]
# we want to run the dev client from prod as well
if airflow_env == TtdEnvFactory.prod:
    kserve_envs.append(TtdEnvFactory.prodTest)
for env in kserve_envs:
    kserve_env = KserveClientEnvEnum.PROD if env == TtdEnvFactory.prod else KserveClientEnvEnum.NON_PROD
    # example op where user inputs resource as an endpoint configuration via the 'tenzing_endpoint_configuration_spec' param
    put1 = OpTask(
        op=TenzingBaseOperator(
            tenzing_unique_id="tf-mnist-af",
            tenzing_namespace="model-serving-aifun",
            tenzing_api_action=ApiActionEnum.PUT,
            tenzing_endpoint_configuration_spec={
                "endpoints": [
                    {
                        "name": "base-af",
                        "modelFormat": "tensorflow",
                        "sourceUri": base_model_uri_xcom_value,  # insert the pulled model URI value into your configuration
                    },
                    {
                        "name": "vmlf-af",
                        "modelFormat": "sklearn",
                        "sourceMlflowName": "iris_rf_test",
                        "sourceMlflowVersion":
                        vmlf_model_version_xcom_value  # insert the pulled model version value into your configuration
                    },
                    {
                        "name": "vpeg-batch-af",
                        "modelFormat": "tensorflow-batch",
                        "sourceUri": "s3://thetradedesk-mlplatform-us-east-1/mlops/model_serving/store/aifun/test_tf/vTest",
                        "tensorflowBatchingConfig": {
                            "maxBatchSize": 32,
                            "batchTimeoutMicros": 1000,
                        }
                    }
                ]
            },
            tenzing_kserve_client_metric_labels={"put_num": "1"},  # you can add extra labels to metrics like so
            tenzing_kserve_client_env_config=kserve_env,
            task_name=f"execute_tenzing_kserve_api_af_test_PUT_1_{env.execution_env}",
            task_id=f"execute_af_mock_job_PUT_1_{env.execution_env}",
        )
    )
    # another example op where user inputs resource as a list of JSON via the 'tenzing_resource_assets' parameter
    put2 = OpTask(
        op=TenzingBaseOperator(
            tenzing_unique_id="tf-mnist-af",
            tenzing_namespace="model-serving-aifun",
            tenzing_api_action=ApiActionEnum.PUT,
            tenzing_resource_assets=resource_assets,
            tenzing_kserve_client_metric_labels={"put_num": "2"},
            tenzing_kserve_client_env_config=kserve_env,
            task_name=f"execute_tenzing_kserve_api_af_test_PUT_2_{env.execution_env}",
            task_id=f"execute_af_mock_job_PUT_2_{env.execution_env}",
        )
    )
    # example delete op
    delete = OpTask(
        op=TenzingBaseOperator(
            tenzing_unique_id="tf-mnist-af",
            tenzing_namespace="model-serving-aifun",
            tenzing_api_action=ApiActionEnum.DELETE,
            tenzing_kserve_client_env_config=kserve_env,
            task_name=f"execute_tenzing_kserve_api_af_test_DELETE_{env}",
            task_id=f"execute_af_mock_job_DELETE_{env}",
        )
    )
    ops.append(put1)
    ops.append(put2)
    ops.append(delete)

ttd_dag >> ops[0]
for i, _ in enumerate(ops[:-1]):
    ops[i] >> ops[i + 1]

dag = ttd_dag.airflow_dag
