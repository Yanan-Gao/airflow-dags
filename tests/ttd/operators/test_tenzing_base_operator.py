import unittest

from src.ttd.operators.tenzing_base_operator import TenzingBaseOperator, ApiActionEnum

endpoint_spec = {
    "endpoints": [
        {
            "name": "base-af",
            "modelFormat": "tensorflow",
            "sourceUri": "uri1",
        },
        {
            "name": "vpeg-af",
            "modelFormat": "tensorflow",
            "sourceUri": "uri2",
        },
        {
            "name": "vpeg-batch-af",
            "modelFormat": "tensorflow-batch",
            "sourceUri": "uri3",
            "tensorflowBatchingConfig": {
                "maxBatchSize": 32,
                "batchTimeoutMicros": 1000,
            },
        },
    ]
}

valid_put_op_ep_json_spec = TenzingBaseOperator(
    tenzing_unique_id="unique-id",
    tenzing_namespace="namespace",
    tenzing_api_action=ApiActionEnum.PUT,
    tenzing_endpoint_configuration_spec=endpoint_spec,
    task_name="task",
    task_id="task_id",
    operator_namespace="op_namespace",
    random_name_suffix=True,
    get_logs=True,
    image_pull_policy="Always",
    startup_timeout_seconds=500,
    log_events_on_failure=True,
    tenzing_kserve_client_metric_labels={"my_label": "my_value"},
)

valid_put_op_resource_json = TenzingBaseOperator(
    tenzing_unique_id="unique-id",
    tenzing_namespace="namespace",
    tenzing_api_action=ApiActionEnum.PUT,
    tenzing_resource_assets=[{}],
    task_name="task",
    task_id="task_id",
    operator_namespace="op_namespace",
    random_name_suffix=True,
    get_logs=True,
    image_pull_policy="Always",
    startup_timeout_seconds=500,
    log_events_on_failure=True,
)


class TestTenzingBaseOperator(unittest.TestCase):

    def test_assert_multiple_sources_not_allowed(self):
        with self.assertRaises(Exception):
            TenzingBaseOperator(
                tenzing_unique_id="unique-id",
                tenzing_namespace="namespace",
                tenzing_api_action=ApiActionEnum.PUT,
                tenzing_endpoint_configuration_spec=endpoint_spec,
                tenzing_team_assets_configuration_spec={},
                task_name="task",
                task_id="task_id",
                operator_namespace="op_namespace",
                random_name_suffix=True,
                get_logs=True,
                image_pull_policy="Always",
                startup_timeout_seconds=500,
                log_events_on_failure=True,
            )

        with self.assertRaises(Exception):
            TenzingBaseOperator(
                tenzing_unique_id="unique-id",
                tenzing_namespace="namespace",
                tenzing_api_action=ApiActionEnum.PUT,
                tenzing_endpoint_configuration_spec=endpoint_spec,
                tenzing_resource_assets=[{}],
                task_name="task",
                task_id="task_id",
                operator_namespace="op_namespace",
                random_name_suffix=True,
                get_logs=True,
                image_pull_policy="Always",
                startup_timeout_seconds=500,
                log_events_on_failure=True,
            )

    def test_correct_arguments(self):
        assert valid_put_op_ep_json_spec.arguments == [
            "tenzing_kserve_client/airflow/run_kserve_client/script.py",
            "--namespace",
            "namespace",
            "--unique_id",
            "unique-id",
            "--api_action",
            "put",
            "--ephemeral",
            "False",
            "--endpoint_spec_definition",
            "{'endpoints': [{'name': 'base-af', 'modelFormat': 'tensorflow', 'sourceUri': 'uri1'}, {'name': 'vpeg-af', 'modelFormat': 'tensorflow', 'sourceUri': 'uri2'}, {'name': 'vpeg-batch-af', 'modelFormat': 'tensorflow-batch', 'sourceUri': 'uri3', 'tensorflowBatchingConfig': {'maxBatchSize': 32, 'batchTimeoutMicros': 1000}}]}",
            "--metric_labels",
            "{'my_label': 'my_value'}",
        ]

        assert valid_put_op_resource_json.arguments == [
            "tenzing_kserve_client/airflow/run_kserve_client/script.py",
            "--namespace",
            "namespace",
            "--unique_id",
            "unique-id",
            "--api_action",
            "put",
            "--ephemeral",
            "False",
            "--resource_assets_json",
            "[{}]",
        ]
