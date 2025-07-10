from dags.forecast.utils.model_serving.constants import \
    MERGE_REQUEST_IID_KEY
from dags.forecast.utils.model_serving.mr.creator import \
    ModelServingMergeRequestCreator
from dags.forecast.utils.model_serving.mr.types import \
    ModelServingEndpointDetails
from dags.forecast.utils.model_serving.sensors import ModelServingEndpointSensor, ModelServingMergeRequestSensor
from ttd.eldorado.xcom.helpers import get_xcom_pull_jinja_string
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask


class ModelServingTask(ChainOfTasks):

    def __init__(
        self,
        task_id: str,
        branch_name: str,
        merge_request_token: str,
        approval_token: str,
        reviewer_ids: list[int],
        model_serving_endpoint_details: ModelServingEndpointDetails,
    ):
        create_model_serving_merge_request = OpTask(
            op=ModelServingMergeRequestCreator(
                task_id=f"{task_id}_create_merge_request",
                branch_name=branch_name,
                merge_request_token=merge_request_token,
                reviewer_ids=reviewer_ids,
                model_serving_endpoint_details=model_serving_endpoint_details
            )
        )
        merge_request_iid = get_xcom_pull_jinja_string(
            task_ids=create_model_serving_merge_request.task_id,
            key=MERGE_REQUEST_IID_KEY,
        )
        merge_request_sensor = OpTask(
            op=ModelServingMergeRequestSensor(
                task_id=f"{task_id}_check_merge_request_is_merged",
                merge_request_iid=merge_request_iid,
                merge_token=merge_request_token,
                approval_token=approval_token
            )
        )
        endpoint_sensor = OpTask(
            op=ModelServingEndpointSensor(
                task_id=f"{task_id}_check_endpoint_is_active",
                team_name=model_serving_endpoint_details.team_name,
                endpoint_group=model_serving_endpoint_details.endpoint_group,
                endpoint=model_serving_endpoint_details.endpoint,
                branch_name=branch_name,
                private_token=merge_request_token,
            )
        )

        super().__init__(task_id=task_id, tasks=[create_model_serving_merge_request, merge_request_sensor, endpoint_sensor])

        self.as_taskgroup(group_id=task_id)
