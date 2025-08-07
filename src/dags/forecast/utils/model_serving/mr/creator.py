"""Houses code to create model serving MRs on KServe"""
from airflow.operators.python import PythonVirtualenvOperator

from dags.forecast.utils.model_serving.mr.types import \
    ModelServingEndpointDetails


def _create_new_model_merge_request(
    branch_name: str,
    gitlab_project_access_token: str,
    reviewer_ids: list[int],
    model_serving_endpoint_details: ModelServingEndpointDetails,
) -> dict[str, str]:
    import subprocess

    from dags.forecast.utils.model_serving.mr.creator_helpers import create_branch_with_new_model, create_merge_request

    # Install git
    subprocess.check_output("sudo apt-get install -y git", shell=True)

    create_branch_with_new_model(
        branch_name=branch_name,
        gitlab_project_access_token=gitlab_project_access_token,
        model_serving_endpoint_details=model_serving_endpoint_details,
    )

    return create_merge_request(
        branch_name=branch_name,
        gitlab_project_access_token=gitlab_project_access_token,
        reviewer_ids=reviewer_ids,
    )


class ModelServingMergeRequestCreator(PythonVirtualenvOperator):

    def __init__(
        self, branch_name: str, merge_request_token: str, reviewer_ids: list[int],
        model_serving_endpoint_details: ModelServingEndpointDetails, **kwargs
    ):
        super().__init__(
            python_callable=_create_new_model_merge_request,
            # Not pinning this to allow for updates,
            # if we start failing we might want to consider pinning
            requirements=["python-gitlab", "GitPython"],
            op_kwargs=dict(
                branch_name=branch_name,
                gitlab_project_access_token=merge_request_token,
                reviewer_ids=reviewer_ids,
                model_serving_endpoint_details=model_serving_endpoint_details,
            ),
            multiple_outputs=True,
            **kwargs
        )
