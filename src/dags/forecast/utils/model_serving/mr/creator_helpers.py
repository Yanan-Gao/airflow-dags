"""Houses code to help create model serving MRs on KServe"""

from tempfile import TemporaryDirectory
from typing import Any, Final, Literal, cast

import yaml

from dags.forecast.utils.model_serving.constants import MERGE_REQUEST_IID_KEY, MODEL_SERVING_PROJECT_ID
from dags.forecast.utils.model_serving.mr.types import (MlFlowModelSource, ModelServingEndpointDetails, ModelSource, ScalingSettings)
from dags.forecast.utils.model_serving.constants import GITLAB_HOST

MODEL_SERVING_REPO_URL: Final = (
    f"https://username:{{gitlab_project_access_token}}@{GITLAB_HOST}/thetradedesk/teams/aifun/model-serving-endpoints.git"
)


def _load_yaml(filename: str):
    with open(filename, "r", encoding="utf8") as file:
        return yaml.safe_load(file)


def _dump_yaml(data, filename: str):
    with open(filename, "w", encoding="utf8") as file:
        yaml.safe_dump(data, file, sort_keys=False)


def _add_endpoint_to_yaml(
    allow_endpoint_overwrite: bool,
    endpoints_group_yaml_file: str,
    endpoint: str,
    model_format: str,
    model_source: ModelSource,
    scaling_settings: None | ScalingSettings,
) -> None:
    served_models: dict[Literal["endpoints"], list[dict[str, Any]]]
    try:
        served_models = _load_yaml(endpoints_group_yaml_file)
    except FileNotFoundError:
        served_models = {"endpoints": []}

    endpoints = served_models["endpoints"]

    endpoint_already_exists = False
    # Find the endpoint if it does already exist
    for i, endpoint_dict in enumerate(endpoints):
        endpoint_already_exists = (endpoint_dict["name"] == endpoint)
        if endpoint_already_exists:
            if not allow_endpoint_overwrite:
                raise AssertionError(f"The following endpoint already exists: {endpoint}")
            break

    if endpoint_already_exists:
        # Clear out the existing model source in case we are switching between source uri and source emlflow
        for k in ["sourceUri", "sourceMlflowName", "sourceMlflowVersion"]:
            endpoint_dict.pop(k, None)
    else:
        endpoint_dict = {"name": endpoint}
        endpoints.append(endpoint_dict)

    endpoint_dict["modelFormat"] = model_format

    if isinstance(model_source, str):
        endpoint_dict["sourceUri"] = model_source
    else:
        endpoint_dict |= {
            "sourceMlflowName": model_source.name,
            "sourceMlflowVersion": model_source.version,
        }

    # Is scaling setting is None, leave these unchanged
    # If there is no existing settings, then these will just be set to default values in KServe
    if scaling_settings is not None:
        endpoint_dict |= scaling_settings

    _dump_yaml(served_models, endpoints_group_yaml_file)


def _delete_endpoints(
    endpoints_group_yaml_file: str,
    mlflow_model_source: MlFlowModelSource,
    versions_to_keep: int,
) -> None:
    # Clear out old versions so we're not serving too many

    served_models = _load_yaml(endpoints_group_yaml_file)

    endpoints = served_models["endpoints"]

    # Find all the served versions (including our new one)
    sorted_mlflow_versions = sorted({
        int(ep["sourceMlflowVersion"])  # Needs to be ordered as an int
        for ep in endpoints.values() if ep.get("sourceMlflowName", None) == mlflow_model_source.name
    })
    if len(sorted_mlflow_versions) > versions_to_keep:
        # If we have more versions served than the max
        # Find the nth newest/largest version
        min_mlflow_version = sorted_mlflow_versions[-versions_to_keep]
        if mlflow_model_source.version < min_mlflow_version:
            raise AssertionError("We are trying to remove our new endpoint!")
        # Filter endpoints to remove versions lower than this
        endpoints_to_delete = [
            endpoint_name for endpoint_name, endpoint_details in endpoints.items() if (
                endpoint_details.get("sourceMlflowName", None) == mlflow_model_source.name
                and int(endpoint_details["sourceMlflowVersion"]) < min_mlflow_version
            )
        ]
        for endpoint_name in endpoints_to_delete:
            del endpoints[endpoint_name]

        _dump_yaml(served_models, endpoints_group_yaml_file)


def create_branch_with_new_model(
    branch_name: str,
    gitlab_project_access_token: str,
    model_serving_endpoint_details: ModelServingEndpointDetails,
) -> None:
    """
    The create a branch in git with for the model specified in the args
    The args are designed for mlflow trained models
    """
    from git import Repo

    with TemporaryDirectory() as tmpdirname:
        # Let's checkout the model serving repo
        repo = Repo.clone_from(
            MODEL_SERVING_REPO_URL.format(gitlab_project_access_token=gitlab_project_access_token),
            tmpdirname,
        )
        repo.config_writer().set_value("user", "email", "airflow@thetradedesk.com").release()
        origin = repo.remote(name="origin")
        # Create a new branch and switch to it
        repo.head.reference = repo.create_head(branch_name)
        repo.head.reference.set_tracking_branch(origin.refs.master).checkout()

        repo.git.pull("origin", repo.heads.master)

        # All team names are lowercase in model serving repo
        endpoints_group_yaml_file = f"{tmpdirname}/teams/{model_serving_endpoint_details.team_name.lower()}/{model_serving_endpoint_details.endpoint_group}.yaml"
        _add_endpoint_to_yaml(
            allow_endpoint_overwrite=model_serving_endpoint_details.allow_endpoint_overwrite,
            endpoints_group_yaml_file=endpoints_group_yaml_file,
            endpoint=model_serving_endpoint_details.endpoint,
            model_format=model_serving_endpoint_details.model_format,
            model_source=model_serving_endpoint_details.model_source,
            scaling_settings=model_serving_endpoint_details.scaling_settings,
        )

        if model_serving_endpoint_details.versions_to_keep is not None:
            _delete_endpoints(
                endpoints_group_yaml_file=endpoints_group_yaml_file,
                mlflow_model_source=model_serving_endpoint_details.model_source,
                versions_to_keep=model_serving_endpoint_details.versions_to_keep,
            )

        repo.index.add([endpoints_group_yaml_file])
        repo.index.commit(f"{branch_name} Adding the following endpoint: {model_serving_endpoint_details.endpoint}")
        push_info_list = origin.push(branch_name)
        push_info_list.raise_if_error()


def _get_gitlab_model_serving_project(private_token: str):
    """
    This fetches the gitlab project for the model serving repo
    """
    from gitlab import Gitlab

    gitlab = Gitlab(f"https://{GITLAB_HOST}/", private_token=private_token)
    return gitlab.projects.get(MODEL_SERVING_PROJECT_ID)


def create_merge_request(branch_name: str, gitlab_project_access_token: str, reviewer_ids: list[int]) -> dict[str, str]:
    """Creates a gitlab model serving MR"""
    from gitlab.v4.objects.merge_requests import ProjectMergeRequest

    project = _get_gitlab_model_serving_project(gitlab_project_access_token)
    merge_request = cast(
        ProjectMergeRequest,
        project.mergerequests.create({
            "source_branch": branch_name,
            "target_branch": "master",
            "title": branch_name,
            "description": "TTD_BYPASS_CHECK(JiraTicketChecker)\nTTD_BYPASS_CHECK(EngOrgChecker)",
            "remove_source_branch": True,
            "squash": True,
            "reviewer_ids": reviewer_ids,
        }),
    )
    # Return the iid for the MR
    return {MERGE_REQUEST_IID_KEY: merge_request.iid}
