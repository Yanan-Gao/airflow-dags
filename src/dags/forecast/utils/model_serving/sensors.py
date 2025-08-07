"""
Houses sensors for model serving on KServe
"""

import logging
from datetime import timedelta
from typing import Any, Callable, Final
from urllib.parse import urlencode

from airflow.exceptions import AirflowFailException
from airflow.sensors.base import BaseSensorOperator
from requests import HTTPError, Session

from dags.forecast.utils.model_serving.constants import \
    GITLAB_HOST, MODEL_SERVING_PROJECT_ID
from dags.forecast.utils.requests import get_request_session, install_certificates

_GITLAB_API_BASE_URL_FOR_MODEL_SERVING: Final = f"https://{GITLAB_HOST}/api/v4/projects/{MODEL_SERVING_PROJECT_ID}"
_GITLAB_MERGE_REQUESTS_URL: Final = f"{_GITLAB_API_BASE_URL_FOR_MODEL_SERVING}/merge_requests"
_GITLAB_JOBS_URL: Final = f"{_GITLAB_API_BASE_URL_FOR_MODEL_SERVING}/jobs"
_USAGE_BOT_MERGE_REQUESTS_MANUAL_SCAN_URL: Final = f"https://usage-bot.gen.adsrvr.org/trigger/{MODEL_SERVING_PROJECT_ID}"
_NON_SUCCESS_JOB_SCOPES: Final = ["created", "pending", "running", "failed", "canceled", "waiting_for_resource", "manual"]


def _get_job_ids(request_session: Session, job_finder: Callable[[dict[str, Any]], bool], scope: None | str | list[str] = None) -> list[str]:
    url = _GITLAB_JOBS_URL
    # This will only return the most recent 100 jobs
    # If we need more, look at https://docs.gitlab.com/api/rest/#pagination
    params: dict[str, Any] = {"per_page": 100}
    doseq = not isinstance(scope, None | str)
    if scope is not None:
        params["scope[]"] = scope

    url += "?" + urlencode(params, doseq=doseq)
    res = request_session.get(url, timeout=5)
    res.raise_for_status()
    return [job["id"] for job in res.json() if job_finder(job)]


def _try_run_manual_jobs(
    job_finder: Callable[[dict[str, Any]], bool],
    private_token: str,
) -> int:
    with get_request_session(headers={"PRIVATE-TOKEN": private_token}) as session:
        all_job_ids = _get_job_ids(session, job_finder)
        if not all_job_ids:
            # If we can't find any job ids, we should fail. Perhaps the jobs haven't been created yet
            # If the task fails and never finds any jobs, then we probably have an issue
            raise RuntimeError("Could not find any gitlab jobs, could be due to a race condition")
        # Find and run the jobs in a manual state
        manual_job_ids = _get_job_ids(session, job_finder, scope="manual")
        if manual_job_ids:
            logging.info("Found %s manual pipeline steps that we will try and run", len(manual_job_ids))
        for job_id in manual_job_ids:
            session.post(f"{_GITLAB_JOBS_URL}/{job_id}/play")
        # Find the jobs not in a success state
        non_success_job_ids = _get_job_ids(session, job_finder, scope=_NON_SUCCESS_JOB_SCOPES)
    if non_success_job_ids:
        logging.info(
            "Found %s pipeline steps that have not yet succeeded. If this task fails, you might need to run any failed steps manually",
            len(non_success_job_ids)
        )
    return len(non_success_job_ids)


def _try_run_manual_jobs_for_merge_commit(branch_name: str, private_token: str) -> int:

    def _job_finder(job: dict[str, Any]) -> bool:
        return job["commit"]["title"] == f"Merge branch '{branch_name}' into 'master'"

    return _try_run_manual_jobs(_job_finder, private_token)


def _try_run_manual_jobs_for_merge_request(merge_request_iid: str, private_token: str) -> int:

    def _job_finder(job: dict[str, Any]) -> bool:
        return job["ref"] == f"refs/merge-requests/{merge_request_iid}/head"

    return _try_run_manual_jobs(_job_finder, private_token)


def _trigger_manual_scan_for_usage_bot(merge_request_iid: str) -> None:
    with get_request_session() as session:
        session.get(
            f"{_USAGE_BOT_MERGE_REQUESTS_MANUAL_SCAN_URL}/{merge_request_iid}",
            timeout=5,
            verify=install_certificates(),
        )


class ModelServingMergeRequestSensor(BaseSensorOperator):
    """
    Checks if an MR has been merged in the model serving codebase
    If it hasn't been merged,
    then we try to find manual jobs to run that might help get the MR merged
    """

    template_fields = tuple(set(BaseSensorOperator.template_fields) | {"merge_request_iid", "merge_token", "approval_token"})

    def __init__(self, merge_request_iid: str, merge_token: str, approval_token: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.merge_request_iid = merge_request_iid
        self.merge_token = merge_token
        self.approval_token = approval_token

    def poke(self, context) -> bool:
        url = f"{_GITLAB_MERGE_REQUESTS_URL}/{self.merge_request_iid}"
        with get_request_session(headers={"PRIVATE-TOKEN": self.merge_token}) as session:
            res = session.get(url, timeout=5)
            res.raise_for_status()
            res_json = res.json()
            state = res_json["state"]
            if state == "closed":
                raise AirflowFailException("Merge request was closed")
            if state == "merged":
                return True
            description = res_json["description"]
            # If the merge request isn't merged, try running any manual steps
            _try_run_manual_jobs_for_merge_request(self.merge_request_iid, self.merge_token)
            # Try approving merge request
            session.post(f"{url}/approve", headers={"PRIVATE-TOKEN": self.approval_token})
            # Force bots to refresh
            _trigger_manual_scan_for_usage_bot(self.merge_request_iid)
            # Little hack here, devbot checks for updates to the MR
            # So we can force devbot to rescan by "updating" the MR
            # We just set the description, to the same description as before
            session.put(f"{url}?description={description}", timeout=5)
            # Then try merging
            session.put(f"{url}/merge?merge_when_pipeline_succeeds=true")

        # Next time the sensor runs, if it has merged the sensor will pass
        # For now, assume the merge wasn't successful
        return False


class ModelServingEndpointSensor(BaseSensorOperator):
    """
    Checks if a model serving endpoint is available
    If it isn't available,
    then we try to find manual jobs to run that might help get the endpoint active
    """

    template_fields = tuple(set(BaseSensorOperator.template_fields) | {"endpoint_group", "endpoint", "branch_name", "private_token"})

    _KSERVE_URL: Final = "https://model-serving.gen.adsrvr.org/{team_name}/{endpoint_group}/{endpoint}/v1/models/{endpoint_group}-{endpoint}"

    def __init__(
        self,
        team_name: str,
        endpoint_group: str,
        endpoint: str,
        branch_name,
        private_token,
        timeout=timedelta(minutes=30).total_seconds(),
        **kwargs,
    ) -> None:
        super().__init__(timeout=timeout, **kwargs)
        self.team_name = team_name.lower()
        self.endpoint_group = endpoint_group
        self.endpoint = endpoint
        self.branch_name = branch_name
        self.private_token = private_token

    def poke(self, context) -> bool:
        url = self._KSERVE_URL.format(
            team_name=self.team_name,
            endpoint_group=self.endpoint_group,
            endpoint=self.endpoint,
        )
        n_manual_jobs_left = _try_run_manual_jobs_for_merge_commit(self.branch_name, self.private_token)
        if n_manual_jobs_left != 0:
            return False
        with get_request_session() as session:
            res = session.get(url, timeout=5, verify=install_certificates())
        try:
            res.raise_for_status()
        except HTTPError:
            logging.info("Received HTTPError from endpoint indicating that it is not functional yet")
            return False
        res_json = res.json()
        try:
            return res_json["model_version_status"][0]["state"] == "AVAILABLE"
        except KeyError:
            # This is the format for torch models
            logging.info("Could not find model_version_status key in response, model is likely pytorch model")
            return res_json["ready"] == "True"
