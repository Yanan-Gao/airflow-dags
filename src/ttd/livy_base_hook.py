from __future__ import annotations

import json
from abc import abstractmethod
from typing import Any, List, Optional, Tuple

from requests import Session
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class LivyBaseHook(HttpHook):

    def __init__(self, livy_conn_id: Optional[str] = None, **kwargs):
        self.livy_conn_id = livy_conn_id
        self.endpoint = self.get_endpoint()
        super().__init__(http_conn_id=livy_conn_id or "http_default")

    def get_conn(self, headers: dict[Any, Any] | None = None) -> Session:
        _headers = {"Content-Type": "application/json"}
        if headers is not None:
            _headers.update(headers)

        session = super().get_conn(headers=_headers)

        return session

    @abstractmethod
    def get_endpoint(self) -> str:
        pass

    def submit_spark_job(
        self,
        jar_loc: str,
        job_class: Optional[str],
        config_option: dict,
        driver_cores: int,
        driver_memory: str,
        executor_memory: str,
        executor_cores: int,
        command_line_arguments: List[str],
    ) -> int:

        livy_payload = \
            {
                "file": jar_loc,
                "conf": config_option,
                "executorMemory": executor_memory,
                "executorCores": executor_cores,
                "driverMemory": driver_memory,
                "driverCores": driver_cores,
                "args": command_line_arguments,
            }

        if job_class is not None:
            livy_payload["className"] = job_class

        data = json.dumps(livy_payload)

        self.log.info(f"Submitting Spark job using URL {self.endpoint}\n"
                      f"Request body: {data}")
        session = self.get_conn()
        submit_response = session.post(url=self.endpoint, data=data)
        response_content = submit_response.text
        self.log.info(f"Response: {response_content}")

        if submit_response.ok:
            return json.loads(response_content)["id"]

        self.log.error("Error while submitting the job.")
        raise AirflowException(response_content)

    def get_spark_job_status_and_app_id(self, batch_id: int) -> Tuple[str, str]:
        status_url = f"{self.endpoint}/{batch_id}"

        self.log.info(f"Getting job status using URL {status_url}")
        session = self.get_conn()
        status_response = session.get(url=status_url)
        response_content = status_response.text
        self.log.info(f"Response: {response_content}")

        if status_response.ok:
            state = json.loads(response_content)['state']
            app_id = json.loads(response_content)['appId']
            return state, app_id

        self.log.error("Error while getting job status.")
        raise AirflowException(response_content)

    def get_spark_job_logs(self, batch_id: int) -> List[str]:
        log_url = f"{self.endpoint}/{batch_id}/log"
        self.log.info(f"Getting logs using URL {log_url}")
        session = self.get_conn()
        logs_response = session.get(url=log_url)
        response_content = logs_response.text

        if logs_response.ok:
            self.log.info(f"Response: {response_content}")
            return json.loads(response_content)["log"]

        self.log.error("Error while getting job logs.")
        raise AirflowException(response_content)
