from abc import abstractmethod
import logging
import requests
from typing import Protocol
from pydantic import BaseModel, Field, conlist


class VectorResult(BaseModel):
    metric: dict[str, str]
    # this will cause some loss in precision for each value, but that is fine for our use case
    value: conlist(float, min_length=2, max_length=2)  # type: ignore[valid-type]


class VectorData(BaseModel):
    resultType: str = Field("vector", pattern="^vector$")
    result: list[VectorResult] = []


class VectorQueryResponse(BaseModel):
    status: str = Field("success", pattern="^success$")
    data: VectorData = VectorData(resultType="vector")


class MatrixResult(BaseModel):
    metric: dict[str, str]
    values: list[conlist(float, min_length=2, max_length=2)]  # type: ignore[valid-type]


class MatrixData(BaseModel):
    resultType: str = Field("matrix", pattern="^matrix$")
    result: list[MatrixResult] = []


class MatrixQueryResponse(BaseModel):
    status: str = Field("success", pattern="^success$")
    data: MatrixData = MatrixData(resultType="matrix")


class PrometheusClient(Protocol):

    @abstractmethod
    def query_instant(self, query: str, time: float) -> VectorQueryResponse:
        ...

    @abstractmethod
    def query_range(self, query: str, start: float, end: float, step_in_seconds: float) -> MatrixQueryResponse:
        ...


class MqpPrometheusClient(PrometheusClient):

    def __init__(self, endpoint: str, user: str, password: str):
        super().__init__()
        self._endpoint = endpoint
        self._user = user
        self._password = password

    def _format_query(self, query: str) -> str:
        return " ".join(query.strip().split())  # remove all unnecessary whitespace

    def query_instant(self, query: str, time: float) -> VectorQueryResponse:
        query = self._format_query(query)
        logging.info(query)
        payload = {'query': query, 'time': time}
        r = requests.post(self._endpoint + "/api/v1/query", params=payload, auth=(self._user, self._password))
        logging.info("Prometheus response: %s", r.text)
        if r.status_code != 200:
            raise Exception(f"Non-200 status: {r.status_code}")
        return VectorQueryResponse.model_validate_json(r.text)

    def query_range(self, query: str, start: float, end: float, step_in_seconds: float) -> MatrixQueryResponse:
        query = self._format_query(query)
        logging.info(query)
        payload = {'query': query, 'start': start, 'end': end, 'step': step_in_seconds}
        r = requests.post(self._endpoint + "/api/v1/query_range", params=payload, auth=(self._user, self._password))
        logging.info("Prometheus response: %s", r.text)
        if r.status_code != 200:
            raise Exception(f"Non-200 status: {r.status_code}")
        return MatrixQueryResponse.model_validate_json(r.text)
