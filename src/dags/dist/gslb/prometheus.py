from abc import abstractmethod
import logging
import requests
from typing import NamedTuple, Protocol
from pydantic import BaseModel, Field, conlist

from dags.dist.gslb import ns1
from dags.dist.gslb.config import Config
from dags.dist.gslb.ema import ExponentialMovingAverage


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


class Weights(NamedTuple):
    raw: float
    smoothed: float


def weights_from_values(values: list[list[float]], minimum_lookback: int) -> Weights:
    if len(values) < minimum_lookback:
        raise Exception(f"Not enough samples: {len(values)}")

    values.sort()  # sort the values in order of time (the first value of each value list)

    ema = ExponentialMovingAverage(minimum_lookback)
    for value in values:
        ema.record(value[1])

    last_value = values[len(values) - 1][1]

    return Weights(last_value, ema.average)


class WeightsResponse(NamedTuple):
    raw: dict[str, float]
    smoothed: dict[str, float]


class QueryRepository(Protocol):

    @abstractmethod
    def get_all_ssps(self, unix_timestamp: float) -> set[str]:
        ...

    @abstractmethod
    def get_opted_in_ssps(self, domain_name: str, zone: ns1.Zone, all_ssps: set[str]) -> set[str]:
        ...

    @abstractmethod
    def get_raw_weights_for_region(
        self, unix_timestamp: float, dc_list: list[str], opted_in_ssps: list[str], opted_out_ssps: list[str], default_opted_in: bool
    ) -> WeightsResponse:
        ...


class PrometheusQueryRepository(QueryRepository):

    def __init__(self, prometheus_client: PrometheusClient, config: Config):
        self._prometheus_client = prometheus_client
        self._config = config

    def get_all_ssps(self, unix_timestamp: float) -> set[str]:
        ssps = set()
        query_result = self._prometheus_client.query_instant('sum by (ssv) (datacenter_ssv:bid_request_count:rate5m)', unix_timestamp)
        for result in query_result.data.result:
            if 'ssv' in result.metric:
                ssps.add(result.metric['ssv'])

        return ssps

    def get_opted_in_ssps(self, domain_name: str, zone: ns1.Zone, all_ssps: set[str]) -> set[str]:
        opted_in_ssps = set()
        for record in zone.records:
            if record.type.lower() != 'cname':
                continue
            for answer in record.answers:
                if answer.region is None or len(answer.answer) == 0 or answer.answer[0] != domain_name:
                    continue
                ssp = self._config.get_ssp_from_domain(answer.region, record.domain)
                if ssp is not None and ssp in all_ssps:
                    opted_in_ssps.add(ssp)

        return opted_in_ssps

    def get_raw_weights_for_region(
        self, unix_timestamp: float, dc_list: list[str], opted_in_ssps: list[str], opted_out_ssps: list[str], default_opted_in: bool
    ) -> WeightsResponse:
        dc_regex = "|".join(dc_list)

        # (QPS_Region / Cores_Region) * Cores_DC - ExemptQPS_DC
        #
        # In English: Find the total QPS/CPU core in the region, then multiply by the number of cores in the DC and
        # subtract the amount of QPS in the DC that is not being handled by the GSLB.
        #
        # Even when all SSPs are exempt, we may not have zero weights which can be thought of as "the QPS needs to
        # change by this much to make all of the DCs have the same QPS/core." Note that it can be below zero also,
        # which we cap at config.minimum_weight since that's probably good enough.
        query = f"""
        (
            sum (datacenter_ssv:bid_request_count:rate5m{{datacenter=~"{dc_regex}"}}) /
            (sum (kube_pod_container_resource_requests{{namespace=~"adplat-bidder.*", resource="cpu", datacenter=~"{dc_regex}"}}))
        ) * ignoring (datacenter) group_right() (
            sum by (datacenter) (kube_pod_container_resource_requests{{namespace=~"adplat-bidder.*", resource="cpu", datacenter=~"{dc_regex}"}})
        )
        """

        bids_by_datacenter = "sum by (datacenter) (datacenter_ssv:bid_request_count:rate5m{{datacenter=~\"{dc_regex}\", ssv=~\"{ssv_regex}\"}})"
        up_by_datacenter = f"max by (datacenter) (up{{datacenter=~\"{dc_regex}\"}})"
        if default_opted_in:
            if len(opted_out_ssps) > 0:
                ssv_regex = "|".join(opted_out_ssps)
                query += " - (" + bids_by_datacenter.format(dc_regex=dc_regex, ssv_regex=ssv_regex) + " or " + up_by_datacenter + ")"
        else:
            query += " - " + bids_by_datacenter.format(dc_regex=dc_regex, ssv_regex=".+")
            if len(opted_in_ssps) > 0:
                ssv_regex = "|".join(opted_in_ssps)
                query += " + (" + bids_by_datacenter.format(dc_regex=dc_regex, ssv_regex=ssv_regex) + " or " + up_by_datacenter + ")"

        query_response = self._prometheus_client.query_range(
            query, unix_timestamp - self._config.lookback_distance_in_seconds, unix_timestamp, self._config.lookback_interval_in_seconds
        )

        raw = {}
        smoothed = {}

        for result in query_response.data.result:
            weights = weights_from_values(result.values, self._config.minimum_lookback)
            datacenter = result.metric["datacenter"]
            raw[datacenter] = weights.raw
            smoothed[datacenter] = weights.smoothed

        return WeightsResponse(raw, smoothed)
