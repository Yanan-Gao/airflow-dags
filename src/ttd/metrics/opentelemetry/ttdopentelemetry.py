from typing import Dict
from opentelemetry.metrics._internal.instrument import Gauge, Counter
from ttd.metrics.opentelemetry.ttd_otel_client import TtdOtelClient
from cachetools.func import ttl_cache

_clients: Dict[str, TtdOtelClient] = {}


@ttl_cache(maxsize=1, ttl=60)
def _global_labels() -> Dict[str, str]:
    from ttd.ttdenv import TtdEnvFactory
    from ttd.ttdplatform import TtdPlatform
    return {"platform": TtdPlatform.get_from_system(), "env": TtdEnvFactory.get_from_system().execution_env}


class TtdGauge:

    def __init__(self, actual_gauge: Gauge, default_labels: Dict[str, str]):
        self._default_labels = default_labels
        self._actual_gauge = actual_gauge

    def labels(self, labels: Dict[str, str]):
        return TtdGauge(actual_gauge=self._actual_gauge, default_labels={**self._default_labels, **labels})

    def set(self, value: float, labels: Dict[str, str] = None):
        global_labels = _global_labels()
        self._actual_gauge.set(amount=value, attributes={**self._default_labels, **(labels or {}), **global_labels})


class TtdCounter:

    def __init__(self, actual_counter: Counter, default_labels: Dict[str, str]):
        self._default_labels = default_labels
        self._actual_counter = actual_counter

    def labels(self, labels: Dict[str, str]):
        return TtdCounter(actual_counter=self._actual_counter, default_labels={**self._default_labels, **labels})

    def inc(self, value: int = 1, labels: Dict[str, str] = None):
        global_labels = _global_labels()
        self._actual_counter.add(amount=value, attributes={**self._default_labels, **(labels or {}), **global_labels})


def _get_or_create_client(job: str) -> TtdOtelClient:
    if job not in _clients:
        _clients[job] = TtdOtelClient(job=job)
    return _clients[job]


def get_or_register_gauge(job: str, name: str, description: str) -> TtdGauge:
    _otel_client = _get_or_create_client(job)
    _otel_gauge = _otel_client.create_gauge(name=name, description=description)
    return TtdGauge(actual_gauge=_otel_gauge, default_labels={"job": job})


def get_or_register_counter(job: str, name: str, description: str) -> TtdCounter:
    _otel_client = _get_or_create_client(job)
    _otel_counter = _otel_client.create_counter(name=name, description=description)
    return TtdCounter(actual_counter=_otel_counter, default_labels={"job": job})


@ttl_cache(maxsize=1, ttl=60)
def _get_collector_endpoint() -> str:
    from airflow.models import Variable
    return Variable.get("OTEL_COLLECTOR_ENDPOINT", default_var="http://opentelemetry-collector.monitoring.svc.cluster.local:4317")


def push_all(job: str):
    endpoint = _get_collector_endpoint()
    _get_or_create_client(job).flush(collector_endpoint=endpoint)
