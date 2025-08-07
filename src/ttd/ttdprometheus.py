import logging
import typing

import prometheus_client
from cachetools.func import ttl_cache
from prometheus_client import CollectorRegistry, Gauge, Counter
from typing import List, Dict, Optional

from prometheus_client.metrics import MetricWrapperBase

registries = {}
metrics: Dict[str, MetricWrapperBase] = {}


def get_or_create_registry(job: str):
    if job not in registries:
        registries[job] = CollectorRegistry()

    return registries[job]


def get_or_register_gauge(job: str, name: str, description: str, labels: List[str]) -> Gauge:
    if name in metrics:
        return typing.cast(Gauge, metrics[name])
    else:
        gauge = Gauge(name, description, labels)
        metrics[name] = gauge
        get_or_create_registry(job).register(gauge)
        return gauge


def get_or_register_counter(job: str, name: str, description: str, labels: List[str]) -> Counter:
    if name in metrics:
        return typing.cast(Counter, metrics[name])
    else:
        counter = Counter(name, description, labels)
        metrics[name] = counter
        get_or_create_registry(job).register(counter)
        return counter


def get_metric(name: str):
    if name in metrics:
        return metrics[name]
    else:
        return None


@ttl_cache(maxsize=1, ttl=60)
def get_prometheus_pushgateway_endpoint():
    from airflow.models import Variable
    return Variable.get("PROMETHEUS_PUSHGATEWAY_ENDPOINT", default_var="http://prometheus-pushgateway.monitoring.svc.cluster.local:9091")


# Attempt to push metrics to prometheus gateway 3 times
def push_all(job, grouping_key: Optional[Dict[str, str]] = None):
    pushgateway_endpoint = get_prometheus_pushgateway_endpoint()

    if grouping_key is None:
        grouping_key = {}

    from ttd.ttdenv import TtdEnvFactory
    from ttd.ttdplatform import TtdPlatform

    grouping_key["platform"] = TtdPlatform.get_from_system()
    grouping_key["env"] = TtdEnvFactory.get_from_system().execution_env

    left = 3
    success = False

    while not success and left > 0:
        try:
            left -= 1
            prometheus_client.push_to_gateway(
                pushgateway_endpoint,
                job=job,
                grouping_key=grouping_key,
                registry=get_or_create_registry(job),
            )
            success = True
        except Exception as e:
            logging.info("Push to Prometheus Failed")
            logging.info(e)

    if not success:
        return
        # slack_client.chat_postMessage(channel="#dev-airflow-alerts", text="Error pushing metrics to prometheus")
