from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics._internal.instrument import Gauge, Counter
from opentelemetry.sdk.resources import Resource
import logging


class TtdOtelClient:

    def __init__(self, job: str):
        self.job = job
        resource = Resource(attributes={"service.name": self.job})
        self.reader = InMemoryMetricReader()
        self.meter_provider = MeterProvider(metric_readers=[self.reader], resource=resource)
        self.meter = self.meter_provider.get_meter(self.job + "_meter")

    def create_gauge(self, name: str, description: str = "") -> Gauge:
        return self.meter.create_gauge(name=name, description=description)

    def create_counter(self, name: str, description: str = "") -> Counter:
        return self.meter.create_counter(name=name, description=description)

    def flush(self, collector_endpoint: str):
        exporter = OTLPMetricExporter(endpoint=collector_endpoint)
        metrics_data = self.reader.get_metrics_data()
        if metrics_data is not None:
            result = exporter.export(metrics_data)  # type: ignore
            if result.value > 0:
                logging.warning("Failed to export metrics to Otel Collector.")
        else:
            logging.warning("No metrics were logged to be exported.")
