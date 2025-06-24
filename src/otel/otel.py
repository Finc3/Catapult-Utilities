import atexit
import threading
from multiprocessing import Queue
from queue import Empty
from typing import Any, Dict, Optional

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from .otel_types import Metric


# Prevent PeriodicExportingMetricReader from reinit _ticker thread and duplicate metrics exporting
def _noop_at_fork_reinit(self):
    pass


if hasattr(PeriodicExportingMetricReader, "_at_fork_reinit"):
    PeriodicExportingMetricReader._at_fork_reinit = _noop_at_fork_reinit


class OTELMetricsExporter:
    def __init__(
        self,
        endpoint: str = "http://0.0.0.0:4318/v1/metrics",
        service_name: str = "multiprocess_app",
        credentials: Optional[Dict[str, Any]] = None,
        export_interval_ms: int = 10000,
        compression: str = "none",
    ):
        self._metric_queue = Queue()
        self._instruments = {}
        self._setup_meter_provider(endpoint, service_name, credentials, export_interval_ms, compression)
        self._start_consumer_thread()
        atexit.register(self.shutdown)

    def _setup_meter_provider(
        self,
        endpoint: str,
        service_name: str,
        credentials: Optional[Dict[str, Any]],
        export_interval_ms: int,
        compression: str,
    ):
        """Configure the OpenTelemetry meter provider"""
        exporter_args = {"endpoint": endpoint}
        if credentials:
            if "headers" in credentials:
                exporter_args["headers"] = credentials["headers"]
            if "insecure" in credentials:
                exporter_args["insecure"] = credentials["insecure"]
        if compression:
            exporter_args["compression"] = Compression(compression)

        exporter = OTLPMetricExporter(**exporter_args)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=export_interval_ms)
        resource = Resource.create({"service.name": service_name})
        self._meter_provider = MeterProvider(metric_readers=[reader], resource=resource)
        metrics.set_meter_provider(self._meter_provider)
        self._meter = metrics.get_meter("metrics_exporter")

    def _start_consumer_thread(self):
        """Start thread to process metrics from the queue"""
        self._consumer_thread = threading.Thread(target=self._consume_metrics, daemon=True)
        self._consumer_thread.start()

    def _consume_metrics(self):
        """Process metrics from the queue"""
        while True:
            try:
                metric = self._metric_queue.get(timeout=1)
                if metric is None:  # Shutdown signal
                    break
                self._process_metric(metric)
            except Empty:
                continue

    def _process_metric(self, metric: Metric):
        """Record the metric based on its type"""
        instr = self._get_or_create_instrument(metric)
        if metric.type == "counter":
            instr.add(metric.value, metric.attributes)
        elif metric.type == "histogram":
            instr.record(metric.value, metric.attributes)

    def _get_or_create_instrument(self, metric: Metric):
        """Get or create the appropriate instrument for the metric"""
        key = (metric.type, metric.name)
        if key in self._instruments:
            return self._instruments[key]

        if metric.type == "counter":
            self._instruments[key] = self._meter.create_counter(metric.name, description=metric.description, unit=metric.unit)
        elif metric.type == "gauge":
            self._instruments[key] = self._meter.create_observable_gauge(
                metric.name,
                callbacks=[lambda _: [Observation(metric.value, metric.attributes)]],
                description=metric.description,
                unit=metric.unit,
            )
        elif metric.type == "histogram":
            self._instruments[key] = self._meter.create_histogram(metric.name, description=metric.description, unit=metric.unit)
        else:
            raise ValueError(f"Unsupported metric type: {metric.type}")

        return self._instruments[key]

    def shutdown(self):
        """Clean shutdown of metrics collection"""
        self._metric_queue.put(None)  # Signal consumer to stop
        self._consumer_thread.join()
        self._meter_provider.shutdown()

    def record_counter(
        self,
        name: str,
        value: int = 1,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        """Record a counter metric from any process"""
        self._metric_queue.put(Metric.counter(name, value, attributes, description, unit))

    def record_gauge(
        self,
        name: str,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        """Record a gauge metric from any process"""
        self._metric_queue.put(Metric.gauge(name, value, attributes, description, unit))

    def record_histogram(
        self,
        name: str,
        value: float,
        attributes: Optional[Dict[str, str]] = None,
        description: str = "",
        unit: str = "1",
    ):
        """Record a histogram metric from any process"""
        self._metric_queue.put(Metric.histogram(name, value, attributes, description, unit))
