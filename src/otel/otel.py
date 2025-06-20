import atexit
import threading
from multiprocessing import SimpleQueue
from typing import Any, Dict, Optional

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from .otel_types import Metric


class OTELMetricsExporter:
    def __init__(
        self,
        endpoint: str = "localhost:4317",
        service_name: str = "multiprocess_app",
        credentials: Optional[Dict[str, Any]] = None,
        export_interval_ms: int = 5000,
    ):
        self._metric_queue = SimpleQueue()
        self._instruments = {}
        self._setup_meter_provider(endpoint, service_name, credentials, export_interval_ms)
        self._start_consumer_thread()
        atexit.register(self.shutdown)

    def _setup_meter_provider(self, endpoint: str, service_name: str, credentials: Optional[Dict[str, Any]], export_interval_ms: int):
        """Configure the OpenTelemetry meter provider"""
        resource = Resource.create({"service.name": service_name})

        exporter_args = {"endpoint": endpoint}
        if credentials:
            if "headers" in credentials:
                exporter_args["headers"] = credentials["headers"]
            if "insecure" in credentials:
                exporter_args["insecure"] = credentials["insecure"]

        exporter = OTLPMetricExporter(**exporter_args)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=export_interval_ms)

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
            metric = self._metric_queue.get()
            if metric is None:  # Shutdown signal
                break
            self._process_metric(metric)

    def _process_metric(self, metric: Metric):
        """Record the metric based on its type"""
        metric_type = metric.type
        name = metric.name
        value = metric.value
        attributes = metric.attributes
        description = metric.description
        unit = metric.unit

        # Get or create instrument
        instrument_key = f"{metric_type}:{name}"
        if instrument_key not in self._instruments:
            if metric_type == "counter":
                self._instruments[instrument_key] = self._meter.create_counter(name, description=description, unit=unit)
            elif metric_type == "gauge":
                self._instruments[instrument_key] = self._meter.create_observable_gauge(
                    name, callbacks=[lambda _: [Observation(value, attributes)]], description=description, unit=unit
                )
            elif metric_type == "histogram":
                self._instruments[instrument_key] = self._meter.create_histogram(name, description=description, unit=unit)

        if metric_type == "counter":
            self._instruments[instrument_key].add(value, attributes)
        elif metric_type == "histogram":
            self._instruments[instrument_key].record(value, attributes)

    def shutdown(self):
        """Clean shutdown of metrics collection"""
        self._metric_queue.put(None)  # Signal consumer to stop
        self._consumer_thread.join()
        self._meter_provider.shutdown()

    def record_counter(self, name: str, value: int = 1, attributes: Optional[Dict[str, str]] = None, description: str = "", unit: str = "1"):
        """Record a counter metric from any process"""
        self._metric_queue.put(Metric.counter(name, value, attributes, description, unit))

    def record_gauge(self, name: str, value: float, attributes: Optional[Dict[str, str]] = None, description: str = "", unit: str = "1"):
        """Record a gauge metric from any process"""
        self._metric_queue.put(Metric.gauge(name, value, attributes, description, unit))

    def record_histogram(self, name: str, value: float, attributes: Optional[Dict[str, str]] = None, description: str = "", unit: str = "1"):
        """Record a histogram metric from any process"""
        self._metric_queue.put(Metric.histogram(name, value, attributes, description, unit))
