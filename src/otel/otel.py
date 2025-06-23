import atexit
import threading
import time
from multiprocessing import SimpleQueue
from typing import Any, Dict, Optional

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http import Compression
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from .otel_types import Metric, MetricType


class OTELMetricsExporter:
    def __init__(
        self,
        endpoint: str = "localhost:4317",
        service_name: str = "my_service",
        credentials: Optional[Dict[str, Any]] = None,
        export_interval_s: int = 5,
        compression: str = "none",
    ):
        self._metric_queue = SimpleQueue()
        self._instruments = {}
        self._batch = {}
        self._last_flush = 0
        self._export_interval_s = export_interval_s
        self._setup_meter_provider(endpoint, service_name, credentials, export_interval_s, compression)
        self._start_consumer_thread()
        atexit.register(self.shutdown)

    def _setup_meter_provider(
        self,
        endpoint: str,
        service_name: str,
        credentials: Optional[Dict[str, Any]],
        export_interval_s: int,
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
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=export_interval_s * 1000)
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
        self._last_flush = time.time()
        while True:
            metric = self._metric_queue.get()
            if metric is None:  # Shutdown signal
                self._flush()
                break
            self._add_to_batch(metric)
            if (time.time() - self._last_flush) >= self._export_interval_s:
                self._flush()

    def _add_to_batch(self, metric: Metric):
        """Add a metric to the batch for processing"""
        key = self._get_metric_key(metric)
        if key not in self._batch:
            self._batch[key] = []
        self._batch[key].append(metric)

    def _get_metric_key(self, metric: Metric) -> tuple[MetricType, str, frozenset]:
        """Create a unique key for the metric based on its type, name and attributes"""
        return (
            metric.type,
            metric.name,
            frozenset(metric.attributes.items()) if metric.attributes else frozenset(),
        )

    def _flush(self):
        """Flush the batch of metrics to the exporter"""
        for key, metrics_list in self._batch.items():
            metric_template = metrics_list[0]
            instr = self._get_or_create_instrument(key, metric_template)
            if metric_template.type == "counter":
                self._process_counters(instr, metrics_list)
            elif metric_template.type == "histogram":
                self._process_histograms(instr, metrics_list)
            # Gauges are handled differently as they're observable

        self._batch.clear()
        self._last_flush = time.time()

    def _get_or_create_instrument(self, key: tuple, metric: Metric):
        """Get or create the appropriate instrument for the metric"""
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

    def _process_counters(self, instrument, metrics):
        """Process counter metrics and record them."""
        total_value = sum(metric.value for metric in metrics)
        instrument.add(
            total_value,
            attributes=metrics[0].attributes if metrics[0].attributes else {},
        )

    def _process_histograms(self, instrument, metrics):
        """Process histogram metrics and record them"""
        for metric in metrics:
            instrument.record(
                metric.value,
                attributes=metric.attributes if metric.attributes else {},
            )

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
