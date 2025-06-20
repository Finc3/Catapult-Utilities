from otel import OTELMetricsExporter

endpoint = "127.0.0.1:4317"
token = ""
credentials = {"headers": {"authorization": f"Basic {token}"}}

exporter = OTELMetricsExporter(endpoint=endpoint, service_name="test_util", credentials=credentials)

exporter.record_counter(
    name="test_counter",
    value=1,
    attributes={"key": "value"},
    description="Test counter metric",
    unit="1",
)
exporter.record_gauge(
    name="test_gauge",
    value=1,
    attributes={"key": "value"},
    description="Test gauge metric",
    unit="1",
)
exporter.record_histogram(
    name="test_histogram",
    value=1.0,
    attributes={"key": "value"},
    description="Test histogram metric",
    unit="1",
)
