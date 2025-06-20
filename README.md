# Enalytics Python Utilities  
A collection of utilities for us in different systems.

## MongoLocks
A simple Implementation that uses MongoDB as a backend for shared locks. The use of a heartbeat thread ensures long-running jobs maintain a lock until execution is completed.  
Intended for use in distributed systems.

Example usage:
```python
from time import sleep
from pymongo import MongoClient
from locking import MongoLocks

mc = MongoClient()
mongo_locks = MongoLocks(mc, "my_project")


# This operation is now protected by a lock for the duration of this method execution
@mongo_locks.lock("op1")
def op1():
    print("Working...")
    sleep(20)
    print("...Done")

op1()
```

## EventHubSender

A lightweight Python class for asynchronously sending events to Azure Event Hub using background batching.

Ideal for distributed systems requiring high-throughput logging or streaming.

Example usage:
```python
import json
from eventhub_sender import EventHubSender

sender = EventHubSender(
    connection_string="your_connection_string",
    eventhub_name="your_eventhub"
)

sender.run()

# Add events (must be serialized, e.g., JSON)
sender.add_event(json.dumps({"event": "start"}))
sender.add_event(json.dumps({"event": "end"}))

# Graceful shutdown handled automatically on exit
```

## OTELMetricsExporter

Metrics Exporter to a OTEL Collector using OTEL Protocol. It is Process-Safe due to its multiprocessing.Queue. 

Example usage:
```python
from otel import OTELMetricsExporter

endpoint = "127.0.0.1:4317"
credentials = {"headers": {"authorization": f"Basic <token>"}}

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

```