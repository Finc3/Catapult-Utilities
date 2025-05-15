import asyncio
import threading
import time
import atexit
import logging

from multiprocessing import Queue
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


def _make_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    return logger


class EventHubSender:
    """EventHubSender: A class for sending events to Azure Event Hub with batching support."""

    def __init__(self, connection_string: str, eventhub_name: str, batch_size: int = 100, batch_timeout: float = 2.0):
        """
        This constructor sets up the EventHubSender with the necessary configuration for sending events to Azure Event Hub.
        It initializes a thread-safe queue for event batching, sets up logging, and registers a shutdown handler to ensure
        graceful cleanup of resources.

        Parameters:
        - connection_string (str): The connection string for the Azure Event Hub namespace.
        - eventhub_name (str): The name of the specific Event Hub to send events to.
        - batch_size (int, optional): The maximum number of events to include in a single batch. Default is 100.
        - batch_timeout (float, optional): The maximum time (in seconds) to wait before sending a batch, even if it's not full. Default is 2.0 seconds.
        """
        self.queue = Queue()
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.logger = _make_logger()
        self._running = False
        self._thread = None
        atexit.register(self.shutdown)

    def run(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()

    def add_event(self, payload):
        """
        Add a pre-serialized event payload (e.g., JSON string) to the internal queue for batching
        and sending to Azure Event Hub. It ensures that the EventHubSender is running before accepting events.
        Ensure the payload is serialized (e.g., using `json.dumps(event)`) before calling this method.

        Parameters:
        - payload (str): The serialized event data (e.g., JSON string) to be added to the queue.
        """
        if not self._running:
            raise RuntimeError("EventHubSender is not running")
        self.queue.put(payload)

    def shutdown(self):
        if self._running:
            self._running = False
            if self._thread:
                self._thread.join(timeout=15)

    def _start_loop(self):
        asyncio.run(self._run_loop())

    async def _run_loop(self):
        producer = self._create_producer()
        if not producer:
            self.logger.error("[EventHubSender] No producer created, exiting")
            self._running = False
            return
        batch = []
        last_flush = time.time()
        try:
            while self._running:
                now = time.time()
                # Drain as many items as possible without blocking
                while not self.queue.empty() and len(batch) < self.batch_size:
                    payload = self.queue.get()
                    batch.append(payload)
                # Flush if batch is full or timeout reached
                if batch and (len(batch) >= self.batch_size or (now - last_flush) >= self.batch_timeout):
                    await self._send_batch(producer, batch)
                    batch.clear()
                    last_flush = now

                await asyncio.sleep(0.01)
        finally:
            if batch:
                await self._send_batch(producer, batch)
            await producer.close()
            self.logger.debug("[EventHubSender] Producer closed")

    async def _send_batch(self, producer, payloads):
        batch = await producer.create_batch()
        for payload in payloads:
            try:
                event = EventData(payload)
                batch.add(event)
            except ValueError:
                self.logger.debug("[EventHubSender] Skipped event too large for batch")
        await producer.send_batch(batch)
        self.logger.debug(f"[EventHubSender] Sent batch of {len(payloads)} event(s)")

    def _create_producer(self):
        try:
            producer = EventHubProducerClient.from_connection_string(self.connection_string, eventhub_name=self.eventhub_name)
            self.logger.debug("[EventHubSender] Producer created")
            return producer
        except Exception as e:
            self.logger.error(f"[EventHubSender] Failed to create producer: {e}")
            return None
