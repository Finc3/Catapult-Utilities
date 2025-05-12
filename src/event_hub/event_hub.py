import asyncio
import threading
import time
import atexit

from multiprocessing import Queue
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


class EventHubSender:
    def __init__(self, connection_string: str, eventhub_name: str, batch_size: int = 100, batch_timeout: float = 2.0):
        self.queue = Queue()
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._running = False
        self._thread = None

    def run(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._start_loop)
        self._thread.start()

    def add_event(self, payload):
        if not self._running:
            raise RuntimeError("EventHubSender is not running")
        self.queue.put(payload)

    @atexit.register
    def shutdown(self):
        if self._running:
            self._running = False
            self._thread.join()

    def _start_loop(self):
        asyncio.run(self._run_loop())

    async def _run_loop(self):
        producer = EventHubProducerClient.from_connection_string(conn_str=self.connection_string, eventhub_name=self.eventhub_name)
        print("[Sender] Started event loop with batching")

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
            print("[Sender] Producer closed")

    async def _send_batch(self, producer, payloads):
        batch = await producer.create_batch()
        for payload in payloads:
            try:
                event = EventData(payload)
                batch.add(event)
            except ValueError:
                print("[Sender] Skipped event too large for batch")
        await producer.send_batch(batch)
        print(f"[Sender] Sent batch of {len(payloads)} event(s)")
