import asyncio
import time

import ray


@ray.remote
class MicroBatchCoordinator:
    """Async Ray actor that buffers Arrow tables between Spark and Ray Data.

    Spark's foreachBatch callback pushes micro-batches via put_batch().
    A Ray Data ReadTask generator pulls them via get_batch().

    Must be async: a sync actor with threading.Condition would deadlock
    because get_batch blocking the event loop prevents put_batch from
    being processed.
    """

    def __init__(self, max_buffered_batches: int = 4):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_buffered_batches)
        self._complete = False
        self._max_buffered = max_buffered_batches
        self._batches_put = 0
        self._batches_got = 0
        self._peak_buffer = 0

    async def put_batch(self, table):
        """Enqueue an Arrow table. Blocks (async) when buffer is full."""
        await self._queue.put(table)
        self._batches_put += 1
        current_size = self._queue.qsize()
        if current_size > self._peak_buffer:
            self._peak_buffer = current_size

    async def get_batch(self):
        """Dequeue the next Arrow table, or None if the stream is done.

        Polls with short sleeps so the actor remains responsive to
        mark_complete() between polls.
        """
        while True:
            try:
                table = self._queue.get_nowait()
                self._batches_got += 1
                return table
            except asyncio.QueueEmpty:
                if self._complete:
                    return None
                await asyncio.sleep(0.1)

    def mark_complete(self):
        """Signal that no more batches will be produced."""
        self._complete = True

    def get_stats(self) -> dict:
        return {
            "batches_put": self._batches_put,
            "batches_got": self._batches_got,
            "peak_buffer": self._peak_buffer,
            "max_buffered": self._max_buffered,
            "complete": self._complete,
            "current_queue_size": self._queue.qsize(),
        }
