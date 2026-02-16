import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pyarrow as pa
import ray


@dataclass
class MicroBatch:
    batch_id: int
    partition_refs: List[ray.ObjectRef]
    schema: pa.Schema
    num_rows: int
    timestamp: float


@ray.remote
class StreamCoordinator:
    """Named Ray actor that buffers Arrow table refs between Spark and consumers.

    Supports multiple independent consumers, each with their own cursor.
    Batches are garbage-collected once all consumers have advanced past them.

    Must be async: consumers block on pull_batch while producers call
    publish_batch concurrently — a sync actor would deadlock.
    """

    def __init__(self, stream_id: str, max_buffered_batches: int = 64):
        self._stream_id = stream_id
        self._max_buffered = max_buffered_batches

        # Ordered buffer: batch_id -> MicroBatch
        self._buffer: OrderedDict[int, MicroBatch] = OrderedDict()
        self._next_batch_id = 0

        # Consumer cursors: consumer_id -> next batch_id to read
        self._consumers: Dict[str, int] = {}

        # Signals
        self._complete = False
        self._error: Optional[str] = None
        self._space_available = asyncio.Event()
        self._space_available.set()  # initially there's space
        self._batch_available = asyncio.Event()

        # Schema from first batch
        self._schema: Optional[pa.Schema] = None

        # Stats
        self._batches_published = 0
        self._batches_gc = 0

    # -- Producer API --

    async def publish_batch(
        self, partition_refs: List[ray.ObjectRef], schema_bytes: bytes, num_rows: int
    ) -> int:
        """Publish a batch of Arrow table refs. Blocks when buffer is full."""
        # Wait for space
        while len(self._buffer) >= self._max_buffered:
            self._space_available.clear()
            await self._space_available.wait()

        schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
        if self._schema is None:
            self._schema = schema

        batch_id = self._next_batch_id
        self._next_batch_id += 1

        self._buffer[batch_id] = MicroBatch(
            batch_id=batch_id,
            partition_refs=partition_refs,
            schema=schema,
            num_rows=num_rows,
            timestamp=time.time(),
        )
        self._batches_published += 1

        # Signal waiting consumers
        self._batch_available.set()
        return batch_id

    async def signal_complete(self):
        """Signal that no more batches will be produced."""
        self._complete = True
        self._batch_available.set()  # wake any waiting consumers

    async def signal_error(self, error_msg: str):
        """Signal an error to all consumers."""
        self._error = error_msg
        self._batch_available.set()  # wake any waiting consumers

    # -- Consumer API --

    async def register_consumer(self, consumer_id: str) -> Optional[bytes]:
        """Register a consumer. Returns serialized schema bytes (or None if no batches yet)."""
        # Cursor starts at the earliest available batch
        if self._buffer:
            first_key = next(iter(self._buffer))
            self._consumers[consumer_id] = first_key
        else:
            self._consumers[consumer_id] = self._next_batch_id
        if self._schema is not None:
            return self._schema.serialize().to_pybytes()
        return None

    async def pull_batch(self, consumer_id: str) -> Optional[dict]:
        """Pull the next batch for this consumer. Returns None when stream is done."""
        if consumer_id not in self._consumers:
            raise ValueError(f"Unknown consumer: {consumer_id}")

        while True:
            # Check for error
            if self._error is not None:
                raise RuntimeError(f"Stream error: {self._error}")

            cursor = self._consumers[consumer_id]

            if cursor in self._buffer:
                mb = self._buffer[cursor]
                self._consumers[consumer_id] = cursor + 1
                self._gc_batches()
                return {
                    "batch_id": mb.batch_id,
                    "partition_refs": mb.partition_refs,
                    "num_rows": mb.num_rows,
                    "timestamp": mb.timestamp,
                }

            # No batch at cursor — either stream is done or we need to wait
            if self._complete and cursor >= self._next_batch_id:
                return None

            # Wait for new batch
            self._batch_available.clear()
            # Re-check after clearing to avoid race
            if (
                self._consumers[consumer_id] in self._buffer
                or self._complete
                or self._error is not None
            ):
                continue
            await self._batch_available.wait()

    async def deregister_consumer(self, consumer_id: str):
        """Remove a consumer and GC any batches it was holding back."""
        self._consumers.pop(consumer_id, None)
        self._gc_batches()

    # -- Internal --

    def _gc_batches(self):
        """Remove batches that all consumers have already read."""
        if not self._consumers:
            return
        min_cursor = min(self._consumers.values())
        to_remove = [bid for bid in self._buffer if bid < min_cursor]
        for bid in to_remove:
            del self._buffer[bid]
            self._batches_gc += 1
        if len(self._buffer) < self._max_buffered:
            self._space_available.set()

    # -- Observability --

    async def get_stats(self) -> dict:
        return {
            "stream_id": self._stream_id,
            "batches_published": self._batches_published,
            "batches_gc": self._batches_gc,
            "buffer_size": len(self._buffer),
            "max_buffered": self._max_buffered,
            "num_consumers": len(self._consumers),
            "consumer_cursors": dict(self._consumers),
            "complete": self._complete,
            "error": self._error,
        }
