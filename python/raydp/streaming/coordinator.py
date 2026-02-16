import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pyarrow as pa
import ray
from ray.util.metrics import Counter, Gauge, Histogram


@dataclass
class MicroBatch:
    batch_id: int
    partition_refs: List[ray.ObjectRef]
    schema: pa.Schema
    num_rows: int
    num_bytes: int
    timestamp: float
    watermark: Optional[str] = None  # ISO8601 UTC string from Spark


@ray.remote
class StreamCoordinator:
    """Named Ray actor that buffers Arrow table refs between Spark and consumers.

    Supports multiple independent consumers, each with their own cursor.
    Batches are garbage-collected once all consumers have advanced past them.

    Must be async: consumers block on pull_batch while producers call
    publish_batch concurrently — a sync actor would deadlock.
    """

    def __init__(self, stream_id: str, max_buffered_batches: int = 64,
                 max_buffered_bytes: int = 2 * 1024**3):
        self._stream_id = stream_id
        self._max_buffered = max_buffered_batches
        self._max_bytes = max_buffered_bytes
        self._buffered_bytes = 0

        # Ordered buffer: batch_id -> MicroBatch
        self._buffer: OrderedDict[int, MicroBatch] = OrderedDict()
        self._next_batch_id = 0

        # Consumer cursors: consumer_id -> next batch_id to read
        self._consumers: Dict[str, int] = {}
        # Partition assignments: consumer_id -> list of partition indices (None = all)
        self._consumer_partitions: Dict[str, Optional[List[int]]] = {}

        # Signals
        self._complete = False
        self._error: Optional[str] = None
        self._space_available = asyncio.Event()
        self._space_available.set()  # initially there's space
        self._batch_available = asyncio.Event()

        # Schema from first batch
        self._schema: Optional[pa.Schema] = None
        self._schema_available = asyncio.Event()

        # Watermark
        self._latest_watermark: Optional[str] = None

        # Stats
        self._batches_published = 0
        self._batches_gc = 0

        # Ray metrics
        self._m_published = Counter(
            "raydp_stream_batches_published",
            description="Total batches published",
            tag_keys=("stream_id",),
        ).set_default_tags({"stream_id": stream_id})

        self._m_buffer_size = Gauge(
            "raydp_stream_buffer_size",
            description="Current buffered batches",
            tag_keys=("stream_id",),
        ).set_default_tags({"stream_id": stream_id})

        self._m_publish_latency = Histogram(
            "raydp_stream_publish_latency_ms",
            description="Publish latency (ms)",
            boundaries=[1, 5, 10, 50, 100, 500, 1000, 5000],
            tag_keys=("stream_id",),
        ).set_default_tags({"stream_id": stream_id})

        self._m_pull_latency = Histogram(
            "raydp_stream_pull_latency_ms",
            description="Pull wait time (ms)",
            boundaries=[1, 5, 10, 50, 100, 500, 1000, 5000],
            tag_keys=("stream_id",),
        ).set_default_tags({"stream_id": stream_id})

        self._m_gc = Counter(
            "raydp_stream_batches_gc",
            description="Total batches GC'd",
            tag_keys=("stream_id",),
        ).set_default_tags({"stream_id": stream_id})

        self._m_buffered_bytes = Gauge(
            "raydp_stream_buffered_bytes",
            description="Current buffered bytes in object store",
            tag_keys=("stream_id",),
        ).set_default_tags({"stream_id": stream_id})

        self._m_consumer_lag = Gauge(
            "raydp_stream_consumer_lag",
            description="Batches behind head per consumer",
            tag_keys=("stream_id", "consumer_id"),
        )

    # -- Producer API --

    async def publish_batch(
        self,
        partition_refs: List[ray.ObjectRef],
        schema_bytes: bytes,
        num_rows: int,
        watermark: Optional[str] = None,
        num_bytes: int = 0,
    ) -> int:
        """Publish a batch of Arrow table refs. Blocks when buffer is full."""
        publish_start = time.time()

        # Wait for space (batch count OR byte budget)
        while (len(self._buffer) >= self._max_buffered
               or (self._buffered_bytes >= self._max_bytes
                   and len(self._buffer) > 0)):
            self._space_available.clear()
            await self._space_available.wait()

        schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
        if self._schema is None:
            self._schema = schema
            self._schema_available.set()
        elif not self._schema.equals(schema):
            raise ValueError(
                f"Schema mismatch on batch {self._next_batch_id}: "
                f"stream schema has {self._schema}, "
                f"but batch has {schema}. "
                f"Spark Structured Streaming does not support schema "
                f"evolution within a query — restart the stream if the "
                f"schema has changed."
            )

        batch_id = self._next_batch_id
        self._next_batch_id += 1

        if watermark is not None:
            self._latest_watermark = watermark

        self._buffer[batch_id] = MicroBatch(
            batch_id=batch_id,
            partition_refs=partition_refs,
            schema=schema,
            num_rows=num_rows,
            num_bytes=num_bytes,
            timestamp=time.time(),
            watermark=watermark,
        )
        self._buffered_bytes += num_bytes
        self._batches_published += 1

        # Metrics
        self._m_published.inc()
        self._m_buffer_size.set(len(self._buffer))
        self._m_buffered_bytes.set(self._buffered_bytes)
        self._m_publish_latency.observe((time.time() - publish_start) * 1000)

        # Signal waiting consumers
        self._batch_available.set()
        return batch_id

    async def signal_complete(self):
        """Signal that no more batches will be produced."""
        self._complete = True
        self._schema_available.set()  # unblock register_consumer if waiting
        self._batch_available.set()  # wake any waiting consumers

    async def signal_error(self, error_msg: str):
        """Signal an error to all consumers."""
        self._error = error_msg
        self._complete = True
        self._schema_available.set()  # unblock register_consumer if waiting
        self._batch_available.set()  # wake any waiting consumers

    # -- Consumer API --

    async def register_consumer(
        self, consumer_id: str, start_batch_id: Optional[int] = None,
        partition_ids: Optional[List[int]] = None,
    ) -> Optional[bytes]:
        """Register a consumer. Blocks until schema is available.

        Returns serialized schema bytes, or None if stream completed with no batches.
        If start_batch_id is provided, the consumer resumes from that batch offset.
        If partition_ids is provided, pull_batch will only return those partition refs.
        """
        # Block until schema is available or stream ends
        while self._schema is None and not self._complete:
            self._schema_available.clear()
            if self._schema is not None or self._complete:
                break
            await self._schema_available.wait()

        # If error was signaled, raise
        if self._error is not None:
            raise RuntimeError(f"Stream error: {self._error}")

        # Stream completed with no data
        if self._schema is None:
            return None

        # Set cursor
        if start_batch_id is not None:
            # Validate start_batch_id is not GC'd
            if self._buffer:
                first_buffered = next(iter(self._buffer))
                if start_batch_id < first_buffered:
                    raise ValueError(
                        f"start_batch_id {start_batch_id} has been GC'd "
                        f"(earliest buffered: {first_buffered})"
                    )
            elif start_batch_id < self._next_batch_id:
                # Buffer is empty — all past batches were GC'd
                raise ValueError(
                    f"start_batch_id {start_batch_id} has been GC'd "
                    f"(all {self._next_batch_id} batches consumed and GC'd)"
                )
            self._consumers[consumer_id] = start_batch_id
        else:
            # Cursor starts at the earliest available batch
            if self._buffer:
                first_key = next(iter(self._buffer))
                self._consumers[consumer_id] = first_key
            else:
                self._consumers[consumer_id] = self._next_batch_id

        self._consumer_partitions[consumer_id] = partition_ids
        return self._schema.serialize().to_pybytes()

    async def pull_batch(
        self, consumer_id: str, timeout: Optional[float] = None
    ) -> Optional[dict]:
        """Pull the next batch for this consumer. Returns None when stream is done or on timeout."""
        if consumer_id not in self._consumers:
            raise ValueError(f"Unknown consumer: {consumer_id}")

        pull_start = time.time()

        while True:
            # Check for error
            if self._error is not None:
                raise RuntimeError(f"Stream error: {self._error}")

            cursor = self._consumers[consumer_id]

            if cursor in self._buffer:
                mb = self._buffer[cursor]
                self._consumers[consumer_id] = cursor + 1
                self._gc_batches()
                self._m_pull_latency.observe((time.time() - pull_start) * 1000)

                # Emit consumer lag
                lag = self._next_batch_id - (cursor + 1)
                self._m_consumer_lag.set(
                    lag,
                    {"stream_id": self._stream_id, "consumer_id": consumer_id},
                )

                # Filter partition refs if consumer has assigned partitions
                assigned = self._consumer_partitions.get(consumer_id)
                if assigned is not None:
                    filtered_refs = [
                        mb.partition_refs[i] for i in assigned
                        if i < len(mb.partition_refs)
                    ]
                    filtered_rows = None
                else:
                    filtered_refs = mb.partition_refs
                    filtered_rows = mb.num_rows

                return {
                    "batch_id": mb.batch_id,
                    "partition_refs": filtered_refs,
                    "num_rows": filtered_rows,
                    "timestamp": mb.timestamp,
                    "watermark": mb.watermark,
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
            try:
                await asyncio.wait_for(self._batch_available.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

    async def deregister_consumer(self, consumer_id: str):
        """Remove a consumer and GC any batches it was holding back."""
        self._consumers.pop(consumer_id, None)
        self._consumer_partitions.pop(consumer_id, None)
        self._gc_batches()

    # -- Drain --

    async def wait_for_drain(self, timeout: float = 30.0) -> bool:
        """Wait until all consumers have consumed all batches. Returns False on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._consumers:
                return True
            all_drained = all(c >= self._next_batch_id for c in self._consumers.values())
            if all_drained:
                return True
            await asyncio.sleep(0.1)
        return False

    # -- Internal --

    def _gc_batches(self):
        """Remove batches that all consumers have already read.

        Dropping the MicroBatch from the buffer releases the coordinator's
        reference to the ObjectRefs. The actual object store memory is freed
        by Ray's reference counting once consumers also drop their refs
        (after ray.get). We intentionally do NOT call ray.internal.free()
        here because consumers may still hold unresolved ObjectRefs returned
        by pull_batch.
        """
        if not self._consumers:
            return
        min_cursor = min(self._consumers.values())
        to_remove = [bid for bid in self._buffer if bid < min_cursor]
        for bid in to_remove:
            self._buffered_bytes -= self._buffer[bid].num_bytes
            del self._buffer[bid]
            self._batches_gc += 1
            self._m_gc.inc()
        if to_remove:
            self._m_buffer_size.set(len(self._buffer))
            self._m_buffered_bytes.set(self._buffered_bytes)
        if (len(self._buffer) < self._max_buffered
                and self._buffered_bytes < self._max_bytes):
            self._space_available.set()

    # -- Observability --

    async def get_watermark(self) -> Optional[str]:
        return self._latest_watermark

    async def get_stats(self) -> dict:
        return {
            "stream_id": self._stream_id,
            "batches_published": self._batches_published,
            "batches_gc": self._batches_gc,
            "buffer_size": len(self._buffer),
            "max_buffered": self._max_buffered,
            "buffered_bytes": self._buffered_bytes,
            "max_buffered_bytes": self._max_bytes,
            "num_consumers": len(self._consumers),
            "consumer_cursors": dict(self._consumers),
            "complete": self._complete,
            "error": self._error,
            "latest_watermark": self._latest_watermark,
        }
