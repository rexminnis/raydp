import logging
import uuid
from typing import Callable, Iterator, List, Optional, Tuple, Union

import pyarrow as pa
import ray

from raydp.streaming.coordinator import StreamCoordinator
from raydp.streaming.consumer import StreamingIterator
from raydp.streaming.sink import SparkStreamingSink
from raydp.streaming.source import (
    _JvmBridgeThread,
    _ProducerThread,
)
from raydp.streaming.monitor import print_stats

logger = logging.getLogger(__name__)


def from_spark_streaming(
    streaming_df,
    stream_id=None,
    max_buffered_batches=64,
    max_buffered_bytes=2 * 1024**3,
    trigger=None,
    checkpoint_location=None,
):
    """Start consuming a Spark Structured Streaming DataFrame via Ray.

    Returns (StreamingIterator, StreamingQuery). The iterator yields
    pa.Table batches in real-time. Call query.stop() to end the stream.

    Parameters
    ----------
    streaming_df : pyspark.sql.DataFrame
        A streaming DataFrame (e.g. from spark.readStream).
    stream_id : str, optional
        Unique identifier for the stream. Auto-generated if not provided.
    max_buffered_batches : int
        Maximum number of micro-batches buffered before backpressure.
    max_buffered_bytes : int
        Maximum total bytes buffered before backpressure (default 2 GB).
    trigger : dict, optional
        Spark trigger config, e.g. {"processingTime": "2 seconds"}.
    checkpoint_location : str, optional
        Spark checkpoint directory for the streaming query.
    """
    stream_id = stream_id or f"stream_{uuid.uuid4().hex[:8]}"
    sink = SparkStreamingSink(
        stream_id=stream_id, max_buffered_batches=max_buffered_batches,
        max_buffered_bytes=max_buffered_bytes,
    )

    writer = streaming_df.writeStream.foreachBatch(sink.process_batch)

    if trigger:
        writer = writer.trigger(**trigger)
    if checkpoint_location:
        writer = writer.option("checkpointLocation", checkpoint_location)

    raw_query = writer.start()
    sink.set_query(raw_query)

    query = _StreamingQueryWrapper(raw_query, sink)
    iterator = StreamingIterator(sink.coordinator)

    return iterator, query


class _StreamingQueryWrapper:
    """Wraps a Spark StreamingQuery so that stop() also signals the coordinator."""

    def __init__(self, query, sink):
        self._query = query
        self._sink = sink

    def stop(self, drain_timeout=30.0):
        self._query.stop()           # 1. Stop Spark (no more batches)
        self._sink.stop()             # 2. signal_complete on coordinator

        # 3. Wait for consumers to drain
        coordinator = self._sink.coordinator
        try:
            drained = ray.get(coordinator.wait_for_drain.remote(timeout=drain_timeout))
            if not drained:
                logger.warning(
                    "Stream %s: not all consumers drained within %.1fs",
                    self._sink.stream_id, drain_timeout,
                )
        except Exception:
            pass

        # 4. Kill detached actor
        try:
            ray.kill(coordinator)
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self._query, name)


def create_partitioned_iterators(
    coordinator, num_consumers: int, num_partitions: int,
) -> List[StreamingIterator]:
    """Create N StreamingIterators with round-robin partition assignment.

    Args:
        coordinator: StreamCoordinator actor handle.
        num_consumers: Number of consumers to create.
        num_partitions: Total partitions per batch.

    Returns:
        List of StreamingIterators with disjoint partition assignments.
    """
    partitions_per_consumer: List[List[int]] = [[] for _ in range(num_consumers)]
    for p in range(num_partitions):
        partitions_per_consumer[p % num_consumers].append(p)

    return [
        StreamingIterator(coordinator, partition_ids=parts)
        for parts in partitions_per_consumer
    ]


class _ReverseStreamHandle:
    """Handle returned by to_spark_streaming() for lifecycle management.

    Call stop() to shut down the producer thread, bridge thread, and coordinator.
    """

    def __init__(self, coordinator, stream_id: str, producer: _ProducerThread,
                 bridge: _JvmBridgeThread = None, jvm_state=None):
        self._coordinator = coordinator
        self._stream_id = stream_id
        self._producer = producer
        self._bridge = bridge
        self._jvm_state = jvm_state

    @property
    def coordinator(self):
        return self._coordinator

    @property
    def stream_id(self) -> str:
        return self._stream_id

    @property
    def producer_error(self) -> Optional[Exception]:
        return self._producer.error

    def stop(self, drain_timeout: float = 10.0):
        """Stop the producer, bridge thread, and kill the coordinator."""
        if self._bridge is not None:
            self._bridge.stop()
            self._bridge.join(timeout=drain_timeout)
        self._producer.join(timeout=drain_timeout)
        try:
            ray.kill(self._coordinator)
        except Exception:
            pass


def to_spark_streaming(
    source: Union[Iterator[pa.Table], Callable[[], Optional[pa.Table]]],
    spark,
    stream_id: str = None,
    max_buffered_batches: int = 64,
    max_buffered_bytes: int = 2 * 1024**3,
):
    """Stream Ray-side Arrow data into a Spark Structured Streaming DataFrame.

    Uses a JVM-native DataSource V2 MicroBatchStream. Python pushes resolved
    Arrow data to JVM via py4j forward calls (no callbacks).

    Parameters
    ----------
    source : Iterator[pa.Table] or Callable[[], Optional[pa.Table]]
        Data source. An iterator that yields Arrow tables, or a callable that
        returns an Arrow table on each call (return None to signal completion).
    spark : SparkSession
        Active Spark session.
    stream_id : str, optional
        Unique stream identifier. Auto-generated if not provided.
    max_buffered_batches : int
        Maximum micro-batches buffered before backpressure (default 64).
    max_buffered_bytes : int
        Maximum bytes buffered before backpressure (default 2 GB).

    Returns
    -------
    (DataFrame, _ReverseStreamHandle)
        A streaming DataFrame and a handle to control the stream lifecycle.
        Call handle.stop() to shut down the producer and coordinator.
    """
    stream_id = stream_id or f"stream_{uuid.uuid4().hex[:8]}"

    # Create coordinator (same pattern as SparkStreamingSink)
    actor_name = f"stream_coord_{stream_id}"
    try:
        coordinator = ray.get_actor(actor_name)
    except ValueError:
        coordinator = StreamCoordinator.options(
            name=actor_name, lifetime="detached",
        ).remote(
            stream_id=stream_id,
            max_buffered_batches=max_buffered_batches,
            max_buffered_bytes=max_buffered_bytes,
        )

    # Start producer thread (publishes tables to coordinator)
    producer = _ProducerThread(source, coordinator, stream_id)
    producer.start()

    # Wait for schema to become available (first batch published)
    schema_bytes = ray.get(
        coordinator.register_consumer.remote("__schema_probe__")
    )
    ray.get(coordinator.deregister_consumer.remote("__schema_probe__"))
    if schema_bytes is None:
        raise RuntimeError(
            f"Stream '{stream_id}' completed with no data — cannot infer schema"
        )
    arrow_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))

    # Convert Arrow schema → Spark StructType JSON
    from raydp.streaming.source import _arrow_type_to_spark_ddl
    from pyspark.sql.types import StructType
    schema_ddl = ", ".join(
        f"`{field.name}` {_arrow_type_to_spark_ddl(field.type)}"
        for field in arrow_schema
    )
    spark_schema = StructType.fromDDL(schema_ddl)
    schema_json = spark_schema.json()

    # Get the session timezone for Arrow IPC deserialization in JVM
    time_zone_id = spark.conf.get("spark.sql.session.timeZone", "UTC")

    # Create JVM-side RayStreamingState via py4j
    jvm = spark._jvm
    jvm_state = jvm.org.apache.spark.sql.raydp.RayStreamingState(
        stream_id, schema_json, time_zone_id,
    )
    jvm.org.apache.spark.sql.raydp.RayStreamingState.register(
        stream_id, jvm_state,
    )

    # Start the bridge thread that pushes Arrow IPC data from coordinator to JVM
    bridge = _JvmBridgeThread(coordinator, jvm_state, stream_id)
    bridge.start()

    # Create streaming DataFrame via JVM DataSource V2
    streaming_df = (
        spark.readStream
        .format("org.apache.spark.sql.raydp.RayStreamingTableProvider")
        .option("stream_id", stream_id)
        .load()
    )

    handle = _ReverseStreamHandle(
        coordinator, stream_id, producer,
        bridge=bridge, jvm_state=jvm_state,
    )
    return streaming_df, handle


__all__ = [
    "StreamCoordinator",
    "SparkStreamingSink",
    "StreamingIterator",
    "from_spark_streaming",
    "to_spark_streaming",
    "create_partitioned_iterators",
    "print_stats",
]
