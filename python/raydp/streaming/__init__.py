import logging
import uuid
from typing import List

import ray

from raydp.streaming.coordinator import StreamCoordinator
from raydp.streaming.consumer import StreamingIterator
from raydp.streaming.sink import SparkStreamingSink
from raydp.streaming.monitor import print_stats

logger = logging.getLogger(__name__)


def from_spark_streaming(
    streaming_df,
    stream_id=None,
    max_buffered_batches=64,
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
    trigger : dict, optional
        Spark trigger config, e.g. {"processingTime": "2 seconds"}.
    checkpoint_location : str, optional
        Spark checkpoint directory for the streaming query.
    """
    stream_id = stream_id or f"stream_{uuid.uuid4().hex[:8]}"
    sink = SparkStreamingSink(
        stream_id=stream_id, max_buffered_batches=max_buffered_batches
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


__all__ = [
    "StreamCoordinator",
    "SparkStreamingSink",
    "StreamingIterator",
    "from_spark_streaming",
    "create_partitioned_iterators",
    "print_stats",
]
