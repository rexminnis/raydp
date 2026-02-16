import uuid

from raydp.streaming.coordinator import StreamCoordinator
from raydp.streaming.consumer import StreamingIterator
from raydp.streaming.sink import SparkStreamingSink


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
    query = _StreamingQueryWrapper(raw_query, sink)
    iterator = StreamingIterator(sink.coordinator)

    return iterator, query


class _StreamingQueryWrapper:
    """Wraps a Spark StreamingQuery so that stop() also signals the coordinator."""

    def __init__(self, query, sink):
        self._query = query
        self._sink = sink

    def stop(self):
        self._query.stop()
        self._sink.stop()

    def __getattr__(self, name):
        return getattr(self._query, name)


__all__ = [
    "StreamCoordinator",
    "SparkStreamingSink",
    "StreamingIterator",
    "from_spark_streaming",
]
