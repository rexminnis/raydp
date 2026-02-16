import logging
import uuid
from typing import List, Optional

import pyarrow as pa
import ray

from raydp.streaming.coordinator import StreamCoordinator

logger = logging.getLogger(__name__)


class SparkStreamingSink:
    """Bridges Spark Structured Streaming to a StreamCoordinator.

    Used as a foreachBatch sink: each micro-batch DataFrame is converted to
    an Arrow table via toArrow() (PySpark 4.x native path), put into the Ray
    object store, and published to the coordinator as lightweight ObjectRefs.

    Thread safety: Spark's micro-batch engine serializes foreachBatch calls —
    the next micro-batch does not start until process_batch returns. This
    means process_batch (and the JVM bridge in partitioned mode) is never
    called concurrently, so no additional synchronization is needed.

    Arrow fast-path: Both toArrow() and Spark411SQLHelper.toArrowBatchRdd()
    respect the Spark session config. RayDP sets lz4 compression
    (spark.sql.execution.arrow.compression.codec) and unlimited batch size
    (spark.sql.execution.arrow.maxRecordsPerBatch=0) by default in
    ray_cluster.py, so the streaming path inherits these optimizations
    automatically.
    """

    def __init__(self, stream_id: str = None, max_buffered_batches: int = 64, partitioned: bool = False):
        self._stream_id = stream_id or f"stream_{uuid.uuid4().hex[:8]}"
        self._partitioned = partitioned
        self._query = None

        actor_name = f"stream_coord_{self._stream_id}"
        try:
            self._coordinator = ray.get_actor(actor_name)
        except ValueError:
            self._coordinator = StreamCoordinator.options(
                name=actor_name, lifetime="detached",
            ).remote(
                stream_id=self._stream_id,
                max_buffered_batches=max_buffered_batches,
            )

    def set_query(self, query):
        """Set the Spark StreamingQuery for watermark extraction."""
        self._query = query

    def process_batch(self, batch_df, batch_id):
        """foreachBatch callback: convert DataFrame → Arrow → object store → coordinator."""
        if batch_df.isEmpty():
            return

        watermark = self._extract_watermark()

        if self._partitioned:
            tables = self._collect_partitions_as_arrow(batch_df)
            refs = [ray.put(t) for t in tables]
            schema_bytes = tables[0].schema.serialize().to_pybytes()
            total_rows = sum(t.num_rows for t in tables)
        else:
            # Use PySpark 4.x native Arrow path — skips Pandas intermediate
            table = batch_df.toArrow()
            refs = [ray.put(table)]
            schema_bytes = table.schema.serialize().to_pybytes()
            total_rows = table.num_rows

        ray.get(
            self._coordinator.publish_batch.remote(
                refs, schema_bytes, total_rows, watermark
            )
        )

    def _collect_partitions_as_arrow(self, batch_df) -> List[pa.Table]:
        """Collect each Spark partition as a separate Arrow table via JVM bridge."""
        jvm = batch_df.sparkSession.sparkContext._jvm
        helper = jvm.org.apache.spark.sql.Spark411SQLHelper
        arrow_rdd = helper.toArrowBatchRdd(batch_df._jdf)
        # collect() returns Java ArrayList of byte[] — one per Spark partition
        java_partitions = arrow_rdd.toJavaRDD().collect()
        tables = []
        for ipc_bytes in java_partitions:
            reader = pa.ipc.open_stream(pa.py_buffer(bytes(ipc_bytes)))
            tables.append(reader.read_all())
        return tables

    def stop(self):
        """Signal the coordinator that no more batches will arrive."""
        ray.get(self._coordinator.signal_complete.remote())

    def _extract_watermark(self) -> Optional[str]:
        try:
            if self._query is None:
                return None
            progress = self._query.lastProgress
            if progress and progress.get("eventTime"):
                return progress["eventTime"].get("watermark")
        except Exception:
            pass
        return None

    @property
    def coordinator(self):
        """The underlying StreamCoordinator actor handle."""
        return self._coordinator

    @property
    def stream_id(self):
        return self._stream_id
