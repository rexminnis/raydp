import logging
import uuid
from typing import Optional

import pyarrow as pa
import ray

from raydp.streaming.coordinator import StreamCoordinator

logger = logging.getLogger(__name__)


class SparkStreamingSink:
    """Bridges Spark Structured Streaming to a StreamCoordinator.

    Used as a foreachBatch sink: each micro-batch DataFrame is converted to
    an Arrow table via toPandas(), put into the Ray object store, and
    published to the coordinator as lightweight ObjectRefs.
    """

    def __init__(self, stream_id: str = None, max_buffered_batches: int = 64):
        self._stream_id = stream_id or f"stream_{uuid.uuid4().hex[:8]}"
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

        pdf = batch_df.toPandas()
        table = pa.Table.from_pandas(pdf)
        ref = ray.put(table)

        schema_bytes = table.schema.serialize().to_pybytes()

        ray.get(
            self._coordinator.publish_batch.remote(
                [ref], schema_bytes, table.num_rows, watermark
            )
        )

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
