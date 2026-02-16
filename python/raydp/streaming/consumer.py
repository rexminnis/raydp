import uuid
from typing import Optional

import ray
import ray.data


class StreamingIterator:
    """Pulls Arrow tables from a StreamCoordinator in real-time.

    Two consumption modes:
      - __iter__(): yields pa.Table as each micro-batch arrives
      - iter_datasets(window_size): accumulates N tables into a
        ray.data.Dataset per window
    """

    def __init__(self, coordinator, consumer_id: str = None, start_batch_id: Optional[int] = None):
        self._coordinator = coordinator
        self._consumer_id = consumer_id or f"consumer_{uuid.uuid4().hex[:8]}"
        self._start_batch_id = start_batch_id
        self._last_batch_id: Optional[int] = None

    @property
    def consumer_id(self):
        return self._consumer_id

    @property
    def last_batch_id(self) -> Optional[int]:
        """The batch_id of the most recently consumed batch, for checkpointing."""
        return self._last_batch_id

    def __iter__(self):
        ray.get(self._coordinator.register_consumer.remote(
            self._consumer_id, self._start_batch_id
        ))
        try:
            while True:
                result = ray.get(
                    self._coordinator.pull_batch.remote(self._consumer_id)
                )
                if result is None:
                    return
                self._last_batch_id = result["batch_id"]
                # Resolve partition refs to Arrow tables
                tables = ray.get(result["partition_refs"])
                for table in tables:
                    yield table
        finally:
            ray.get(
                self._coordinator.deregister_consumer.remote(self._consumer_id)
            )

    def iter_datasets(self, window_size: int):
        """Yield ray.data.Dataset windows of `window_size` batches each."""
        ray.get(self._coordinator.register_consumer.remote(
            self._consumer_id, self._start_batch_id
        ))
        try:
            window_refs = []
            while True:
                result = ray.get(
                    self._coordinator.pull_batch.remote(self._consumer_id)
                )
                if result is None:
                    break
                self._last_batch_id = result["batch_id"]
                window_refs.extend(result["partition_refs"])
                if len(window_refs) >= window_size:
                    yield ray.data.from_arrow_refs(window_refs)
                    window_refs = []
            if window_refs:
                yield ray.data.from_arrow_refs(window_refs)
        finally:
            ray.get(
                self._coordinator.deregister_consumer.remote(self._consumer_id)
            )
