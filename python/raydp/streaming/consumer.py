import uuid
from typing import List, Optional

import ray
import ray.data


class StreamingIterator:
    """Pulls Arrow tables from a StreamCoordinator in real-time.

    Three consumption modes:
      - __iter__(): yields pa.Table as each micro-batch arrives
      - iter_datasets(window_size): accumulates N tables into a
        ray.data.Dataset per window
      - iter_spark_dataframes(spark, window_size): like iter_datasets, but each
        window is read into a Spark DataFrame EXECUTOR-SIDE (the executors resolve
        the Ray ObjectRefs; no driver funnel). Requires a RayDP cluster.
    """

    def __init__(self, coordinator, consumer_id: str = None, start_batch_id: Optional[int] = None,
                 partition_ids: Optional[List[int]] = None):
        self._coordinator = coordinator
        self._consumer_id = consumer_id or f"consumer_{uuid.uuid4().hex[:8]}"
        self._start_batch_id = start_batch_id
        self._partition_ids = partition_ids
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
            self._consumer_id, self._start_batch_id, self._partition_ids
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
            self._consumer_id, self._start_batch_id, self._partition_ids
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

    def iter_spark_dataframes(self, spark, window_size: int):
        """Yield executor-side Spark DataFrames, ``window_size`` batches each.

        Like :meth:`iter_datasets`, but each window is read into a Spark
        DataFrame whose partitions are resolved from the Ray object store BY THE
        SPARK EXECUTORS (via ``ray_dataset_to_spark_dataframe``) rather than being
        pulled to the driver and re-shipped. The driver only handles the (tiny)
        ObjectRef metadata; the batch bytes flow Ray-object-store -> executor
        directly, with one Spark partition per Ray block. Requires a RayDP cluster
        (Spark executors running as Ray actors).

        :param spark: the RayDP SparkSession.
        :param window_size: number of coordinator batches (partition refs) per
            emitted DataFrame.
        """
        from raydp.spark.dataset import ray_dataset_to_spark_dataframe

        for ds in self.iter_datasets(window_size):
            block_refs = [
                block_ref
                for ref_bundle in ds.iter_internal_ref_bundles()
                for block_ref, _ in ref_bundle.blocks
            ]
            yield ray_dataset_to_spark_dataframe(spark, ds.schema(), block_refs)
