import logging
import time
import uuid
from typing import List, Optional

import pyarrow as pa
from pyspark.storagelevel import StorageLevel
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

    def __init__(self, stream_id: str = None, max_buffered_batches: int = 64,
                 max_buffered_bytes: int = 2 * 1024**3, partitioned: bool = False):
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
                max_buffered_bytes=max_buffered_bytes,
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
            # _owner transfers object lifetime to the detached coordinator actor,
            # so objects survive if the Spark driver crashes. This is an underscore-
            # prefixed Ray API, stable since Ray 2.x; falling back to driver
            # ownership (the default) is safe if Ray removes it in a future version.
            refs = [ray.put(t, _owner=self._coordinator) for t in tables]
            schema_bytes = tables[0].schema.serialize().to_pybytes()
            total_rows = sum(t.num_rows for t in tables)
            total_bytes = sum(t.nbytes for t in tables)
        else:
            # Use PySpark 4.x native Arrow path — skips Pandas intermediate
            table = batch_df.toArrow()
            refs = [ray.put(table, _owner=self._coordinator)]
            schema_bytes = table.schema.serialize().to_pybytes()
            total_rows = table.num_rows
            total_bytes = table.nbytes

        ray.get(
            self._coordinator.publish_batch.remote(
                refs, schema_bytes, total_rows, watermark, num_bytes=total_bytes,
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


def _enable_load_code_from_local():
    """Enable Ray cross-language support via internal API (task worker only)."""
    try:
        from ray._private.worker import global_worker as w
        w.set_load_code_from_local(True)
    except Exception:
        pass


@ray.remote(max_retries=-1)
def _fetch_and_store_partition(
    executor_actor_name: str,
    rdd_id: int,
    partition_id: int,
    schema_json: str,
    driver_agent_url: str,
    coordinator_handle,
) -> dict:
    """Fetch Arrow IPC from a JVM executor actor, decode to pa.Table,
    put into object store owned by the coordinator, and return metadata.

    Returns a dict with keys: ref, num_rows, nbytes, schema_bytes.
    The table data stays in the object store — only the lightweight
    metadata dict is resolved on the driver.
    """
    _enable_load_code_from_local()
    executor_actor = ray.get_actor(executor_actor_name)
    ipc_bytes = ray.get(
        executor_actor.getRDDPartition.remote(
            rdd_id, partition_id, schema_json, driver_agent_url
        )
    )
    reader = pa.ipc.open_stream(pa.BufferReader(ipc_bytes))
    table = reader.read_all()
    table_ref = ray.put(table, _owner=coordinator_handle)
    return {
        "ref": table_ref,
        "num_rows": table.num_rows,
        "nbytes": table.nbytes,
        "schema_bytes": table.schema.serialize().to_pybytes(),
    }


class JvmStreamingSink:
    """Distributed streaming sink that bypasses the PySpark driver bottleneck.

    Instead of collecting Arrow data through the driver process, this sink
    dispatches Ray tasks to fetch Arrow IPC bytes directly from Spark executor
    actors and put them into the Ray object store. The driver only handles
    lightweight ObjectRef handles (~20 bytes each) and metadata dicts.

    Requires a RayDP cluster with executor actors running (i.e., the Spark
    session was created via raydp.init_spark).

    Data flow:
        Executor → BlockManager.persist()
        Ray task → executor.getRDDPartition() → pa.Table → ray.put() → Object Store
        Driver only collects: ObjectRef handles + metadata dicts (~200 bytes each)
    """

    def __init__(self, stream_id: str = None, max_buffered_batches: int = 64,
                 max_buffered_bytes: int = 2 * 1024**3):
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
                max_buffered_bytes=max_buffered_bytes,
            )

        self._fetch_num_cpus = 0.0
        self._fetch_memory = 0.0

    def set_query(self, query):
        """Set the Spark StreamingQuery for watermark extraction."""
        self._query = query

    def configure_fetch_resources(self, num_cpus: float = 0.0, memory: float = 0.0):
        """Configure resource requirements for fetch tasks."""
        self._fetch_num_cpus = num_cpus
        self._fetch_memory = memory

    def process_batch(self, batch_df, batch_id):
        """foreachBatch callback: distributed fetch via executor actors."""
        if batch_df.isEmpty():
            return

        watermark = self._extract_watermark()

        sc = batch_df.sparkSession.sparkContext
        jvm = sc._jvm
        object_store_writer = jvm.org.apache.spark.sql.raydp.ObjectStoreWriter
        storage_level = sc._getJavaStorageLevel(StorageLevel.MEMORY_AND_DISK)

        handle = object_store_writer.startStreamingRecoverableRDD(
            batch_df._jdf, storage_level
        )
        rdd_id = handle.rddId()
        num_partitions = handle.numPartitions()
        schema_json = handle.schemaJson()
        driver_agent_url = handle.driverAgentUrl()

        task_opts = {
            "num_cpus": self._fetch_num_cpus,
            "memory": self._fetch_memory,
        }
        fetch_task = _fetch_and_store_partition.options(**task_opts)

        # Poll for completed partitions and dispatch fetch tasks
        task_refs = [None] * num_partitions
        dispatched = set()

        while len(dispatched) < num_partitions:
            err = handle.getError()
            if err is not None:
                handle.unpersist()
                raise RuntimeError(f"Spark materialization failed: {err}")

            ready = handle.getReadyPartitions()
            new_count = 0
            for i in range(num_partitions):
                if i not in dispatched and ready[i] is not None:
                    executor_actor_name = f"raydp-executor-{ready[i]}"
                    task_refs[i] = fetch_task.remote(
                        executor_actor_name, rdd_id, i,
                        schema_json, driver_agent_url,
                        self._coordinator,
                    )
                    dispatched.add(i)
                    new_count += 1

            if len(dispatched) < num_partitions and new_count == 0:
                time.sleep(0.05)

        # Resolve metadata dicts (NOT table data — only ~200 bytes each)
        results = ray.get(list(task_refs))
        partition_refs = [r["ref"] for r in results]
        schema_bytes = results[0]["schema_bytes"]
        total_rows = sum(r["num_rows"] for r in results)
        total_bytes = sum(r["nbytes"] for r in results)

        ray.get(
            self._coordinator.publish_batch.remote(
                partition_refs, schema_bytes, total_rows, watermark,
                num_bytes=total_bytes,
            )
        )

        handle.unpersist()

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
