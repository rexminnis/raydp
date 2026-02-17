"""Reverse bridge: stream Ray-side Arrow data into Spark Structured Streaming.

Uses a JVM-native DataSource V2 MicroBatchStream (RayStreamingTableProvider)
instead of PySpark's Python Data Source API, avoiding subprocess forking issues.

Architecture:
  Producer (user code) → _ProducerThread → StreamCoordinator
                                               ↓
                                         _JvmBridgeThread → RayStreamingState (JVM)
                                               ↓
                                         MicroBatchStream → Spark Streaming DataFrame
"""

import logging
import threading
import time
from typing import Optional

import pyarrow as pa
import ray

from raydp.streaming.coordinator import StreamCoordinator

logger = logging.getLogger(__name__)


def _arrow_type_to_spark_ddl(arrow_type: pa.DataType) -> str:
    """Convert a PyArrow type to a Spark DDL type string."""
    mapping = {
        pa.int8(): "TINYINT",
        pa.int16(): "SMALLINT",
        pa.int32(): "INT",
        pa.int64(): "BIGINT",
        pa.float16(): "FLOAT",
        pa.float32(): "FLOAT",
        pa.float64(): "DOUBLE",
        pa.string(): "STRING",
        pa.large_string(): "STRING",
        pa.utf8(): "STRING",
        pa.large_utf8(): "STRING",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
        pa.date64(): "DATE",
        pa.binary(): "BINARY",
        pa.large_binary(): "BINARY",
    }
    if arrow_type in mapping:
        return mapping[arrow_type]
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_decimal(arrow_type):
        return f"DECIMAL({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        inner = _arrow_type_to_spark_ddl(arrow_type.value_type)
        return f"ARRAY<{inner}>"
    if pa.types.is_map(arrow_type):
        k = _arrow_type_to_spark_ddl(arrow_type.key_type)
        v = _arrow_type_to_spark_ddl(arrow_type.item_type)
        return f"MAP<{k},{v}>"
    if pa.types.is_struct(arrow_type):
        fields = ", ".join(
            f"`{f.name}`: {_arrow_type_to_spark_ddl(f.type)}"
            for f in arrow_type
        )
        return f"STRUCT<{fields}>"
    raise ValueError(f"Unsupported Arrow type: {arrow_type}")


class _ProducerThread(threading.Thread):
    """Daemon thread that iterates a user-provided source and publishes to coordinator.

    Handles both Iterator[pa.Table] and Callable[[], Optional[pa.Table]] sources.
    Calls signal_complete() on exhaustion or signal_error() on exception.
    """

    def __init__(self, source, coordinator, stream_id: str):
        super().__init__(daemon=True, name=f"ray-producer-{stream_id}")
        self._source = source
        self._coordinator = coordinator
        self._stream_id = stream_id
        self._error = None

    @property
    def error(self):
        return self._error

    def run(self):
        try:
            if callable(self._source) and not hasattr(self._source, "__next__"):
                self._run_callable()
            else:
                self._run_iterator()
            ray.get(self._coordinator.signal_complete.remote())
        except Exception as e:
            self._error = e
            try:
                ray.get(self._coordinator.signal_error.remote(str(e)))
            except Exception:
                pass

    def _run_iterator(self):
        for table in self._source:
            self._publish(table)

    def _run_callable(self):
        while True:
            table = self._source()
            if table is None:
                break
            self._publish(table)

    def _publish(self, table: pa.Table):
        # Don't use _owner=coordinator here — ray.put with _owner requires the
        # actor's worker to be fully alive. The producer runs in the user's
        # process, so normal driver ownership is fine. The coordinator holds
        # a ref to the ObjectRef in its buffer, preventing GC.
        ref = ray.put(table)
        schema_bytes = table.schema.serialize().to_pybytes()
        ray.get(self._coordinator.publish_batch.remote(
            [ref], schema_bytes, table.num_rows, num_bytes=table.nbytes,
        ))


class _JvmBridgeThread(threading.Thread):
    """Daemon thread that pulls batches from the coordinator and pushes Arrow IPC
    data to the JVM-side RayStreamingState via py4j.

    This is the push model: Python resolves ObjectRefs and forwards the data
    to JVM. No py4j callbacks needed.

    Invariant: setLatestBatchId(N) is called only after addBatch(N, data) has
    completed, guaranteeing data availability for the MicroBatchStream.
    """

    def __init__(self, coordinator, jvm_state, stream_id: str):
        super().__init__(daemon=True, name=f"ray-jvm-bridge-{stream_id}")
        self._coordinator = coordinator
        self._jvm_state = jvm_state
        self._stream_id = stream_id
        self._consumer_id = "__jvm_bridge__"
        self._error: Optional[Exception] = None
        self._stop_event = threading.Event()

    @property
    def error(self) -> Optional[Exception]:
        return self._error

    def stop(self):
        self._stop_event.set()

    def run(self):
        try:
            # Register as a consumer on the coordinator
            ray.get(self._coordinator.register_consumer.remote(
                self._consumer_id
            ))
            self._pump_loop()
        except Exception as e:
            self._error = e
            logger.error("JVM bridge thread error for stream %s: %s",
                         self._stream_id, e)
            try:
                self._jvm_state.setError(str(e))
            except Exception:
                pass
        finally:
            try:
                ray.get(self._coordinator.deregister_consumer.remote(
                    self._consumer_id
                ))
            except Exception:
                pass

    def _pump_loop(self):
        last_acked = -1

        while not self._stop_event.is_set():
            # Check if Spark has committed batches we can ack
            committed = self._jvm_state.getCommittedBatchId()
            if committed > last_acked:
                ray.get(self._coordinator.ack_committed.remote(committed))
                last_acked = committed

            # Pull next batch from coordinator
            result = ray.get(self._coordinator.pull_batch.remote(
                self._consumer_id, timeout=1.0
            ))

            if result is None:
                # Check if stream is complete
                stats = ray.get(self._coordinator.get_stats.remote())
                if stats["complete"]:
                    # Do one final commit ack
                    committed = self._jvm_state.getCommittedBatchId()
                    if committed > last_acked:
                        ray.get(
                            self._coordinator.ack_committed.remote(committed)
                        )
                    self._jvm_state.setComplete()
                    return
                continue

            batch_id = result["batch_id"]
            partition_refs = result["partition_refs"]

            # Resolve ObjectRefs to Arrow tables
            tables = ray.get(partition_refs)

            # Serialize all partition tables into a single Arrow IPC stream
            if tables:
                # Use the first table's schema for the IPC stream
                sink = pa.BufferOutputStream()
                writer = pa.ipc.new_stream(sink, tables[0].schema)
                for table in tables:
                    writer.write_table(table)
                writer.close()
                ipc_bytes = sink.getvalue().to_pybytes()
            else:
                ipc_bytes = bytes()

            # Push to JVM state (py4j forward call)
            self._jvm_state.addBatch(batch_id, ipc_bytes)
            # INVARIANT: setLatestBatchId only after addBatch completes
            self._jvm_state.setLatestBatchId(batch_id)
