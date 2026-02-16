import pyarrow as pa
import ray


def create_streaming_sink(coordinator):
    """Return a foreachBatch callback that pushes micro-batches to a coordinator.

    Each Spark micro-batch DataFrame is converted to an Arrow table via
    toPandas() -> pa.Table.from_pandas(). This is the simple path for the
    spike; production would use the JVM Arrow path.
    """

    def sink_fn(batch_df, batch_id):
        pdf = batch_df.toPandas()
        table = pa.Table.from_pandas(pdf)
        ray.get(coordinator.put_batch.remote(table))

    return sink_fn
