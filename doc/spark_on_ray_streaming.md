# Spark on Ray Streaming Bridge

RayDP provides a bidirectional streaming bridge between Spark Structured Streaming and Ray. You can consume Spark streaming data in Ray (`from_spark_streaming`) or push Ray-side data into Spark Structured Streaming (`to_spark_streaming`).

## Quick Start

### Spark → Ray (forward bridge)

```python
import ray
import raydp
from raydp.streaming import from_spark_streaming

ray.init()
spark = raydp.init_spark("my_app", num_executors=2, executor_cores=2, executor_memory="2g")

# Create a Spark streaming source (e.g. Kafka, rate, file, etc.)
streaming_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", "1000")
    .load()
)

# Bridge to Ray — returns an iterator and a query handle
iterator, query = from_spark_streaming(
    streaming_df,
    trigger={"processingTime": "2 seconds"},
)

# Consume Arrow tables in real-time
for table in iterator:
    print(f"Got {table.num_rows} rows: {table.column_names}")

query.stop()
```

### Ray → Spark (reverse bridge)

```python
import pyarrow as pa
from raydp.streaming import to_spark_streaming

# Any iterator or callable that produces Arrow tables
def generate_tables():
    for i in range(100):
        yield pa.table({"id": [i], "value": [i * 1.5]})

streaming_df, handle = to_spark_streaming(generate_tables(), spark)

# Use Spark Structured Streaming as usual
query = (
    streaming_df.writeStream
    .format("console")
    .trigger(processingTime="1 second")
    .start()
)
query.awaitTermination(timeout=30)
query.stop()
handle.stop()
```

## API Reference

### `from_spark_streaming`

```python
from raydp.streaming import from_spark_streaming

iterator, query = from_spark_streaming(
    streaming_df,
    stream_id=None,
    max_buffered_batches=64,
    max_buffered_bytes=2 * 1024**3,
    trigger=None,
    checkpoint_location=None,
    use_jvm_sink=False,
)
```

Bridges a Spark Structured Streaming DataFrame into Ray. Returns a `StreamingIterator` and a wrapped `StreamingQuery`.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `streaming_df` | `DataFrame` | required | A Spark streaming DataFrame (e.g. from `spark.readStream`) |
| `stream_id` | `str` | auto | Unique identifier for the stream |
| `max_buffered_batches` | `int` | `64` | Max micro-batches buffered before backpressure kicks in |
| `max_buffered_bytes` | `int` | `2 * 1024**3` | Max bytes buffered before backpressure (default 2 GB) |
| `trigger` | `dict` | `None` | Spark trigger config, e.g. `{"processingTime": "2 seconds"}` |
| `checkpoint_location` | `str` | `None` | Spark checkpoint directory |
| `use_jvm_sink` | `bool` | `False` | Use the distributed JVM-native sink (see [JVM-Native Sink](#jvm-native-sink-distributed-mode)) |

**Returns:** `(StreamingIterator, StreamingQuery)`

### `to_spark_streaming`

```python
from raydp.streaming import to_spark_streaming

streaming_df, handle = to_spark_streaming(
    source,
    spark,
    stream_id=None,
    max_buffered_batches=64,
    max_buffered_bytes=2 * 1024**3,
)
```

Streams Ray-side Arrow data into a Spark Structured Streaming DataFrame.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `Iterator[pa.Table]` or `Callable` | required | Iterator yielding Arrow tables, or a callable returning `pa.Table` (return `None` to signal completion) |
| `spark` | `SparkSession` | required | Active Spark session |
| `stream_id` | `str` | auto | Unique stream identifier |
| `max_buffered_batches` | `int` | `64` | Max micro-batches buffered before backpressure |
| `max_buffered_bytes` | `int` | `2 * 1024**3` | Max bytes buffered before backpressure (default 2 GB) |

**Returns:** `(DataFrame, _ReverseStreamHandle)` — a streaming DataFrame and a lifecycle handle. Call `handle.stop()` when done.

### `StreamingIterator`

The iterator returned by `from_spark_streaming` supports two consumption modes:

**Direct iteration** — yields `pa.Table` as each micro-batch arrives:
```python
for table in iterator:
    process(table)
```

**Windowed Ray Datasets** — accumulates N batches into a `ray.data.Dataset`:
```python
for dataset in iterator.iter_datasets(window_size=5):
    # dataset is a ray.data.Dataset with 5 micro-batches concatenated
    dataset.map(transform_fn).write_parquet("/output")
```

**Properties:**
- `iterator.last_batch_id` — the batch ID of the most recently consumed batch (useful for checkpointing)
- `iterator.consumer_id` — this consumer's unique identifier

### `create_partitioned_iterators`

```python
from raydp.streaming import create_partitioned_iterators

iterators = create_partitioned_iterators(coordinator, num_consumers=4, num_partitions=8)
```

Creates N `StreamingIterator` instances with round-robin partition assignment. Each iterator receives a disjoint subset of partitions, enabling parallel consumption across Ray workers.

### `print_stats`

```python
from raydp.streaming import print_stats

print_stats(coordinator, interval=2.0, max_iterations=None)
```

Continuously prints coordinator stats (published batches, buffer usage, throughput, watermark) to stdout. Useful for development and debugging.

## Consumption Patterns

### Simple iteration

Consume each micro-batch as an Arrow table:

```python
iterator, query = from_spark_streaming(streaming_df)

for table in iterator:
    # table is a pyarrow.Table
    df = table.to_pandas()
    model.predict(df)

query.stop()
```

### Windowed Ray Datasets

Group micro-batches into fixed-size windows for batch-style processing with Ray Data:

```python
iterator, query = from_spark_streaming(streaming_df)

for dataset in iterator.iter_datasets(window_size=10):
    # dataset is a ray.data.Dataset containing 10 micro-batches
    result = dataset.map_batches(my_model.predict, batch_size=256)
    result.write_parquet(f"/output/{iterator.last_batch_id}")

query.stop()
```

### Parallel partitioned consumption

Distribute partitions across multiple Ray workers for parallel processing:

```python
from raydp.streaming import from_spark_streaming, create_partitioned_iterators

iterator, query = from_spark_streaming(
    streaming_df,
    stream_id="partitioned_stream",
)

# Create 4 consumers, each handling a subset of the 8 Spark partitions
iterators = create_partitioned_iterators(
    iterator._coordinator,  # access the underlying coordinator
    num_consumers=4,
    num_partitions=8,
)

# Launch each consumer as a Ray task or actor
@ray.remote
def consume(it):
    for table in it:
        process(table)

futures = [consume.remote(it) for it in iterators]
ray.get(futures)
query.stop()
```

### Resumable consumption

Use `last_batch_id` to implement checkpointing and resume from where you left off:

```python
iterator = StreamingIterator(
    coordinator,
    start_batch_id=last_checkpoint,  # resume from saved position
)

for table in iterator:
    process(table)
    save_checkpoint(iterator.last_batch_id)
```

## Backpressure

The bridge enforces backpressure with two independent limits:

- **Batch count**: `max_buffered_batches` (default 64) — the maximum number of unprocessed micro-batches in the buffer.
- **Byte budget**: `max_buffered_bytes` (default 2 GB) — the maximum total size of buffered data.

When either limit is reached, the Spark `foreachBatch` callback blocks until consumers pull enough data to free space. This prevents unbounded memory growth without dropping data.

You can monitor buffer usage with `print_stats`:

```python
import threading
from raydp.streaming import from_spark_streaming, print_stats

iterator, query = from_spark_streaming(streaming_df)

# Monitor in a background thread
monitor = threading.Thread(
    target=print_stats,
    args=(iterator._coordinator,),
    kwargs={"interval": 2.0},
    daemon=True,
)
monitor.start()
```

Output:
```
[my_stream] published=15 gc=10 buffer=5/64 bytes=3.2MB/2.0GB consumers=1 throughput=2.5 batches/s watermark=2024-01-15T10:30:00.000Z complete=False
```

## JVM-Native Sink (Distributed Mode)

By default, `from_spark_streaming` uses the `SparkStreamingSink` which collects all Arrow data through the PySpark driver process. This works well for small to medium micro-batches but becomes a bottleneck for large batches with many partitions.

The **JVM-native sink** (`use_jvm_sink=True`) bypasses this bottleneck by dispatching parallel Ray tasks that fetch Arrow IPC bytes directly from Spark executor actors and put them into the Ray object store. The driver only handles lightweight ObjectRef handles (~20 bytes each).

### Data flow comparison

```
Default sink:
  Executor → [collect to driver] → driver.toArrow() → driver.ray.put() → Object Store
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
              ALL DATA flows through a single process

JVM-native sink:
  Executor → BlockManager.persist()
  Ray task → executor.getRDDPartition() → pa.Table → ray.put() → Object Store
  Driver only collects: ObjectRef handles + metadata dicts (~200 bytes each)
```

### Usage

```python
iterator, query = from_spark_streaming(
    streaming_df,
    trigger={"processingTime": "2 seconds"},
    use_jvm_sink=True,  # enable distributed mode
)
```

### When to use each mode

| | Default (`use_jvm_sink=False`) | JVM-native (`use_jvm_sink=True`) |
|---|---|---|
| **Data path** | All data through PySpark driver | Distributed via executor actors |
| **Best for** | Small micro-batches, simple setup | Large micro-batches, many partitions |
| **Requires** | Any Spark setup | RayDP cluster (`raydp.init_spark`) |
| **Driver memory** | O(batch_size) | O(num_partitions * 200 bytes) |
| **Consumer impact** | None — both yield `pa.Table` | None — both yield `pa.Table` |

### Requirements

The JVM-native sink requires:

1. A Spark session started via `raydp.init_spark` (so executor actors are running).
2. The RayDP JARs must include the `ObjectStoreWriter` class (included by default).

There is no consumer-side impact — `ray.get(partition_ref)` returns `pa.Table` identically in both modes. Existing consumer code works without changes.

## Reverse Bridge (Ray → Spark)

The reverse bridge lets you push Arrow data produced by Ray into a Spark Structured Streaming query.

### Iterator source

```python
import pyarrow as pa
from raydp.streaming import to_spark_streaming

tables = [pa.table({"x": range(i*10, (i+1)*10)}) for i in range(20)]
streaming_df, handle = to_spark_streaming(iter(tables), spark)

# Write to any Spark sink
query = (
    streaming_df.writeStream
    .format("parquet")
    .option("path", "/output")
    .option("checkpointLocation", "/checkpoint")
    .trigger(processingTime="1 second")
    .start()
)

query.awaitTermination(timeout=60)
query.stop()
handle.stop()
```

### Callable source

Use a callable for dynamic data generation (return `None` to signal completion):

```python
import time

call_count = 0

def data_source():
    global call_count
    if call_count >= 100:
        return None
    table = pa.table({"ts": [time.time()], "value": [call_count]})
    call_count += 1
    return table

streaming_df, handle = to_spark_streaming(data_source, spark)
```

### Lifecycle management

The `_ReverseStreamHandle` returned by `to_spark_streaming` manages the background producer and bridge threads:

```python
streaming_df, handle = to_spark_streaming(source, spark)
# ... start a Spark streaming query on streaming_df ...

# Check for producer errors
if handle.producer_error is not None:
    print(f"Producer failed: {handle.producer_error}")

# Clean shutdown (stops producer, bridge thread, kills coordinator)
handle.stop(drain_timeout=10.0)
```

## Monitoring

### Console monitoring

Use `print_stats` for quick debugging:

```python
from raydp.streaming import print_stats

# One-shot stats check
print_stats(coordinator, interval=1.0, max_iterations=1)

# Continuous monitoring
print_stats(coordinator, interval=2.0)  # runs until stream completes
```

### Programmatic stats

Access stats directly from the coordinator:

```python
stats = ray.get(coordinator.get_stats.remote())
# Returns:
# {
#     "stream_id": "my_stream",
#     "batches_published": 42,
#     "batches_gc": 35,
#     "buffer_size": 7,
#     "max_buffered": 64,
#     "buffered_bytes": 3145728,
#     "max_buffered_bytes": 2147483648,
#     "num_consumers": 2,
#     "complete": False,
#     "error": None,
#     "latest_watermark": "2024-01-15T10:30:00.000Z",
# }
```

### Grafana dashboard

A pre-built Grafana dashboard is available at `examples/grafana/raydp-streaming-dashboard.json`. Import it into your Grafana instance to visualize throughput, buffer utilization, consumer lag, and backpressure events.

## Architecture

The streaming bridge consists of four core components:

```
┌──────────────┐     foreachBatch      ┌───────────────────┐     pull_batch     ┌───────────────────┐
│    Spark      │ ──────────────────► │  StreamCoordinator  │ ◄──────────────── │ StreamingIterator   │
│  Structured   │   publish_batch     │   (Ray actor)       │    register/      │   (consumer)        │
│  Streaming    │                     │                     │    deregister     │                     │
└──────────────┘                      └───────────────────┘                    └───────────────────┘
                                             │
                                             │  ObjectRefs
                                             ▼
                                      ┌──────────────┐
                                      │  Ray Object   │
                                      │    Store      │
                                      └──────────────┘
```

- **Sink** (`SparkStreamingSink` / `JvmStreamingSink`) — Spark `foreachBatch` callback that converts each micro-batch DataFrame to Arrow, puts it in the Ray object store, and publishes refs to the coordinator.
- **Coordinator** (`StreamCoordinator`) — async Ray actor that manages a bounded buffer of micro-batches, handles multi-consumer fan-out, backpressure, GC, schema validation, and watermark tracking.
- **Consumer** (`StreamingIterator`) — pulls batches from the coordinator via `pull_batch`, resolves ObjectRefs to `pa.Table`.
- **Source** (`RayStreamingSource`) — PySpark 4.1 Python Data Source API implementation for the reverse bridge.
