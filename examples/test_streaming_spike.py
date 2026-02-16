"""
Hybrid Streaming: Direct Coordinator Pull + On-Demand Ray Datasets.

Part A: Real-time streaming via StreamingIterator.__iter__()
  - Spark rate source -> foreachBatch(sink) -> coordinator -> StreamingIterator
  - Validates that data arrives while the streaming query is still active

Part B: Windowed Ray Datasets via StreamingIterator.iter_datasets()
  - Manually push Arrow tables -> iter_datasets(window_size=3)
  - Validates correct windowing into Ray Datasets
"""
import os
import sys
import threading
import time
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="ray")

import pyarrow as pa
import ray
import raydp
from raydp.streaming import (
    MicroBatchCoordinator,
    StreamingIterator,
    create_streaming_sink,
)

os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"

JDK17_JAVA_OPTS = " ".join([
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.math=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
])

MIN_CONSUMER_TABLES = 5
TIMEOUT_SECONDS = 30
MAX_BUFFERED = 2

# ============================================================
# Part A: Real-Time Streaming
# ============================================================
print("=" * 60)
print("PART A: Real-Time Streaming (StreamingIterator.__iter__)")
print("=" * 60)

# ---------- 1. Init Ray + Spark ----------
print("\n=== Initializing Ray ===")
ray.init(include_dashboard=False)
node_ip = ray.util.get_node_ip_address()

print("=== Initializing Spark ===")
spark = raydp.init_spark(
    app_name="Streaming Hybrid",
    num_executors=1,
    executor_cores=1,
    executor_memory="500M",
    configs={
        "spark.driver.host": node_ip,
        "spark.driver.bindAddress": node_ip,
        "spark.network.timeout": "120s",
        "spark.executor.heartbeatInterval": "20s",
        "spark.ui.enabled": "false",
        "spark.executor.extraJavaOptions": JDK17_JAVA_OPTS,
        "spark.driver.extraJavaOptions": JDK17_JAVA_OPTS,
        "spark.ray.raydp_app_master.extraJavaOptions": JDK17_JAVA_OPTS,
    },
)
print(f"Spark {spark.version} ready")

# Wait for executors to register
print("Waiting 10s for executors to register...")
time.sleep(10)

# ---------- 2. Create coordinator ----------
print("\n=== Creating MicroBatchCoordinator ===")
coordinator = MicroBatchCoordinator.remote(max_buffered_batches=MAX_BUFFERED)

# ---------- 3. Start consumer thread ----------
consumed_tables = []
consumed_timestamps = []
consumer_error = None


def consumer_thread_fn():
    global consumer_error
    try:
        it = StreamingIterator(coordinator)
        for table in it:
            consumed_tables.append(table)
            consumed_timestamps.append(time.time())
            print(f"  [consumer] got table {len(consumed_tables)}: "
                  f"{table.num_rows} rows")
    except Exception as e:
        consumer_error = e


print("\n=== Starting consumer thread ===")
consumer = threading.Thread(target=consumer_thread_fn, daemon=True)
consumer.start()

# ---------- 4. Start Spark streaming ----------
print("=== Starting Spark Structured Streaming (rate source) ===")
rate_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", "10")
    .load()
)

sink_fn = create_streaming_sink(coordinator)

query = (
    rate_stream.writeStream
    .foreachBatch(sink_fn)
    .trigger(processingTime="2 seconds")
    .start()
)
print(f"Streaming query started: {query.name}")

# ---------- 5. Wait for consumer to collect 5+ tables while streaming ----------
print(f"\n=== Waiting for consumer to receive {MIN_CONSUMER_TABLES}+ "
      f"tables (timeout {TIMEOUT_SECONDS}s) ===")
deadline = time.time() + TIMEOUT_SECONDS
while len(consumed_tables) < MIN_CONSUMER_TABLES and time.time() < deadline:
    time.sleep(1)

tables_while_active = len(consumed_tables)
print(f"Consumer received {tables_while_active} tables while streaming was active")

# ---------- 6. Mid-stream stats ----------
stats_midstream = ray.get(coordinator.get_stats.remote())
print(f"Mid-stream coordinator stats: {stats_midstream}")

# ---------- 7. Clean shutdown ----------
print("\n=== Shutting down ===")
query.stop()
print("Spark streaming stopped")

ray.get(coordinator.mark_complete.remote())
print("Coordinator marked complete")

consumer.join(timeout=30)
consumer_joined = not consumer.is_alive()
print(f"Consumer thread joined: {consumer_joined}")

total_tables = len(consumed_tables)
total_rows = sum(t.num_rows for t in consumed_tables) if consumed_tables else 0
stats_final = ray.get(coordinator.get_stats.remote())
print(f"Final: {total_tables} tables, {total_rows} rows")
print(f"Final coordinator stats: {stats_final}")


# ============================================================
# Part B: Windowed Datasets
# ============================================================
print("\n" + "=" * 60)
print("PART B: Windowed Datasets (StreamingIterator.iter_datasets)")
print("=" * 60)

# ---------- 1. Fresh coordinator (no Spark needed) ----------
coord_b = MicroBatchCoordinator.remote(max_buffered_batches=10)

# ---------- 2. Push 6 Arrow tables manually ----------
for i in range(6):
    table = pa.table({
        "id": pa.array(range(i * 10, i * 10 + 10), type=pa.int64()),
        "value": pa.array([float(i)] * 10, type=pa.float64()),
    })
    ray.get(coord_b.put_batch.remote(table))
ray.get(coord_b.mark_complete.remote())
print("Pushed 6 tables (10 rows each) and marked complete")

# ---------- 3. Iterate windowed datasets ----------
it_b = StreamingIterator(coord_b)
datasets = list(it_b.iter_datasets(window_size=3))
print(f"Collected {len(datasets)} windowed dataset(s)")

for idx, ds in enumerate(datasets):
    count = ds.count()
    cols = ds.schema().names
    print(f"  Dataset {idx}: {count} rows, columns={cols}")


# ============================================================
# Validations
# ============================================================
print("\n" + "=" * 60)
print("VALIDATIONS")
print("=" * 60)

results = []


def check(num, desc, condition):
    status = "PASS" if condition else "FAIL"
    results.append((num, desc, condition))
    print(f"  [{status}] #{num}: {desc}")


# 1: Real-time delivery — consumer got tables while streaming was still active
check(1, f"Real-time delivery ({tables_while_active} tables received mid-stream)",
      tables_while_active >= MIN_CONSUMER_TABLES)

# 2: Data integrity — all tables have the expected schema columns
expected_cols = {"timestamp", "value"}
all_have_schema = all(
    set(t.column_names) >= expected_cols for t in consumed_tables
) if consumed_tables else False
check(2, "Data integrity (all tables have expected schema columns)",
      all_have_schema)

# 3: Coordinator bridges foreachBatch -> consumer
check(3, f"Coordinator bridged batches (got={stats_final['batches_got']})",
      stats_final["batches_got"] >= MIN_CONSUMER_TABLES)

# 4: Backpressure — peak buffer never exceeded max_buffered_batches
check(4, f"Backpressure (peak_buffer={stats_final['peak_buffer']} <= max={MAX_BUFFERED})",
      stats_final["peak_buffer"] <= MAX_BUFFERED)

# 5: Clean shutdown — consumer thread joined without hanging
check(5, "Clean shutdown (consumer thread joined)", consumer_joined)

# 6: Windowed datasets — 2 datasets produced, each with 30 rows
windowed_ok = (
    len(datasets) == 2
    and all(ds.count() == 30 for ds in datasets)
)
check(6, f"Windowed datasets ({len(datasets)} datasets, "
      f"rows={[ds.count() for ds in datasets]})",
      windowed_ok)

# ---------- Summary ----------
all_passed = all(ok for _, _, ok in results)
print()
if all_passed:
    print("ALL VALIDATIONS PASSED")
else:
    failed = [f"#{n}" for n, _, ok in results if not ok]
    print(f"FAILED: {', '.join(failed)}")

# Cleanup
raydp.stop_spark()
ray.shutdown()

sys.exit(0 if all_passed else 1)
