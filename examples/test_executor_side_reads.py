"""Executor-side reverse reads via StreamingIterator.iter_spark_dataframes.

Push Arrow batches to a StreamCoordinator, then read them into Spark DataFrames
whose partitions are resolved BY THE EXECUTORS (ray_dataset_to_spark_dataframe) —
the batch bytes never funnel through the driver. Validates data correctness,
windowing, and executor-side partitioning. Requires a RayDP cluster.
"""

import os
import sys
import time
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="ray")

import pyarrow as pa
import ray
import raydp
from raydp.streaming import StreamCoordinator, StreamingIterator

os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"

ray.init(include_dashboard=False)
node_ip = ray.util.get_node_ip_address()
spark = raydp.init_spark(
    app_name="executor-side-reads",
    num_executors=2,
    executor_cores=1,
    executor_memory="600M",
    configs={
        "spark.driver.host": node_ip,
        "spark.driver.bindAddress": node_ip,
        "spark.network.timeout": "120s",
        "spark.executor.heartbeatInterval": "20s",
        "spark.ui.enabled": "false",
    },
)
print(f"Spark {spark.version} ready; waiting 10s for executors...")
time.sleep(10)

N, ROWS, WINDOW = 6, 5, 2  # 6 batches, 2 per window -> 3 windows

coord = StreamCoordinator.options(name="exec_reads_coord").remote(
    stream_id="exec_reads", max_buffered_batches=64
)
expected = []
for i in range(N):
    lo = i * ROWS
    table = pa.table(
        {
            "id": pa.array(range(lo, lo + ROWS), type=pa.int64()),
            "val": pa.array([f"v{i}"] * ROWS),
        }
    )
    expected += [(r, f"v{i}") for r in range(lo, lo + ROWS)]
    ray.get(coord.publish_batch.remote([ray.put(table)], table.schema.serialize().to_pybytes(), table.num_rows))
ray.get(coord.signal_complete.remote())
print(f"Pushed {N} batches ({N * ROWS} rows) to the coordinator")

it = StreamingIterator(coord)
all_rows = []
part_counts = []
for sdf in it.iter_spark_dataframes(spark, window_size=WINDOW):
    part_counts.append(sdf.rdd.getNumPartitions())
    all_rows += [(r.id, r.val) for r in sdf.collect()]

got = sorted(all_rows)
n_windows = len(part_counts)
ok_data = got == sorted(expected)
ok_windows = n_windows == N // WINDOW
ok_parallel = all(p >= 2 for p in part_counts)
print(f"windows={n_windows} part_counts={part_counts} rows={len(got)}")
print(f"data correct: {ok_data} | windows==3: {ok_windows} | executor-parallel (>=2/window): {ok_parallel}")

try:
    raydp.stop_spark()
except Exception:
    pass
ray.shutdown()

ok = ok_data and ok_windows and ok_parallel
print("EXECUTOR-SIDE READS (iter_spark_dataframes):", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
