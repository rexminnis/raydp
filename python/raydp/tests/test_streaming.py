"""Tests for the streaming bridge: coordinator, consumer, and end-to-end."""

import threading
import time

import pyarrow as pa
import pytest
import ray

from raydp.streaming.coordinator import StreamCoordinator
from raydp.streaming.consumer import StreamingIterator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(n_rows: int, batch_idx: int = 0) -> pa.Table:
    return pa.table({
        "id": pa.array(range(batch_idx * n_rows, (batch_idx + 1) * n_rows), type=pa.int64()),
        "value": pa.array([float(batch_idx)] * n_rows, type=pa.float64()),
    })


def _schema_bytes(table: pa.Table) -> bytes:
    return table.schema.serialize().to_pybytes()


def _publish_table(coordinator, table: pa.Table):
    """Put table in object store and publish ref to coordinator."""
    ref = ray.put(table)
    ray.get(coordinator.publish_batch.remote([ref], _schema_bytes(table), table.num_rows))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ray_env():
    ray.init(num_cpus=2, include_dashboard=False)
    yield
    ray.shutdown()


# ---------------------------------------------------------------------------
# Unit tests — coordinator
# ---------------------------------------------------------------------------

class TestCoordinatorSingleConsumer:

    def test_publish_and_pull(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_single", max_buffered_batches=10)
        tables = [_make_table(5, i) for i in range(5)]

        for t in tables:
            _publish_table(coord, t)
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))
        pulled = []
        while True:
            result = ray.get(coord.pull_batch.remote("c1"))
            if result is None:
                break
            resolved = ray.get(result["partition_refs"])
            pulled.extend(resolved)
        ray.get(coord.deregister_consumer.remote("c1"))

        assert len(pulled) == 5
        for i, t in enumerate(pulled):
            assert t.equals(tables[i])


class TestCoordinatorMultiConsumer:

    def test_two_consumers_see_all_batches(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_multi", max_buffered_batches=10)
        tables = [_make_table(3, i) for i in range(4)]

        for t in tables:
            _publish_table(coord, t)
        ray.get(coord.signal_complete.remote())

        for cid in ("c1", "c2"):
            ray.get(coord.register_consumer.remote(cid))

        for cid in ("c1", "c2"):
            pulled = []
            while True:
                result = ray.get(coord.pull_batch.remote(cid))
                if result is None:
                    break
                resolved = ray.get(result["partition_refs"])
                pulled.extend(resolved)
            assert len(pulled) == 4
            for i, t in enumerate(pulled):
                assert t.equals(tables[i])
            ray.get(coord.deregister_consumer.remote(cid))


class TestCoordinatorBackpressure:

    def test_buffer_full_blocks_producer(self, ray_env):
        max_buf = 2
        coord = StreamCoordinator.remote(stream_id="test_bp", max_buffered_batches=max_buf)
        ray.get(coord.register_consumer.remote("c1"))

        # Fill the buffer
        for i in range(max_buf):
            _publish_table(coord, _make_table(2, i))

        # Next publish should block — use a thread to attempt it
        published = threading.Event()

        def producer():
            _publish_table(coord, _make_table(2, max_buf))
            published.set()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        # Give producer a moment — it should NOT finish
        time.sleep(0.5)
        assert not published.is_set(), "Producer should be blocked by backpressure"

        # Pull one batch to free space
        ray.get(coord.pull_batch.remote("c1"))
        t.join(timeout=5)
        assert published.is_set(), "Producer should unblock after consumer pulls"
        ray.get(coord.deregister_consumer.remote("c1"))


class TestCoordinatorGC:

    def test_batches_freed_after_all_consumers_advance(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_gc", max_buffered_batches=10)

        for i in range(5):
            _publish_table(coord, _make_table(2, i))

        ray.get(coord.register_consumer.remote("c1"))
        ray.get(coord.register_consumer.remote("c2"))

        # c1 reads 3 batches
        for _ in range(3):
            ray.get(coord.pull_batch.remote("c1"))

        stats = ray.get(coord.get_stats.remote())
        assert stats["batches_gc"] == 0, "No GC yet — c2 hasn't advanced"

        # c2 reads 3 batches
        for _ in range(3):
            ray.get(coord.pull_batch.remote("c2"))

        stats = ray.get(coord.get_stats.remote())
        assert stats["batches_gc"] == 3, "3 batches should be GC'd"
        assert stats["buffer_size"] == 2

        ray.get(coord.deregister_consumer.remote("c1"))
        ray.get(coord.deregister_consumer.remote("c2"))


class TestCoordinatorErrorSignaling:

    def test_error_propagates_to_consumer(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_err", max_buffered_batches=10)
        ray.get(coord.register_consumer.remote("c1"))

        ray.get(coord.signal_error.remote("something broke"))

        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(coord.pull_batch.remote("c1"))
        assert "something broke" in str(exc_info.value)

        ray.get(coord.deregister_consumer.remote("c1"))


class TestCoordinatorStats:

    def test_stats_after_operations(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_stats", max_buffered_batches=10)

        _publish_table(coord, _make_table(5, 0))
        _publish_table(coord, _make_table(5, 1))

        stats = ray.get(coord.get_stats.remote())
        assert stats["stream_id"] == "test_stats"
        assert stats["batches_published"] == 2
        assert stats["buffer_size"] == 2
        assert stats["num_consumers"] == 0
        assert stats["complete"] is False
        assert stats["error"] is None


# ---------------------------------------------------------------------------
# Unit tests — StreamingIterator
# ---------------------------------------------------------------------------

class TestStreamingIteratorBasic:

    def test_iterate_tables(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_iter", max_buffered_batches=10)
        tables = [_make_table(5, i) for i in range(4)]

        for t in tables:
            _publish_table(coord, t)
        ray.get(coord.signal_complete.remote())

        it = StreamingIterator(coord)
        collected = list(it)

        assert len(collected) == 4
        for i, t in enumerate(collected):
            assert t.equals(tables[i])


class TestStreamingIteratorDatasets:

    def test_windowed_datasets(self, ray_env):
        coord = StreamCoordinator.remote(stream_id="test_ds", max_buffered_batches=10)

        for i in range(6):
            _publish_table(coord, _make_table(10, i))
        ray.get(coord.signal_complete.remote())

        it = StreamingIterator(coord)
        datasets = list(it.iter_datasets(window_size=3))

        assert len(datasets) == 2
        for ds in datasets:
            assert ds.count() == 30  # 3 batches × 10 rows


# ---------------------------------------------------------------------------
# Integration test — end-to-end with Spark
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("spark_on_ray_small", ["local"], indirect=True)
def test_streaming_end_to_end(spark_on_ray_small):
    """Spark rate source → from_spark_streaming() → consume tables → verify."""
    from raydp.streaming import from_spark_streaming

    spark = spark_on_ray_small

    # Wait for executors
    time.sleep(10)

    rate_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()
    )

    iterator, query = from_spark_streaming(
        rate_stream,
        stream_id="e2e_test",
        max_buffered_batches=4,
        trigger={"processingTime": "2 seconds"},
    )

    # Consume in a thread
    consumed = []
    consumer_error = None

    def consumer_fn():
        nonlocal consumer_error
        try:
            for table in iterator:
                consumed.append(table)
                if len(consumed) >= 5:
                    return
        except Exception as e:
            consumer_error = e

    t = threading.Thread(target=consumer_fn, daemon=True)
    t.start()

    # Wait for at least 5 tables
    deadline = time.time() + 60
    while len(consumed) < 5 and time.time() < deadline:
        time.sleep(1)

    query.stop()
    t.join(timeout=10)

    assert consumer_error is None, f"Consumer error: {consumer_error}"
    assert len(consumed) >= 5, f"Expected 5+ tables, got {len(consumed)}"

    # Verify data shape
    for table in consumed:
        assert "timestamp" in table.column_names
        assert "value" in table.column_names
        assert table.num_rows > 0

    # Verify backpressure — coordinator buffer never exceeded max
    coord = ray.get_actor(f"stream_coord_e2e_test")
    stats = ray.get(coord.get_stats.remote())
    assert stats["buffer_size"] <= 4


@pytest.mark.parametrize("spark_on_ray_small", ["local"], indirect=True)
def test_streaming_iter_datasets_end_to_end(spark_on_ray_small):
    """Spark rate source → from_spark_streaming() → iter_datasets → verify windowing."""
    from raydp.streaming import from_spark_streaming

    spark = spark_on_ray_small

    # Wait for executors
    time.sleep(10)

    rate_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()
    )

    iterator, query = from_spark_streaming(
        rate_stream,
        stream_id="e2e_ds_test",
        max_buffered_batches=8,
        trigger={"processingTime": "2 seconds"},
    )

    # Consume windowed datasets in a thread
    collected_datasets = []
    consumer_error = None
    window_size = 3

    def consumer_fn():
        nonlocal consumer_error
        try:
            for ds in iterator.iter_datasets(window_size=window_size):
                collected_datasets.append(ds)
                if len(collected_datasets) >= 2:
                    return
        except Exception as e:
            consumer_error = e

    t = threading.Thread(target=consumer_fn, daemon=True)
    t.start()

    # Wait for at least 2 windowed datasets
    deadline = time.time() + 60
    while len(collected_datasets) < 2 and time.time() < deadline:
        time.sleep(1)

    query.stop()
    t.join(timeout=10)

    assert consumer_error is None, f"Consumer error: {consumer_error}"
    assert len(collected_datasets) >= 2, (
        f"Expected 2+ datasets, got {len(collected_datasets)}"
    )

    # Each dataset should be a real ray.data.Dataset with rows from multiple batches
    for ds in collected_datasets:
        count = ds.count()
        assert count > 0, "Dataset should have rows"
        schema_names = ds.schema().names
        assert "timestamp" in schema_names
        assert "value" in schema_names
