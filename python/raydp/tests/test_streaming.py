"""Tests for the streaming bridge: coordinator, consumer, and end-to-end."""

import threading
import time
from io import StringIO

import pyarrow as pa
import pytest
import ray

from raydp.streaming.coordinator import StreamCoordinator
from raydp.streaming.consumer import StreamingIterator
from raydp.streaming.sink import SparkStreamingSink
from raydp.streaming.monitor import print_stats
from raydp.streaming import create_partitioned_iterators


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
    ray.get(coordinator.publish_batch.remote(
        [ref], _schema_bytes(table), table.num_rows, num_bytes=table.nbytes,
    ))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ray_env():
    ray.init(num_cpus=2, include_dashboard=False)
    yield
    ray.shutdown()


# ---------------------------------------------------------------------------
# Unit tests — coordinator (Phase 1 originals)
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

        # Publish one batch first so register_consumer has schema available
        _publish_table(coord, _make_table(2, 0))
        ray.get(coord.register_consumer.remote("c1"))

        # Fill the rest of the buffer
        _publish_table(coord, _make_table(2, 1))

        # Buffer is now full (2 items). Next publish should block.
        published = threading.Event()

        def producer():
            _publish_table(coord, _make_table(2, 2))
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

        # Publish one batch so register_consumer has schema available
        _publish_table(coord, _make_table(3, 0))
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
# Unit tests — StreamingIterator (Phase 1 originals)
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
# Phase 2 unit tests — blocking register, resume, timeout, watermark, drain
# ---------------------------------------------------------------------------

class TestRegisterBlocksUntilFirstBatch:

    def test_register_blocks_until_first_batch(self, ray_env):
        """register_consumer should block until the first batch is published."""
        coord = StreamCoordinator.remote(stream_id="test_block_reg", max_buffered_batches=10)

        registered = threading.Event()
        schema_result = [None]

        def register_fn():
            schema_result[0] = ray.get(coord.register_consumer.remote("c1"))
            registered.set()

        t = threading.Thread(target=register_fn, daemon=True)
        t.start()

        # Should not be registered yet (no batches published)
        time.sleep(0.5)
        assert not registered.is_set(), "register_consumer should block before first batch"

        # Publish a batch — should unblock
        _publish_table(coord, _make_table(5, 0))
        t.join(timeout=5)
        assert registered.is_set(), "register_consumer should unblock after first batch"
        assert schema_result[0] is not None, "Schema bytes should be returned"

        ray.get(coord.deregister_consumer.remote("c1"))


class TestResumeFromBatchId:

    def test_resume_from_batch_id(self, ray_env):
        """Consumer with start_batch_id should skip earlier batches."""
        coord = StreamCoordinator.remote(stream_id="test_resume", max_buffered_batches=10)

        # Publish 5 batches (ids 0-4)
        for i in range(5):
            _publish_table(coord, _make_table(3, i))
        ray.get(coord.signal_complete.remote())

        # Register starting at batch 3 — should see batches 3 and 4
        ray.get(coord.register_consumer.remote("c1", start_batch_id=3))
        pulled = []
        while True:
            result = ray.get(coord.pull_batch.remote("c1"))
            if result is None:
                break
            pulled.append(result["batch_id"])
        ray.get(coord.deregister_consumer.remote("c1"))

        assert pulled == [3, 4]


class TestResumeFromGcBatchRaises:

    def test_resume_from_gc_batch_raises(self, ray_env):
        """Resuming from a GC'd batch_id should raise ValueError."""
        coord = StreamCoordinator.remote(stream_id="test_gc_resume", max_buffered_batches=10)

        # Publish 5 batches
        for i in range(5):
            _publish_table(coord, _make_table(3, i))

        # Register c1 and consume all 5 → GC will run
        ray.get(coord.register_consumer.remote("c1"))
        for _ in range(5):
            ray.get(coord.pull_batch.remote("c1"))

        # Batches 0-4 are GC'd. Trying to register at batch 0 should fail.
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(coord.register_consumer.remote("c2", start_batch_id=0))
        assert "GC'd" in str(exc_info.value)

        ray.get(coord.deregister_consumer.remote("c1"))


class TestPullReturnsNoneOnTimeout:

    def test_pull_returns_none_on_timeout(self, ray_env):
        """pull_batch with timeout should return None when no batch arrives."""
        coord = StreamCoordinator.remote(stream_id="test_timeout", max_buffered_batches=10)

        # Publish one batch so schema is available, then register
        _publish_table(coord, _make_table(3, 0))
        ray.get(coord.register_consumer.remote("c1"))
        # Consume the available batch
        ray.get(coord.pull_batch.remote("c1"))

        # Now no batches available — pull with short timeout
        start = time.time()
        result = ray.get(coord.pull_batch.remote("c1", timeout=0.5))
        elapsed = time.time() - start

        assert result is None
        assert 0.3 < elapsed < 2.0, f"Expected ~0.5s wait, got {elapsed:.2f}s"

        ray.get(coord.deregister_consumer.remote("c1"))


class TestPullNoTimeoutWaits:

    def test_pull_no_timeout_waits(self, ray_env):
        """pull_batch without timeout should wait for batch to arrive."""
        coord = StreamCoordinator.remote(stream_id="test_no_timeout", max_buffered_batches=10)

        # Publish one batch so register_consumer doesn't block
        _publish_table(coord, _make_table(3, 0))
        ray.get(coord.register_consumer.remote("c1"))
        # Consume it
        ray.get(coord.pull_batch.remote("c1"))

        # Pull with no timeout — should block until we publish
        result_holder = [None]

        def puller():
            result_holder[0] = ray.get(coord.pull_batch.remote("c1"))

        t = threading.Thread(target=puller, daemon=True)
        t.start()

        time.sleep(0.5)
        assert t.is_alive(), "pull_batch should still be waiting"

        # Publish another batch — should unblock
        _publish_table(coord, _make_table(3, 1))
        t.join(timeout=5)
        assert not t.is_alive()
        assert result_holder[0] is not None
        assert result_holder[0]["batch_id"] == 1

        ray.get(coord.deregister_consumer.remote("c1"))


class TestWatermark:

    def test_watermark_stored_in_batch(self, ray_env):
        """Watermark passed to publish_batch should appear in pull response and stats."""
        coord = StreamCoordinator.remote(stream_id="test_wm", max_buffered_batches=10)

        table = _make_table(3, 0)
        ref = ray.put(table)
        ray.get(coord.publish_batch.remote(
            [ref], _schema_bytes(table), table.num_rows,
            watermark="2024-01-15T10:30:00.000Z",
        ))
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))
        result = ray.get(coord.pull_batch.remote("c1"))

        assert result["watermark"] == "2024-01-15T10:30:00.000Z"

        stats = ray.get(coord.get_stats.remote())
        assert stats["latest_watermark"] == "2024-01-15T10:30:00.000Z"

        wm = ray.get(coord.get_watermark.remote())
        assert wm == "2024-01-15T10:30:00.000Z"

        ray.get(coord.deregister_consumer.remote("c1"))

    def test_watermark_none_when_not_provided(self, ray_env):
        """Watermark should be None when not passed to publish_batch."""
        coord = StreamCoordinator.remote(stream_id="test_wm_none", max_buffered_batches=10)

        _publish_table(coord, _make_table(3, 0))
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))
        result = ray.get(coord.pull_batch.remote("c1"))

        assert result["watermark"] is None

        stats = ray.get(coord.get_stats.remote())
        assert stats["latest_watermark"] is None

        ray.get(coord.deregister_consumer.remote("c1"))


class TestDrain:

    def test_drain_waits_for_consumer(self, ray_env):
        """wait_for_drain should wait until consumer catches up."""
        coord = StreamCoordinator.remote(stream_id="test_drain", max_buffered_batches=10)

        _publish_table(coord, _make_table(3, 0))
        _publish_table(coord, _make_table(3, 1))
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))

        # Start drain in background — won't complete until c1 consumes all
        drain_ref = coord.wait_for_drain.remote(timeout=10.0)

        # Consume both batches
        ray.get(coord.pull_batch.remote("c1"))
        ray.get(coord.pull_batch.remote("c1"))
        # Pull once more to get None (stream complete)
        ray.get(coord.pull_batch.remote("c1"))

        # Drain check: c1 cursor should be at _next_batch_id (2)
        # But c1 is still registered, so wait_for_drain checks cursor >= next_batch_id
        drained = ray.get(drain_ref, timeout=5)
        assert drained is True

        ray.get(coord.deregister_consumer.remote("c1"))

    def test_drain_timeout(self, ray_env):
        """wait_for_drain should return False when consumer hasn't caught up."""
        coord = StreamCoordinator.remote(stream_id="test_drain_to", max_buffered_batches=10)

        _publish_table(coord, _make_table(3, 0))
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))

        # Don't consume — drain should timeout
        drained = ray.get(coord.wait_for_drain.remote(timeout=0.5))
        assert drained is False

        ray.get(coord.deregister_consumer.remote("c1"))


class TestSinkDiscovery:

    def test_sink_discovers_existing_coordinator(self, ray_env):
        """Second SparkStreamingSink with same stream_id should find existing coordinator."""
        stream_id = "test_discovery"

        # Create first sink — creates detached coordinator
        sink1 = SparkStreamingSink(stream_id=stream_id, max_buffered_batches=10)
        coord1 = sink1.coordinator

        # Create second sink — should discover existing coordinator
        sink2 = SparkStreamingSink(stream_id=stream_id, max_buffered_batches=10)
        coord2 = sink2.coordinator

        # Both should point to the same actor
        # Publish via sink1, verify via sink2's coordinator
        table = _make_table(3, 0)
        ref = ray.put(table)
        ray.get(coord1.publish_batch.remote(
            [ref], _schema_bytes(table), table.num_rows
        ))

        stats = ray.get(coord2.get_stats.remote())
        assert stats["batches_published"] == 1

        # Cleanup: kill detached actor
        try:
            ray.kill(coord1)
        except Exception:
            pass


class TestIteratorLastBatchId:

    def test_iterator_last_batch_id(self, ray_env):
        """StreamingIterator.last_batch_id should track the most recently consumed batch."""
        coord = StreamCoordinator.remote(stream_id="test_last_bid", max_buffered_batches=10)

        for i in range(4):
            _publish_table(coord, _make_table(3, i))
        ray.get(coord.signal_complete.remote())

        it = StreamingIterator(coord)
        assert it.last_batch_id is None

        batch_ids_seen = []
        for table in it:
            batch_ids_seen.append(it.last_batch_id)

        assert batch_ids_seen == [0, 1, 2, 3]
        assert it.last_batch_id == 3


class TestMetricsSmoke:

    def test_metrics_smoke(self, ray_env):
        """Verify that metrics don't cause errors during a publish/consume cycle."""
        coord = StreamCoordinator.remote(stream_id="test_metrics", max_buffered_batches=10)

        for i in range(3):
            _publish_table(coord, _make_table(5, i))
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))
        while True:
            result = ray.get(coord.pull_batch.remote("c1"))
            if result is None:
                break
        ray.get(coord.deregister_consumer.remote("c1"))

        stats = ray.get(coord.get_stats.remote())
        assert stats["batches_published"] == 3
        assert stats["complete"] is True


class TestSchemaMismatchRaises:

    def test_schema_mismatch_raises(self, ray_env):
        """Publishing a batch with a different schema should raise ValueError."""
        coord = StreamCoordinator.remote(stream_id="test_schema_mismatch", max_buffered_batches=10)

        # First batch: (id: int64, value: float64)
        _publish_table(coord, _make_table(3, 0))

        # Second batch: different schema (name: string, count: int64)
        bad_table = pa.table({
            "name": pa.array(["a", "b"], type=pa.string()),
            "count": pa.array([1, 2], type=pa.int64()),
        })
        ref = ray.put(bad_table)
        with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
            ray.get(coord.publish_batch.remote(
                [ref], _schema_bytes(bad_table), bad_table.num_rows
            ))
        assert "Schema mismatch" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Phase 3 unit tests — partitioned pull, monitor
# ---------------------------------------------------------------------------

def _publish_multi_ref(coordinator, tables):
    """Publish multiple Arrow table refs as a single batch (simulating partitioned mode)."""
    refs = [ray.put(t) for t in tables]
    schema_bytes = _schema_bytes(tables[0])
    total_rows = sum(t.num_rows for t in tables)
    total_bytes = sum(t.nbytes for t in tables)
    ray.get(coordinator.publish_batch.remote(
        refs, schema_bytes, total_rows, num_bytes=total_bytes,
    ))


class TestPartitionedPullReturnsSubset:

    def test_partitioned_pull_returns_subset(self, ray_env):
        """Publish batch with 4 refs, register consumer with partition_ids=[0,2],
        verify only refs 0 and 2 returned."""
        coord = StreamCoordinator.remote(stream_id="test_part_sub", max_buffered_batches=10)
        tables = [_make_table(3, i) for i in range(4)]
        _publish_multi_ref(coord, tables)
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1", partition_ids=[0, 2]))
        result = ray.get(coord.pull_batch.remote("c1"))
        assert result is not None
        resolved = ray.get(result["partition_refs"])
        assert len(resolved) == 2
        assert resolved[0].equals(tables[0])
        assert resolved[1].equals(tables[2])
        ray.get(coord.deregister_consumer.remote("c1"))


class TestPartitionedTwoConsumersDisjoint:

    def test_partitioned_two_consumers_disjoint(self, ray_env):
        """Two consumers with disjoint partition_ids each get only their assigned refs."""
        coord = StreamCoordinator.remote(stream_id="test_part_disj", max_buffered_batches=10)
        tables = [_make_table(3, i) for i in range(4)]
        _publish_multi_ref(coord, tables)
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1", partition_ids=[0, 1]))
        ray.get(coord.register_consumer.remote("c2", partition_ids=[2, 3]))

        r1 = ray.get(coord.pull_batch.remote("c1"))
        r2 = ray.get(coord.pull_batch.remote("c2"))

        resolved1 = ray.get(r1["partition_refs"])
        resolved2 = ray.get(r2["partition_refs"])

        assert len(resolved1) == 2
        assert resolved1[0].equals(tables[0])
        assert resolved1[1].equals(tables[1])

        assert len(resolved2) == 2
        assert resolved2[0].equals(tables[2])
        assert resolved2[1].equals(tables[3])

        ray.get(coord.deregister_consumer.remote("c1"))
        ray.get(coord.deregister_consumer.remote("c2"))


class TestPartitionedIgnoresOutOfRange:

    def test_partitioned_pull_ignores_out_of_range(self, ray_env):
        """partition_ids=[5] with only 3 refs returns empty list (no crash)."""
        coord = StreamCoordinator.remote(stream_id="test_part_oor", max_buffered_batches=10)
        tables = [_make_table(3, i) for i in range(3)]
        _publish_multi_ref(coord, tables)
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1", partition_ids=[5]))
        result = ray.get(coord.pull_batch.remote("c1"))
        assert result is not None
        assert len(result["partition_refs"]) == 0
        ray.get(coord.deregister_consumer.remote("c1"))


class TestFanOutDefaultUnchanged:

    def test_fan_out_default_unchanged(self, ray_env):
        """Register without partition_ids, verify all refs returned (backward compat)."""
        coord = StreamCoordinator.remote(stream_id="test_fanout", max_buffered_batches=10)
        tables = [_make_table(3, i) for i in range(4)]
        _publish_multi_ref(coord, tables)
        ray.get(coord.signal_complete.remote())

        ray.get(coord.register_consumer.remote("c1"))
        result = ray.get(coord.pull_batch.remote("c1"))
        assert result is not None
        resolved = ray.get(result["partition_refs"])
        assert len(resolved) == 4
        for i, t in enumerate(resolved):
            assert t.equals(tables[i])
        assert result["num_rows"] == sum(t.num_rows for t in tables)
        ray.get(coord.deregister_consumer.remote("c1"))


class TestCreatePartitionedIteratorsFactory:

    def test_create_partitioned_iterators_factory(self, ray_env):
        """Verify round-robin partition assignment."""
        coord = StreamCoordinator.remote(stream_id="test_factory", max_buffered_batches=10)

        iterators = create_partitioned_iterators(coord, num_consumers=3, num_partitions=7)
        assert len(iterators) == 3
        # Partitions: 0,3,6 | 1,4 | 2,5
        assert iterators[0]._partition_ids == [0, 3, 6]
        assert iterators[1]._partition_ids == [1, 4]
        assert iterators[2]._partition_ids == [2, 5]


class TestPrintStatsSmoke:

    def test_print_stats_smoke(self, ray_env, capsys):
        """Verify print_stats runs one iteration without error."""
        coord = StreamCoordinator.remote(stream_id="test_monitor", max_buffered_batches=10)
        _publish_table(coord, _make_table(5, 0))
        ray.get(coord.signal_complete.remote())

        print_stats(coord, interval=0.1, max_iterations=1)

        captured = capsys.readouterr()
        assert "test_monitor" in captured.out
        assert "published=1" in captured.out
        assert "complete=True" in captured.out


# ---------------------------------------------------------------------------
# Phase 4 unit tests — byte-based backpressure
# ---------------------------------------------------------------------------

class TestByteBackpressure:

    def test_byte_limit_triggers_backpressure(self, ray_env):
        """Backpressure should trigger when byte limit is reached, even if batch
        count is under the limit."""
        table = _make_table(100, 0)
        table_bytes = table.nbytes  # ~1600 bytes for 100 rows of (int64, float64)

        # Set byte budget to 2× one table — third publish should block
        coord = StreamCoordinator.remote(
            stream_id="test_byte_bp",
            max_buffered_batches=100,  # high batch limit — won't be the bottleneck
            max_buffered_bytes=table_bytes * 2,
        )

        _publish_table(coord, _make_table(100, 0))
        ray.get(coord.register_consumer.remote("c1"))
        _publish_table(coord, _make_table(100, 1))

        # Buffer: 2 batches, ~2× table_bytes — at byte limit
        published = threading.Event()

        def producer():
            _publish_table(coord, _make_table(100, 2))
            published.set()

        t = threading.Thread(target=producer, daemon=True)
        t.start()
        time.sleep(0.5)
        assert not published.is_set(), "Producer should be blocked by byte backpressure"

        # Pull one batch to free bytes
        ray.get(coord.pull_batch.remote("c1"))
        t.join(timeout=5)
        assert published.is_set(), "Producer should unblock after consumer frees bytes"
        ray.get(coord.deregister_consumer.remote("c1"))


class TestByteTrackingInStats:

    def test_buffered_bytes_in_stats(self, ray_env):
        """buffered_bytes in stats should increase on publish and decrease on GC."""
        coord = StreamCoordinator.remote(
            stream_id="test_byte_stats",
            max_buffered_batches=100,
        )

        table = _make_table(50, 0)
        expected_bytes = table.nbytes

        _publish_table(coord, table)
        stats = ray.get(coord.get_stats.remote())
        assert stats["buffered_bytes"] == expected_bytes
        assert stats["max_buffered_bytes"] == 2 * 1024**3

        _publish_table(coord, _make_table(50, 1))
        stats = ray.get(coord.get_stats.remote())
        assert stats["buffered_bytes"] == expected_bytes * 2

        # Register consumer and consume both → GC should free bytes
        ray.get(coord.register_consumer.remote("c1"))
        ray.get(coord.pull_batch.remote("c1"))
        ray.get(coord.pull_batch.remote("c1"))

        stats = ray.get(coord.get_stats.remote())
        assert stats["buffered_bytes"] == 0
        assert stats["buffer_size"] == 0

        ray.get(coord.deregister_consumer.remote("c1"))


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
