"""Lightweight streaming monitoring utilities."""

import logging
import time
from typing import Optional

import ray

logger = logging.getLogger(__name__)


def _format_bytes(n: int) -> str:
    """Format byte count as human-readable string."""
    if n >= 1024**3:
        return f"{n / 1024**3:.1f}GB"
    if n >= 1024**2:
        return f"{n / 1024**2:.1f}MB"
    if n >= 1024:
        return f"{n / 1024:.1f}KB"
    return f"{n}B"


def print_stats(coordinator, interval: float = 2.0, max_iterations: Optional[int] = None):
    """Continuously print coordinator stats to stdout.

    Useful for development and debugging without Prometheus/Grafana.

    Args:
        coordinator: StreamCoordinator actor handle.
        interval: Seconds between polls.
        max_iterations: Stop after N iterations (None = run until stream completes).
    """
    prev_published = 0
    prev_time = time.time()
    iterations = 0

    while max_iterations is None or iterations < max_iterations:
        stats = ray.get(coordinator.get_stats.remote())
        now = time.time()
        dt = now - prev_time

        published = stats["batches_published"]
        throughput = (published - prev_published) / dt if dt > 0 else 0

        buffered_bytes = stats.get("buffered_bytes", 0)
        max_buffered_bytes = stats.get("max_buffered_bytes", 0)
        buf_display = _format_bytes(buffered_bytes)
        max_display = _format_bytes(max_buffered_bytes)

        print(
            f"[{stats['stream_id']}] "
            f"published={published} "
            f"gc={stats['batches_gc']} "
            f"buffer={stats['buffer_size']}/{stats['max_buffered']} "
            f"bytes={buf_display}/{max_display} "
            f"consumers={stats['num_consumers']} "
            f"throughput={throughput:.1f} batches/s "
            f"watermark={stats.get('latest_watermark', 'N/A')} "
            f"complete={stats['complete']}"
        )

        prev_published = published
        prev_time = now
        iterations += 1

        if stats["complete"]:
            break
        time.sleep(interval)
