import ray
import ray.data


class StreamingIterator:
    """Pulls Arrow tables from a MicroBatchCoordinator in real-time.

    Two consumption modes:
      - __iter__(): yields pa.Table as each micro-batch arrives (simple path)
      - iter_datasets(window_size): accumulates N tables, yields a
        ray.data.Dataset per window (Ray Data path)
    """

    def __init__(self, coordinator):
        self._coordinator = coordinator

    def __iter__(self):
        while True:
            table = ray.get(self._coordinator.get_batch.remote())
            if table is None:
                return
            yield table

    def iter_datasets(self, window_size):
        window = []
        for table in self:
            window.append(ray.put(table))
            if len(window) >= window_size:
                yield ray.data.from_arrow_refs(window)
                window = []
        if window:  # flush remainder
            yield ray.data.from_arrow_refs(window)
