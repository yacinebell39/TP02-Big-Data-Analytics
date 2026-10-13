"""Three execution strategies for complete taxi-fare summaries."""
from dataclasses import dataclass, field
import math
import time
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd

TEXT = ['key', 'pickup_datetime']
NUMERIC = ['fare_amount', 'pickup_longitude', 'pickup_latitude',
           'dropoff_longitude', 'dropoff_latitude', 'passenger_count']
TYPES = {**{name: 'object' for name in TEXT},
         **{name: 'float64' for name in NUMERIC}}
CSV_OPTIONS = {'dtype': TYPES, 'engine': 'c', 'encoding': 'utf-8', 'on_bad_lines': 'error'}
# Keep the same Python-string representation for every strategy.
dask.config.set({'dataframe.convert-string': False})


def package(rows, counts, sums, groups):
    return {'rows': int(rows), 'non_null_counts': {str(k): int(v) for k, v in counts.items()},
            'numeric': {name: {'sum': float(sums[name]),
                              'mean': float(sums[name] / counts[name]) if counts[name] else None}
                        for name in NUMERIC},
            'fares_by_passengers': [
                {'passengers': None if pd.isna(key) else float(key),
                 'valid_fares': int(row['count']), 'fare_sum': float(row['sum']),
                 'mean_fare': float(row['sum'] / row['count']) if row['count'] else None}
                for key, row in groups.sort_index().iterrows()]}


@dataclass
class RunningTotals:
    rows: int = 0
    chunks: int = 0
    largest_chunk: int = 0
    counts: pd.Series = field(default_factory=lambda: pd.Series(0, index=list(TYPES), dtype='int64'))
    sums: np.ndarray = field(default_factory=lambda: np.zeros(len(NUMERIC)))
    correction: np.ndarray = field(default_factory=lambda: np.zeros(len(NUMERIC)))
    passenger_groups: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=['sum', 'count'], dtype='float64'))

    def accept(self, frame):
        self.rows += len(frame)
        self.chunks += 1
        self.largest_chunk = max(self.largest_chunk, len(frame))
        self.counts += frame.count()
        # Kahan accumulation keeps two vectors, independent of file length.
        increment = frame[NUMERIC].sum().to_numpy() - self.correction
        updated = self.sums + increment
        self.correction = (updated - self.sums) - increment
        self.sums = updated
        grouped = frame.groupby('passenger_count', dropna=False)['fare_amount'].agg(['sum', 'count'])
        self.passenger_groups = self.passenger_groups.add(grouped, fill_value=0)

    def result(self):
        return package(self.rows, self.counts, dict(zip(NUMERIC, self.sums)), self.passenger_groups)


def eager(path, **unused):
    # Preliminary C-parser full-load trials exited natively under the memory cap.
    # The Python parser exposes the allocation failure as a catchable MemoryError.
    full_options = {**CSV_OPTIONS, 'engine': 'python'}
    table = pd.read_csv(path, **full_options)
    summary = package(len(table), table.count(), table[NUMERIC].sum(),
                      table.groupby('passenger_count', dropna=False)['fare_amount'].agg(['sum', 'count']))
    return {'summary': summary, 'dataframe_bytes': int(table.memory_usage(deep=True).sum())}


def stream(path, chunksize=100_000, **unused):
    accumulator = RunningTotals()
    with pd.read_csv(path, chunksize=chunksize, **CSV_OPTIONS) as reader:
        for table in reader:
            accumulator.accept(table)
            del table
            if accumulator.chunks % 100 == 0:
                print(f'Streamed {accumulator.rows:,} rows', flush=True)
    return {'summary': accumulator.result(), 'chunks': accumulator.chunks,
            'largest_chunk_rows': accumulator.largest_chunk, 'chunksize': chunksize}


def build_queries(path, block_mib=16):
    table = dd.read_csv(str(path), blocksize=block_mib * 1024**2, **CSV_OPTIONS)
    # All eight column counts force equal parsing scope across the methods.
    queries = (table.shape[0], table.count(), table[NUMERIC].sum(),
               table.groupby('passenger_count', dropna=False)['fare_amount'].agg(['sum', 'count']))
    optimized = tuple(query.optimize(fuse=True) for query in queries)
    graph = dask.highlevelgraph.HighLevelGraph.merge(*(q.__dask_graph__() for q in queries))
    optimized_graph = dask.highlevelgraph.HighLevelGraph.merge(*(q.__dask_graph__() for q in optimized))
    return table, queries, optimized, {'partitions': table.npartitions, 'tasks_before': len(graph),
                                     'tasks_after': len(optimized_graph),
                                     'query_expressions': [str(q.expr) for q in queries]}


def lazy(path, threads=4, block_mib=16, **unused):
    started = time.perf_counter()
    table, queries, optimized, graph = build_queries(path, block_mib)
    construction_seconds = time.perf_counter() - started
    print(f"Lazy graph: {graph['partitions']} partitions, {graph['tasks_before']} -> {graph['tasks_after']} tasks", flush=True)
    started = time.perf_counter()
    rows, counts, sums, groups = dask.compute(*optimized, scheduler='threads', num_workers=threads)
    return {'summary': package(rows, counts, sums, groups), 'graph': graph,
            'graph_build_seconds': construction_seconds, 'compute_seconds': time.perf_counter() - started,
            'threads': threads, 'block_mib': block_mib}


def verify(expected, actual):
    if isinstance(expected, dict):
        assert expected.keys() == actual.keys(), 'Different result fields'
        for name in expected:
            verify(expected[name], actual[name])
    elif isinstance(expected, list):
        assert len(expected) == len(actual), 'Different passenger groups'
        for left, right in zip(expected, actual):
            verify(left, right)
    elif isinstance(expected, float):
        assert math.isclose(expected, actual, rel_tol=1e-10, abs_tol=1e-6), (expected, actual)
    else:
        assert expected == actual, (expected, actual)


METHODS = {'eager': eager, 'stream': stream, 'lazy': lazy}
