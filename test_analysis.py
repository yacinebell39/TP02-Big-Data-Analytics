"""Check unequal chunks, missing values and passenger groups against known answers."""
from pathlib import Path
import tempfile
import unittest
import pandas as pd
from analysis import NUMERIC, TYPES, eager, stream, lazy, verify


class TaxiReductions(unittest.TestCase):
    def test_known_results_for_all_three_strategies(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent / 'results') as temporary:
            path = Path(temporary) / 'fixture.csv'
            table = pd.DataFrame({name: [10.] * 7 for name in NUMERIC})
            table['fare_amount'] = [1., 2., None, -3., 7., 0., 100.]
            table['passenger_count'] = [1., 1., 2., 2., 2., None, 6.]
            table['key'] = [f'ride-{i}' for i in range(7)]
            table['pickup_datetime'] = ['2012-04-21 04:30:42 UTC'] * 7
            table.loc[0, 'dropoff_latitude'] = None
            table.to_csv(path, columns=list(TYPES), index=False)
            complete = eager(path)['summary']
            chunked = stream(path, chunksize=3)
            parallel = lazy(path, threads=2, block_mib=1)['summary']
            verify(complete, chunked['summary'])
            verify(complete, parallel)
            self.assertEqual(complete['rows'], 7)
            self.assertEqual(complete['non_null_counts']['fare_amount'], 6)
            self.assertEqual(complete['non_null_counts']['passenger_count'], 6)
            self.assertEqual(complete['non_null_counts']['dropoff_latitude'], 6)
            self.assertEqual(complete['numeric']['fare_amount']['sum'], 107.)
            self.assertAlmostEqual(complete['numeric']['fare_amount']['mean'], 107 / 6)
            self.assertEqual(complete['numeric']['passenger_count']['sum'], 14.)
            self.assertEqual(chunked['chunks'], 3)
            self.assertEqual(chunked['largest_chunk_rows'], 3)
            groups = complete['fares_by_passengers']
            self.assertEqual([g['passengers'] for g in groups], [1., 2., 6., None])
            self.assertEqual([g['fare_sum'] for g in groups], [3., 4., 100., 0.])
            self.assertEqual([g['valid_fares'] for g in groups], [2, 2, 1, 1])

    def test_multiple_dask_partitions_and_exact_chunk_boundary(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent / 'results') as temporary:
            path = Path(temporary) / 'boundary.csv'
            size = 100_003
            table = pd.DataFrame({name: [2.] * size for name in NUMERIC})
            table['key'] = ['2012-04-21 04:30:42.0000001'] * size
            table['pickup_datetime'] = ['2012-04-21 04:30:42 UTC'] * size
            table.loc[size-1, 'fare_amount'] = 17.
            table.to_csv(path, columns=list(TYPES), index=False)
            chunked = stream(path)
            partitioned = lazy(path, threads=2, block_mib=1)
            self.assertGreater(partitioned['graph']['partitions'], 1)
            self.assertEqual(chunked['chunks'], 2)
            self.assertEqual(chunked['largest_chunk_rows'], 100_000)
            self.assertEqual(chunked['summary']['rows'], size)
            self.assertEqual(chunked['summary']['numeric']['fare_amount']['sum'], 2. * (size-1) + 17.)
            verify(chunked['summary'], partitioned['summary'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
