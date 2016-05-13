import math
from unittest.case import SkipTest

from bndl.compute.dataset.tests import DatasetTest


class StatsTest(DatasetTest):
    worker_count = 3

    def setUp(self):
        self.dset = self.ctx.range(1000)

    def test_calc_min(self):
        self.assertEqual(self.dset.min(), 0)

    def test_calc_max(self):
        self.assertEqual(self.dset.max(), 999)

    def test_calc_mean(self):
        self.assertEqual(self.dset.mean(), 499.5)

    def test_calc_sum(self):
        self.assertEqual(self.dset.sum(), 499500)

    def test_calc_count(self):
        self.assertEqual(self.dset.count(), 1000)

    def test_pcounts(self):
        self.assertEqual(self.ctx.range(1000, pcount=2).count(), 1000)
        self.assertEqual(self.ctx.range(1000, pcount=4).count(), 1000)
        self.assertEqual(self.ctx.range(1000, pcount=8).count(), 1000)

    def test_empty_parts(self):
        self.assertEqual(self.dset.filter(lambda i: i < 500).count(), 500)
        self.assertEqual(self.dset.map_partitions(lambda p: p if p[0] < 500 else None).count(), 500)

    def test_calc_stats(self):
        try:
            import numpy as np
            import scipy.stats
        except ImportError:
            raise SkipTest('no numpy and/or scipy available for testing')

        # use numpy / scipy as reference
        arr = np.log(np.arange(1, 1000))
        # take stats from a log range to get some skew
        dset = self.ctx.range(1, 1000).map(math.log)
        stats = dset.stats()

        self.assertEqual(stats.count, len(arr))
        self.assertAlmostEqual(stats.mean, arr.mean())
        self.assertAlmostEqual(stats.mean, dset.mean())
        self.assertEqual(stats.min, arr.min())
        self.assertEqual(stats.max, arr.max())
        self.assertAlmostEqual(stats.variance, np.var(arr))
        self.assertAlmostEqual(stats.stdev, np.std(arr))
        self.assertAlmostEqual(stats.sample_variance, np.var(arr, ddof=1))
        self.assertAlmostEqual(stats.sample_stdev, np.std(arr, ddof=1))
        self.assertAlmostEqual(stats.skew, scipy.stats.skew(arr))
        self.assertAlmostEqual(stats.kurtosis, scipy.stats.kurtosis(arr))
