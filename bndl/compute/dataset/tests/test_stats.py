from bndl.compute.dataset.tests import DatasetTest


class StatsTest(DatasetTest):
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
        self.assertEqual(self.worker_count, 4)  # just to be sure, the test depends on it
        self.assertEqual(self.ctx.range(1000, pcount=2).count(), 1000)
        self.assertEqual(self.ctx.range(1000, pcount=4).count(), 1000)
        self.assertEqual(self.ctx.range(1000, pcount=8).count(), 1000)

    def test_empty_parts(self):
        self.assertEqual(self.dset.filter(lambda i: i < 500).count(), 500)
        self.assertEqual(self.dset.map_partitions(lambda p: p if p[0] < 500 else None).count(), 500)
