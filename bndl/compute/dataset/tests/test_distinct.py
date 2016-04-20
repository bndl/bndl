from bndl.compute.dataset.tests import DatasetTest


class TestDistinct(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(10)

    def test_distinct_odd_even(self):
        v = sorted(self.dset.map(lambda x: x % 2).distinct().collect())
        self.assertEqual(v, [0, 1])

    def test_collect_as_set(self):
        v = self.dset.map(lambda x: x % 2).distinct().collect_as_set()
        self.assertEqual(v, {0, 1})
