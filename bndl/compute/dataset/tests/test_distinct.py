from bndl.compute.dataset.tests import DatasetTest


class TestDistinct(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(10)

    def test_distinct_odd_even(self):
        remainders = sorted(self.dset.map(lambda x: x % 2).distinct().collect())
        self.assertEqual(remainders, [0, 1])

    def test_collect_as_set(self):
        remainders = self.dset.map(lambda x: x % 2).distinct().collect_as_set()
        self.assertEqual(remainders, {0, 1})
