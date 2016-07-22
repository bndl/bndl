from bndl.compute.dataset.tests import DatasetTest
from collections import Counter


class CountByValueTest(DatasetTest):
    def test_count(self):
        dset = self.ctx.range(0, 1000).map(lambda i: i % 10)
        self.assertEqual(dset.count_by_value(), Counter(dset.collect()))
