import random

from bndl.compute.tests import DatasetTest


class SortTest(DatasetTest):
    def test_sort(self):
        for length in (10, 1000):
            for maxint in (3, 10, 100, 1000):
                col = [random.randint(1, maxint) for _ in range(length)]
                dset = self.ctx.collection(col)
                self.assertEqual(dset.sort().collect(), sorted(col))
