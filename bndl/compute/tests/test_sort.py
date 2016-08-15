import random

from bndl.compute.tests import DatasetTest


class SortTest(DatasetTest):
    def test_sort(self):
        rng = random.Random(1)
        for length in (10, 1000):
            for maxint in (2, 3, 4, 5, 10, 100, 1000):
                dset = self.ctx.collection([rng.randint(1, maxint) for _ in range(length)])
                self.assertEqual(dset.sort().count(), dset.count())
                self.assertEqual(dset.sort().collect(), sorted(dset.collect()))
