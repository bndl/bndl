import random

from bndl.compute.tests import DatasetTest


class SortTest(DatasetTest):
    def test_sort(self):
        col = [str(random.random())
               for _ in range(1000000)]
        col = col * 10
        dset = self.ctx.collection(col, pcount=100)
        self.assertEqual(dset.sort(pcount=100).collect(), sorted(col))
#         for length in (10, 1000):
#             for maxint in (3, 10, 100, 1000):
#                 col = [random.randint(1, maxint) for _ in range(length)]
#                 dset = self.ctx.collection(col)
#                 self.assertEqual(dset.sort().collect(), sorted(col))
