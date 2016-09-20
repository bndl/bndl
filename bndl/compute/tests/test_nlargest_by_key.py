from bndl.compute.tests import DatasetTest
from heapq import nlargest, nsmallest
from itertools import groupby, product
from bndl.compute.dataset import Dataset


class LargestByKeyTest(DatasetTest):
    def test_nlargest_by_key(self):
        ks = [3, 7]
        ops = [(nlargest, Dataset.nlargest_by_key),
               (nsmallest, Dataset.nsmallest_by_key)]

        nums = range(100)
        keyf = lambda i:i // 10

        for k, (op, dop) in product(ks, ops):
            expected = {key: list(op(k, values))
                        for key, values in groupby(sorted(nums, key=keyf), keyf)}

            dset = self.ctx.collection(nums).key_by(keyf)
            self.assertEqual(dop(dset, k).collect_as_map(), expected)
