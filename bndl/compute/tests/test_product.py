from functools import reduce
from itertools import product
import string

from bndl.compute.tests import DatasetTest


class CartesianProductTest(DatasetTest):
    def test_product(self):
        a = string.ascii_letters
        b = string.ascii_lowercase
        c = string.digits
        abc = list(product(a, b, map(int, c)))

        dsets = [
            self.ctx.collection(a, pcount=3).map_partitions(lambda p: iter(p)),
            self.ctx.collection(b, pcount=4).map_partitions(lambda p: sorted(p, reverse=True)),
            self.ctx.collection(c, pcount=5).map(int),
        ]

        dset = reduce(lambda a, b: a.product(b), dsets)
        self.assertEqual(sorted(dset.collect()), sorted(abc))


    def test_selfproduct(self):
        dset = self.ctx.collection(string.ascii_letters)
        actual = dset.product(dset).collect()
        actual.sort()
        expected = sorted(product(*[string.ascii_letters] * 2))
        self.assertEqual(actual, expected)
