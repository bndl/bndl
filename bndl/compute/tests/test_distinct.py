from bndl.compute.tests import DatasetTest
from bndl.util.funcs import iseven, isodd


class TestDistinct(DatasetTest):
    def setUp(self):
        self.dset = self.ctx.range(1000, pcount=3)

    def test_distinct_odd_even(self):
        remainders = sorted(self.dset.map(lambda x: x % 2).distinct().collect())
        self.assertEqual(remainders, [0, 1])

    def test_collect_as_set(self):
        distinct = self.dset.map(lambda x:x % 2).distinct()
        remainders = distinct.collect_as_set()
        self.assertEqual(remainders, {0, 1})
        self.assertEqual(list(remainders), distinct.collect())

    def test_distinct_by_key(self):
        remainders = self.dset.distinct(3, key=lambda x: x % 2).collect()
        self.assertEqual(len(remainders), 2)
        remainders.sort(key=iseven)
        self.assertTrue(isodd(remainders[0]))
        self.assertTrue(iseven(remainders[1]))
