from collections import Counter
from operator import add

from bndl.compute.tests import DatasetTest


class TreeAggregateTest(DatasetTest):
    def setUp(self):
        super().setUp()
        self.dset = self.ctx.range(1000, pcount=100)

    def test_aggregate(self):
        self.assertEqual(self.dset.tree_aggregate(sum), self.dset.sum())
        self.assertEqual(self.dset.tree_aggregate(Counter, lambda counters: sum(counters, Counter())),
                         Counter(self.dset.collect()))
 
    def test_reduce(self):
        self.assertEqual(self.dset.tree_reduce(add), self.dset.sum())
        self.assertEqual(self.dset.map_partitions(Counter).glom().tree_reduce(add),
                         self.dset.count_by_value())
 
    def test_combine(self):
        def count(counter, value):
            counter[value] += 1
            return counter
        self.assertEqual(self.dset.tree_combine(Counter(), count, add),
                         Counter(self.dset.collect()))
 
    def test_depth(self):
        for d in range(2, 5):
            self.assertEqual(self.dset.tree_aggregate(sum, depth=d),
                             self.dset.sum())
