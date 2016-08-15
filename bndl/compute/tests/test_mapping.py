from itertools import chain

from bndl.compute.tests import DatasetTest
from bndl.util.funcs import identity


class MappingTest(DatasetTest):
    def setUp(self):
        super().setUp()
        self.dset = self.ctx.range(10, pcount=3)

    def test_mapping(self):
        remainders = self.dset.map(lambda x: x % 2).collect()
        self.assertEqual(remainders, list(x % 2 for x in range(10)))

    def test_flatmap(self):
        self.assertEqual(self.dset.flatmap(lambda i: (i,)).collect(),
                         list(range(10)))
        self.assertEqual(self.dset.flatmap(lambda x: [x, x + 1]).collect(),
                        list(chain.from_iterable((x, x + 1) for x in range(10))))
        self.assertEqual(''.join(self.ctx.range(100, pcount=3).map(str).flatmap(identity).collect()),
                         ''.join(list(map(str, range(100)))))

    def test_map_partitions(self):
        self.assertEqual(self.dset.map_partitions(identity).collect(), list(range(10)))
        self.assertEqual(sorted(self.dset.map_partitions(lambda p: (len(p),)).collect()), [3, 3, 4])
        self.assertEqual(self.dset.map_partitions_with_index(lambda idx, i: (idx,)).collect(), [0, 1, 2])
        self.assertEqual(self.dset.map_partitions_with_part(lambda p, it: (p.idx,)).collect(), [0, 1, 2])
